import json
import websockets
import os
import json
from redis import Redis
from typing import Dict
from bson import ObjectId
# import httpx
from models.Survey import SurveyResponse
from modules.MongoWrapper import monet_db
from types import SimpleNamespace
from modules.ServerLogger import ServerLogger
from modules.ProdProbe_v2 import Probe, NSIGHT_v2
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from models.Survey import SurveyResponse, SurveyConfig, QuestionConfig
from utils.db_switcher import DBSwitcher

websocket_router = APIRouter(prefix="/ws", tags=["websocket", "ai-qa"])
logger = ServerLogger()

active_connections: Dict[str, WebSocket] = {}
DbSurvey = monet_db.get_collection("surveys")
DbSurveyQuestion = monet_db.get_collection("survey-questions")

probes = {}

@websocket_router.websocket("/ai-qa")
async def websocket_ai_qa(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            survey_response = SurveyResponse.model_validate_json(data)
            
          `  # Validate the survey exists
            survey = DbSurvey.find_one({"_id": ObjectId(survey_response.su_id)})
            if not survey:
                await websocket.send_json({
                    "error": True,
                    "message": "Survey not found",
                    "code": 404
                })
                continue
`
            question = DbSurveyQuestion.find_one({"_id": ObjectId(survey_response.qs_id)})
            if not question:
                await websocket.send_json({
                    "error": True,
                    "message": "Question not found",
                    "code": 404
                })
                continue

            survey = PySurvey(**survey)
            question = PySurveyQuestion(**question)

            try:
                probe = None
                key = f"{survey_response.su_id}-{survey_response.qs_id}-{survey_response.mo_id}"
                if key in probes:
                    probe = probes[key]
                else:
                    probe = Probe(mo_id=survey_response.mo_id,metadata=survey,question=question,simple_store=False,session_no=0, survey_details=survey_response)
                    probes[key] = probe
                if survey_response.question == question.question:
                    session_no = probe.session_no + 1
                    probe = Probe(mo_id=survey_response.mo_id,metadata=survey,question=question,simple_store=False,session_no=session_no, survey_details=survey_response)
                    probes[key] = probe   

                # Generate follow-up using the probe
                stream, metric_stream = probe.gen_streamed_follow_up(survey_response.question, survey_response.response)
                final_response = {
                    "error": False,
                    "message": "streaming-started",
                    "code": 200,
                    "response": {
                        "question": "",
                        "min_probing": probe.question.config.probes,
                        "max_probing": probe.question.config.max_probes,
                    }
                }
                ended_response = {}

                async for metric in metric_stream:
                    final_response["message"] = "streaming-started"
                    final_response["response"] = {
                        **final_response["response"],
                        "ended": True if metric.quality >= probe.question.config.quality_threshold else False,
                        "metrics": metric.model_dump(),
                        "is_gibberish": True if metric.gibberish_score > question.config.gibberish_score else False
                    }
                    ended_response = final_response.copy()
                    ended_response["message"] = "streaming-ended"
                    await websocket.send_json(final_response)

                if final_response["response"]["is_gibberish"] == False:
                    async for chunk in stream:
                        final_response["message"] = "streaming"
                        final_response["response"] = {
                            **final_response["response"],
                            "question": chunk.content,
                            "ended": probe.ended,
                        }
                        await websocket.send_json(final_response)

                await websocket.send_json(ended_response)
                if probe.simple_store:
                    nsight_v2 = NSIGHT_v2(**{**metric.model_dump(), "question": survey_response.question, "response": survey_response.response})
                    probe.store_response(nsight_v2, session_no)
                    
            except Exception as e:
                logger.error(f"Error in microservice WS communication: {e}")
                await websocket.send_json({
                    "error": True,
                    "message": str(e),
                    "code": 500
                })

    except WebSocketDisconnect:
        logger.info(f"Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error:")
        logger.error(e)
        await websocket.close(code=1011, reason="Internal server error")
