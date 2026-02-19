from models.Survey import PdSurvey, PdSurveyQuestion, SurveyConfig, QuestionConfig
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from typing import Dict
from modules.ProdProbe_v2 import Probe, NSIGHT_v2
from models.Survey import PdSurveyResponse
from modules.ServerLogger import ServerLogger

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from models.sql.db import get_db
from models.sql.models import (
    SurveyResponse,
)

websocket_router = APIRouter(prefix="/ws", tags=["websocket", "ai-qa"])
logger = ServerLogger()

active_connections: Dict[str, WebSocket] = {}

probes = {}

@websocket_router.websocket("/ai-qa")
async def websocket_ai_qa(websocket: WebSocket, db: AsyncSession = Depends(get_db)):
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            survey_response = PdSurveyResponse.model_validate_json(data)
            
            # Validate the survey exists
            query_survey = text("SELECT * FROM test_study WHERE study_id = :su_id")
            result = await db.execute(query_survey,{"su_id":survey_response.su_id})
            survey = result.mappings().first()
            global_flags = json.loads(survey.get("global_flags"))
            required_keys = ["llm", "language", "llm_role", "content_type", "description"]
            if not survey and survey["global_flags"]["probing"]:
                await websocket.send_json({
                    "error": True,
                    "message": "Survey not found",
                    "code": 404
                })
                continue
            # elif not all(global_flags.get(k) for k in required_keys):
            #     await WebSocket.send_json({
            #         "error": True,
            #         "message": "Survey not configured correctly",
            #         "code": 404
            #     })
            #     continue
            
            query_question = text("SELECT * FROM probe_survey_question where qs_id= :qs_id AND su_id= :su_id")
            result = await db.execute(query_question,{"su_id":survey_response.su_id, "qs_id": survey_response.qs_id})
            question = result.mappings().first()
            parse_config = json.loads(question.get("config"))
            if not question:
                await websocket.send_json({
                    "error": True,
                    "message": "Question not found",
                    "code": 404
                })
                continue
            
            survey_config = SurveyConfig(
                    language = global_flags.get("language", "English"),
                    # llm = global_flags.get("llm"),
                    # llm_role = global_flags.get("llm_role"),
                    # content_type = global_flags.get("content_type"),
                    # mediaAI = global_flags.get("mediaAI"),
                    # add_context = global_flags.get("add_context"),
                    # adaptive_probing = global_flags.get("adaptive_probing"),
            )
            survey = PdSurvey(
                id=survey.get("id"),
                study_id=survey.get("study_id"),
                cnt_id=survey.get("cnt_id"),
                survey_description=global_flags.get("survey_description", "-"),
                survey_title=survey.get("study_name") or survey.get("cell_name") or survey.get("survey_title"),
                # service=survey.service,
                # llm=global_flags.get("llm"),
                # language=global_flags.get("language"),
                # add_context=global_flags.get("add_context"),
                config=survey_config
            )

            question_config  = QuestionConfig(**parse_config)

            question = PdSurveyQuestion(
                id=question.get("id"),
                su_id=question.get("su_id"),
                cnt_id=question.get("cnt_id"),
                question=question.get("question"),
                description=question.get("description"),
                seq_num=question.get("seq_num"),
                config=question_config
            )

            try:
                probe = None
                key = f"{survey_response.su_id}-{survey_response.qs_id}-{survey_response.mo_id}"
                if key in probes:
                    probe = probes[key]
                else:
                    probe = Probe(survey_response.mo_id,survey,question,True,0)
                    probes[key] = probe


                if survey_response.question == question.question:
                    session_no = probe.session_no + 1
                    probe = Probe(survey_response.mo_id,survey,question,True,session_no)
                    probes[key] = probe
                # Generate follow-up using the probe
                stream, metric_stream = probe.gen_streamed_follow_up(survey_response.question, survey_response.response)

                config = probe.question.config.model_dump()
                if "media" in config:
                    del config["media"]
                final_response = {
                    "error": False,
                    "message": "streaming-started",
                    "code": 200,
                    "response": {
                        "question": "",
                        **config
                    }
                }
                ended_response = {}
                async for metric in metric_stream:
                    final_response["message"] = "streaming-started"
                    final_response["response"] = {
                        **final_response["response"],
                        "ended": True if metric.quality > 4 else False,
                        "metrics": metric.model_dump(),
                        "is_gibberish": True if metric.gibberish_score > question.config.gibberish_score else False
                    }
                    ended_response = final_response.copy()
                    ended_response["message"] = "streaming-ended"
                    await websocket.send_json(final_response)
                if final_response["response"]["is_gibberish"] == False:
                    # Check for low relevance
                    if ended_response["response"]["metrics"].get("relevance", 10) < 4:
                        # Send the requested message
                        await websocket.send_json({
                            "error": False,
                            "message": "relevance-low",
                            "code": 200,
                            "description": "you response did not make any sense"
                        })
                        
                        # Generate a redirection question
                        redirection_stream = probe.gen_streamed_redirection()
                        async for chunk in redirection_stream:
                            final_response["message"] = "streaming"
                            final_response["response"] = {
                                **final_response["response"],
                                "question": chunk.content,
                                "ended": probe.ended,
                            }
                            await websocket.send_json(final_response)
                    else:
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
                    nsight_v2 = NSIGHT_v2(**{**ended_response["response"]["metrics"], "question": survey_response.question, "response": survey_response.response})
                    new_survey_response = SurveyResponse(
                        su_id=survey_response.su_id,
                        mo_id=survey_response.mo_id,
                        qs_id=survey_response.qs_id,
                        cnt_id=survey_response.cnt_id,
                        question=survey_response.question,
                        response=survey_response.response,
                        reason=nsight_v2.reason,
                        keywords=nsight_v2.keywords,
                        quality=nsight_v2.quality,
                        relevance=nsight_v2.relevance,
                        confusion=nsight_v2.confusion,
                        negativity=nsight_v2.negativity,
                        consistency=nsight_v2.consistency,
                        qs_no=probe.counter,
                        session_no=probe.session_no
                    )
                    db.add(new_survey_response)
                    await db.commit()
                    # await db.refresh(new_survey_response)
                    # probe.store_response(nsight_v2)
            except Exception as e:
                logger.err("Error in websocket AI QA:")
                logger.err(e)
                await websocket.send_json({
                    "error": True,
                    "message": str(e),
                    "code": 500
                })

    except WebSocketDisconnect:
        logger.info(f"Client disconnected")
    except Exception as e:
        logger.err(f"WebSocket error:")
        logger.err(e)
        await websocket.close(code=1011, reason="Internal server error")