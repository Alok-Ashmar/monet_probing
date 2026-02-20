import os
import json
from redis import Redis
from typing import Dict
from bson import ObjectId
from types import SimpleNamespace
from modules.ServerLogger import ServerLogger
from modules.ProdProbe_v2 import Probe, NSIGHT_v2
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from models.Survey import SurveyResponse, SurveyConfig, QuestionConfig
from utils.db_switcher import DBSwitcher

websocket_router = APIRouter(prefix="/ws", tags=["websocket", "ai-qa"])
logger = ServerLogger()

active_connections: Dict[str, WebSocket] = {}
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_client = Redis.from_url(redis_url)

probes = {}
db_switcher = DBSwitcher(logger=logger)


def _is_object_id(value: str) -> bool:
    try:
        ObjectId(value)
        return True
    except Exception:
        return False


def _is_int_id(value: str) -> bool:
    return value.isdigit()

@websocket_router.websocket("/ai-qa")
async def websocket_ai_qa(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            survey_response = SurveyResponse.model_validate_json(data)
            
            redis_key = f"survey_details:{survey_response.su_id}:{survey_response.qs_id}"
            cached_payload = redis_client.get(redis_key)
            if not cached_payload:
                su_id = str(survey_response.su_id)
                qs_id = str(survey_response.qs_id)
                if _is_object_id(su_id) and _is_object_id(qs_id):
                    db_type = "mongo"
                elif _is_int_id(su_id) and _is_int_id(qs_id):
                    db_type = "mysql"
                else:
                    await websocket.send_json({
                        "error": True,
                        "message": "Invalid survey or question id format",
                        "code": 400
                    })
                    continue

                output, error = await db_switcher.fetch_and_cache_survey_details(
                    db_type=db_type,
                    survey_response=survey_response,
                )
                if error or not output:
                    await websocket.send_json(error or {
                        "error": True,
                        "message": "Survey details not found",
                        "code": 404
                    })
                    continue
                payload = output
            else:
                payload = json.loads(cached_payload)
            
            survey_data = payload.get("survey") or {}
            question_data = payload.get("question") or {}

            survey_config = SurveyConfig(
                language=survey_data.get("language", "English"),
                add_context=survey_data.get("add_context", True),
            )
            survey = SimpleNamespace(
                id=survey_response.su_id,
                description=survey_data.get("survey_description", "-"),
                config=survey_config,
            )

            question_config = QuestionConfig(
                probes=question_data.get("min_probe", 0),
                max_probes=question_data.get("max_probe", 0),
                quality_threshold=question_data.get("quality_threshold", 4),
                gibberish_score=question_data.get("gibberish_score", 4),
                add_context=question_data.get("add_context", True),
            )
            question = SimpleNamespace(
                id=survey_response.qs_id,
                question=question_data.get("question", ""),
                description=question_data.get("question_description", ""),
                config=question_config,
            )

            try:
                probe = None
                key = f"{survey_response.su_id}-{survey_response.qs_id}-{survey_response.mo_id}"
                if key in probes:
                    probe = probes[key]
                else:
                    probe = Probe(mo_id=survey_response.mo_id,metadata=survey,question=question,simple_store=False,session_no=0, survey_details=survey_response)
                    probes[key] = probe
                if (survey_response.question or "").strip() == (question.question or "").strip():
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
                logger.error("Error in websocket AI QA:")
                logger.error(e)
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
