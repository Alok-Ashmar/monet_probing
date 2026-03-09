import os
import json
from redis import Redis
from typing import Dict
from types import SimpleNamespace
from services.survey_probe import Probe
from services.ServerLogger import ServerLogger
from services.relevance_checker import RelevanceChecker
from services.repetition_checker import RepetitionChecker
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from models.schemas import SurveyResponse, SurveyConfig, QuestionConfig

websocket_router = APIRouter(prefix="/ws", tags=["websocket", "probe_engine"])
logger = ServerLogger()

active_connections: Dict[str, WebSocket] = {}
redis_client = Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))

def _probe_state_key(su_id: str, qs_id: str, mo_id: str) -> str:
    return f"probe_state:{su_id}:{qs_id}:{mo_id}"

def _load_probe_state(key: str) -> dict:
    try:
        cached = redis_client.get(key)
        if not cached:
            return {}
        return json.loads(cached)
    except Exception as e:
        logger.error("Failed to load probe state from Redis")
        logger.error(e)
        return {}

@websocket_router.websocket("/probe_engine")
async def websocket_probe_engine(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            survey_response = SurveyResponse.model_validate_json(data)

            try:
                redis_key = f"survey_details:{survey_response.su_id}:{survey_response.qs_id}"
                cached_survey_details = json.loads(redis_client.get(redis_key))
                
            except Exception as e:
                logger.error("Failed to load survey details from Redis")
                logger.error(e)
                await websocket.send_json({
                    "error": True,
                    "message": "Failed to load survey details from Redis",
                    "code": 500
                })

            survey_data = cached_survey_details.get("survey") or {}
            question_data = cached_survey_details.get("question") or {}

            survey_config = SurveyConfig(
                language=survey_data.get("language"),
                add_context=survey_data.get("add_context"),
                repetition=survey_data.get("repetition"),
            )
            survey = SimpleNamespace(
                id=survey_response.su_id,
                description=survey_data.get("survey_description"),
                config=survey_config,
            )

            question_config = QuestionConfig(
                probes=question_data.get("min_probe"),
                max_probes=question_data.get("max_probe"),
                quality_threshold=question_data.get("quality_threshold"),
                gibberish_score=question_data.get("gibberish_score"),
                add_context=question_data.get("add_context"),
                repetition=question_data.get("repetition"),
            )
            question = SimpleNamespace(
                id=survey_response.qs_id,
                question=question_data.get("question"),
                description=question_data.get("question_description"),
                config=question_config,
            )

            repetition_checker = RepetitionChecker()
            
            if survey_config.repetition:
                is_repetition = repetition_checker.survey_check_repetition(survey_response)
            elif question_config.repetition:
                is_repetition = repetition_checker.question_check_repetition(survey_response)
            else:
                is_repetition = False

            try:
                # If repetition is detected, send the default response payloads over the websocket
                if is_repetition:
                    await websocket.send_json({
                        "error": False,
                        "message": "streaming-started",
                        "code": 200,
                        "response": {
                            "question": "",
                            "min_probing": running_probe.question.config.probes,
                            "max_probing": running_probe.question.config.max_probes,
                            "is_repetition": True,
                        }
                    })
                    await websocket.send_json({
                        "error": False,
                        "message": "streaming-ended",
                        "code": 200,
                        "response": {
                            "question": "",
                            "min_probing": running_probe.question.config.probes,
                            "max_probing": running_probe.question.config.max_probes,
                            "is_repetition": True,
                        }
                    })
                    return
                
                # Initialize the probe
                running_probe = None
                state_key = _probe_state_key(str(survey_response.su_id), str(survey_response.qs_id), str(survey_response.mo_id))
                cached_probe_state = _load_probe_state(state_key)
                session_no = int(cached_probe_state.get("session_no", 0))
                running_probe = Probe(mo_id=survey_response.mo_id, metadata=survey, question=question, simple_store=True, session_no=session_no, survey_details=survey_response)

                # Generate follow-up using the probe
                stream, metric_stream = running_probe.gen_streamed_follow_up(survey_response.question, survey_response.response)
                final_response = {
                    "error": False,
                    "message": "streaming-started",
                    "code": 200,
                    "response": {
                        "question": "",
                        "min_probing": running_probe.question.config.probes,
                        "max_probing": running_probe.question.config.max_probes,
                    }
                }
                ended_response = {}
                
                # Stream the metrics
                async for metric in metric_stream:
                    # Check for relevance threshold and update prompt if needed
                    RelevanceChecker.check_and_update_prompt(running_probe, metric)

                    final_response["message"] = "streaming-started"
                    final_response["response"] = {
                        **final_response["response"],
                        "ended": True if metric.get("quality", 0) >= running_probe.question.config.quality_threshold else False,
                        "metrics": metric,
                        "is_gibberish": True if metric.get("gibberish_score", 0) > running_probe.question.config.gibberish_score else False,
                        "is_repetition": is_repetition,
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
                            "ended": running_probe.ended,
                        }
                        await websocket.send_json(final_response)

                await websocket.send_json(ended_response)

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
