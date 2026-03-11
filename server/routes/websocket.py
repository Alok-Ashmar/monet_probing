import asyncio
from typing import Dict
from types import SimpleNamespace
from services.survey_probe import Probe
from utils.redis_pool import get_redis
from utils.ServerLogger import ServerLogger
from services.relevance_checker import RelevanceChecker
from services.repetition_checker import RepetitionChecker
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from models.schemas import SurveyResponse, SurveyConfig, QuestionConfig
from utils.state_management import load_probe_state, load_survey_details

websocket_router = APIRouter(prefix="/ws", tags=["websocket", "probe_engine"])
logger = ServerLogger()

active_connections: Dict[str, WebSocket] = {}

@websocket_router.websocket("/probe_engine")
async def websocket_probe_engine(websocket: WebSocket):
    await websocket.accept()
    
    redis = get_redis()
    repetition_checker = RepetitionChecker(redis)

    try:
        while True:
            data = await websocket.receive_text()
            survey_response = SurveyResponse.model_validate_json(data)

            try:
                cached_survey_details = await load_survey_details(
                    str(survey_response.su_id), str(survey_response.qs_id)
                )
                
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

            if survey_config.repetition:
                is_repetition = await repetition_checker.survey_check_repetition(survey_response)
            elif question_config.repetition:
                is_repetition = await repetition_checker.question_check_repetition(survey_response)
            else:
                is_repetition = False

            try:
                # Initialize the probe
                running_probe = None
                cached_probe_state = await load_probe_state(
                    str(survey_response.su_id), str(survey_response.qs_id), str(survey_response.mo_id)
                )
                session_no = int(cached_probe_state.get("session_no", 0))

                # If repetition is detected, send the default response payloads over the websocket
                if is_repetition:
                    await websocket.send_json({
                        "error": False,
                        "message": "streaming-started",
                        "code": 200,
                        "response": {
                            "question": "",
                            "min_probing": question.config.probes,
                            "max_probing": question.config.max_probes,
                            "is_repetition": True,
                        }
                    })
                    await websocket.send_json({
                        "error": False,
                        "message": "streaming-ended",
                        "code": 200,
                        "response": {
                            "question": "",
                            "min_probing": question.config.probes,
                            "max_probing": question.config.max_probes,
                            "is_repetition": True,
                        }
                    })
                    running_probe = Probe(mo_id=survey_response.mo_id, metadata=survey, question=question, simple_store=True, session_no=session_no, survey_details=survey_response)
                    await running_probe.init()
                    continue
                
                running_probe = Probe(mo_id=survey_response.mo_id, metadata=survey, question=question, simple_store=True, session_no=session_no, survey_details=survey_response)
                await running_probe.init()

                # Generate follow-up using the probe
                stream, immediate_coro, detailed_coro = running_probe.gen_streamed_follow_up(survey_response.question, survey_response.response)
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

                immediate_task = asyncio.create_task(immediate_coro)
                detailed_task = asyncio.create_task(detailed_coro)
                
                queue = asyncio.Queue()
                
                async def consume_stream():
                    try:
                        async for chunk in stream:
                            await queue.put(chunk)
                        await queue.put(None)
                    except asyncio.CancelledError:
                        await queue.put(None)
                
                stream_task = asyncio.create_task(consume_stream())

                # Await ONLY the immediate metrics first to minimize TTFT
                immediate_metric = await immediate_task
                
                metric = {}
                if isinstance(immediate_metric, dict):
                    metric.update(immediate_metric)
                elif hasattr(immediate_metric, "model_dump"):
                    metric.update(immediate_metric.model_dump())
                    
                is_gibberish = metric.get("gibberish_score", 0) > running_probe.question.config.gibberish_score
                
                # If gibberish, cancel the stream task immediately to save tokens
                if is_gibberish:
                    stream_task.cancel()
                    detailed_task.cancel()
                    
                # Check for relevance threshold and update prompt if needed
                RelevanceChecker.check_and_update_prompt(running_probe, metric)

                final_response["message"] = "streaming-started"
                final_response["response"] = {
                    **final_response["response"],
                    "ended": running_probe.ended, # Default to probe state initially
                    "metrics": metric,
                    "is_gibberish": is_gibberish,
                    "is_repetition": is_repetition,
                }
                
                await websocket.send_json(final_response)

                if not is_gibberish:
                    while True:
                        chunk = await queue.get()
                        if chunk is None:
                            break
                        
                        # Opportunistically inject detailed metrics into the stream payload if it finishes early
                        if detailed_task.done() and "quality" not in metric and not detailed_task.cancelled():
                            try:
                                detailed_metric = detailed_task.result()
                                if isinstance(detailed_metric, dict):
                                    metric.update(detailed_metric)
                                elif hasattr(detailed_metric, "model_dump"):
                                    metric.update(detailed_metric.model_dump())
                                final_response["response"]["metrics"] = metric
                                final_response["response"]["ended"] = True if metric.get("quality", 0) >= running_probe.question.config.quality_threshold else False
                            except Exception as e:
                                logger.error(f"Error getting detailed_task result: {e}")

                        final_response["message"] = "streaming"
                        final_response["response"]["question"] = chunk.content if hasattr(chunk, 'content') else str(chunk)
                        
                        await websocket.send_json(final_response)

                # Ensure detailed metrics have finished before sending streaming-ended
                if not is_gibberish and not detailed_task.done() and not detailed_task.cancelled():
                    detailed_metric = await detailed_task
                    if isinstance(detailed_metric, dict):
                        metric.update(detailed_metric)
                    elif hasattr(detailed_metric, "model_dump"):
                        metric.update(detailed_metric.model_dump())
                    final_response["response"]["metrics"] = metric
                    final_response["response"]["ended"] = True if metric.get("quality", 0) >= running_probe.question.config.quality_threshold else False

                ended_response = final_response.copy()
                ended_response["message"] = "streaming-ended"
                ended_response["response"]["question"] = ""
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
