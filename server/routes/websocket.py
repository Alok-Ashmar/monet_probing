import json
import websockets
from typing import Dict
from bson import ObjectId
# import httpx
from models.Survey import SurveyResponse
from modules.MongoWrapper import monet_db
from modules.ServerLogger import ServerLogger
# from modules.ProdProbe_v2 import Probe, NSIGHT_v2
# from models.Survey import PySurvey, PySurveyQuestion  

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from utils.db_switcher import get_survey_config, save_probe_response
websocket_router = APIRouter(prefix="/ws", tags=["websocket", "ai-qa"])
logger = ServerLogger()

    : Dict[str, WebSocket] = {}
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

            try:
                # fetch data from db switcher 
                survey_config = await get_survey_config(
                    su_id=survey_response.su_id,
                    qs_id=survey_response.qs_id,
                    mo_id=survey_response.mo_id,
                    db_type="mongo",
                    redis_client=None
                )
                print(
                    survey_config,
                    "this is survey config"
                )
                # connect to websocket to probing microservice and collect response
                parsed_response = {}
                async with websockets.connect("ws://localhost:8000/ws/ai-qa") as ws_client:
                    await ws_client.send(data)  

                    async for message in ws_client:
                        if isinstance(message, str):
                            raw_message = message
                            await websocket.send_text(message)
                        else:
                            raw_message = message.decode('utf-8') if hasattr(message, 'decode') else message
                            await websocket.send_bytes(message)
                        
                        try:
                            parsed_response = json.loads(raw_message)
                        except Exception as parse_error:
                            logger.error(f"Error parsing message: {parse_error}")
                            parsed_response = {"message": "error", "error": True}

                        # save response in db when message is streaming-ended
                        if parsed_response.get("message") == "streaming-ended":
                            print(
                                "inside streaming-ended"
                            )
                            await save_probe_response(
                                db_type="mongo", 
                                survey_response=survey_response, 
                                nsight_v2=parsed_response.get("response", {}), 
                                probe=None, 
                                session_no=1 
                            )
                            break
                


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