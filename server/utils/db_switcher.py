import json
import pytz
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from bson import ObjectId
from sqlalchemy import text
from redis.asyncio import Redis
from utils.helper import Helper

from modules.ServerLogger import ServerLogger
# from database.SQL_Wrapper import AsyncSessionLocal

logger = ServerLogger()
helper = Helper()

async def get_survey_config(
    su_id: str,
    qs_id: str,
    mo_id: str,
    redis_client: Redis,
    db_type: str | None = None,
    ttl: int = 86400
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:

    cache_key = f"survey_details:{su_id}:{qs_id}"
    
    try:
        if redis_client:
            cached = await redis_client.get(cache_key)
            if cached:
                return json.loads(cached), None

        if db_type == "mongo" or helper._is_object_id(su_id):
            config = await _fetch_mongo(su_id, qs_id)
        elif db_type == "mysql" or helper._is_int_id(su_id):
            config = await _fetch_mysql(su_id, qs_id)
        else:
            raise ValueError("Invalid db_type")
            
        if redis_client:
            await redis_client.setex(cache_key, ttl, json.dumps(config))
        return config, None

    except Exception as e:
        logger.error(f"Error fetching survey config: {e}")
        return None, {"error": True, "message": str(e), "code": 500}


async def _fetch_mongo(su_id: str, qs_id: str) -> Dict[str, Any]:
    from modules.MongoWrapper import monet_db
    
    survey = monet_db.get_collection("surveys").find_one({"_id": ObjectId(su_id)})
    question = monet_db.get_collection("survey-questions").find_one({"_id": ObjectId(qs_id)})
    
    if not survey or not question:
        raise ValueError("Survey or question not found in MongoDB")
    
    return {
        "survey": {
            "survey_description": survey.get("description", "-"),
            "language": survey.get("config", {}).get("language", "English"),
            "add_context": survey.get("config", {}).get("add_context", True),
        },
        "question": {
            "question": question.get("question", ""),
            "question_description": question.get("description", ""),
            "min_probe": question.get("config", {}).get("probes", 0),
            "max_probe": question.get("config", {}).get("max_probes", 0),
            "quality_threshold": question.get("config", {}).get("quality_threshold", 4),
            "gibberish_score": question.get("config", {}).get("gibberish_score", 4),
            "add_context": question.get("config", {}).get("add_context", True),
        }
    }


async def _fetch_mysql(su_id: str, qs_id: str) -> Dict[str, Any]:
    # async with AsyncSessionLocal() as session:
    #     # Fetch Survey
    #     result = await session.execute(
    #         text("SELECT * FROM test_study WHERE study_id = :su_id"),
    #         {"su_id": su_id}
    #     )
    #     survey_row = result.mappings().first()
        
    #     # Fetch Question
    #     result = await session.execute(
    #         text("SELECT * FROM probe_survey_question WHERE qs_id = :qs_id AND su_id = :su_id"),
    #         {"qs_id": qs_id, "su_id": su_id}
    #     )
    #     question_row = result.mappings().first()
        
    #     if not survey_row or not question_row:
    #         raise ValueError("Survey or question not found in MySQL")
            
    #     global_flags = json.loads(survey_row.get("global_flags") or "{}")
    #     question_config = json.loads(question_row.get("config") or "{}")
        
    #     return {
    #         "survey": {
    #             "survey_description": global_flags.get("survey_description", "-"),
    #             "language": global_flags.get("language", "English"),
    #             "add_context": True, # Default for SQL
    #         },
    #         "question": {
    #             "question": question_row.get("question", ""),
    #             "question_description": question_row.get("description", ""),
    #             "min_probe": question_config.get("probes", 0),
    #             "max_probe": question_config.get("max_probes", 0),
    #             "quality_threshold": question_config.get("quality_threshold", 4),
    #             "gibberish_score": question_config.get("gibberish_score", 4),
    #             "add_context": question_config.get("add_context", True),
    #         }
    #     }
    pass


async def save_probe_response(
    survey_response: Any,
    nsight_v2: Any,
    probe: Any,
    db_type: str | None = None,
    session_no: int = 0
):
    """Save probe interaction to DB"""
    if db_type == "mongo" or helper._is_object_id(survey_response.su_id):
        await _save_mongo(survey_response, nsight_v2, probe, session_no)
    elif db_type == "mysql" or helper._is_int_id(survey_response.su_id):
        await _save_mysql(survey_response, nsight_v2, probe)
    else:
        raise ValueError("Invalid db_type")


async def _save_mongo(survey_response: Any, nsight_v2: Any, probe: Any, session_no: int):
    from modules.MongoWrapper import monet_db_test

    QnAs = monet_db_test.get_collection("QnAs")
    
    data_to_save = survey_response.model_dump() if hasattr(survey_response, "model_dump") else survey_response
    metrics_container = nsight_v2.model_dump() if hasattr(nsight_v2, "model_dump") else nsight_v2
    metrics = metrics_container.get("metrics", {})
    
    print(
        metrics,
        "this is metrics",
        data_to_save,
        "this is data_to_save"
    )

    metadata = {
        "mo_id": probe.mo_id if (probe and hasattr(probe, 'mo_id')) else getattr(survey_response, 'mo_id', None),
        "su_id": probe.su_id if (probe and hasattr(probe, 'su_id')) else getattr(survey_response, 'su_id', None),
        "qs_id": probe.qs_id if (probe and hasattr(probe, 'qs_id')) else getattr(survey_response, 'qs_id', None),
        "quality": metrics.get("quality", 0),
        "relevance": metrics.get("relevance", 0),
        "confusion": metrics.get("confusion", 0),
        "detail": metrics.get("detail", 0),
        "annoyed": metrics.get("annoyed", 0),
        "session_no": session_no,
        "keywords": metrics.get("keywords", []),
        "created_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
    }
    
    QnAs.insert_one({**data_to_save, **metadata})


async def _save_mysql(survey_response: Any, nsight_v2: Any, probe: Any):
    from models.sql.models import SurveyResponseTest
    async with AsyncSessionLocal() as session:
        new_res = SurveyResponseTest(
            su_id=survey_response.su_id,
            mo_id=survey_response.mo_id,
            qs_id=survey_response.qs_id,
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
            session_no=probe.session_no,
        )
        session.add(new_res)
        await session.commit()
