<<<<<<< HEAD
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
=======
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, Protocol, Tuple, TypedDict, TYPE_CHECKING

import pytz

if TYPE_CHECKING:
    from models.Survey import PdSurvey, PdSurveyQuestion  # type: ignore


class ErrorDict(TypedDict):
    """Standard error payload for fetch failures."""

    error: bool
    message: str
    code: int


class SurveyResponseLike(Protocol):
    """Minimum interface required for survey response input."""

    su_id: str
    qs_id: str


if TYPE_CHECKING:
    FetchResult = Tuple[Optional[PdSurvey], Optional[PdSurveyQuestion], Optional[ErrorDict]]
else:
    FetchResult = Tuple[Optional[Any], Optional[Any], Optional[ErrorDict]]


class MongoSurveyRepository:
    """MongoDB data access for surveys and questions."""

    async def fetch_survey_question(
        self,
        survey_response: SurveyResponseLike,
    ) -> FetchResult:
        """Fetch survey and question from MongoDB and normalize to Pydantic models."""
        from bson import ObjectId
        from modules.MongoWrapper import monet_db
        from models.Survey import (
            PySurvey,
            PySurveyQuestion,
            QuestionConfig,
            SurveyConfig,
            PdSurvey,
            PdSurveyQuestion,
        )

        db_survey = monet_db.get_collection("surveys")
        db_question = monet_db.get_collection("survey-questions")

        survey_doc = db_survey.find_one({"_id": ObjectId(survey_response.su_id)})
        if not survey_doc:
            return None, None, {
                "error": True,
                "message": "Survey not found",
                "code": 404,
            }

        question_doc = db_question.find_one({"_id": ObjectId(survey_response.qs_id)})
        if not question_doc:
            return None, None, {
                "error": True,
                "message": "Question not found",
                "code": 404,
            }

        survey = PySurvey(**survey_doc)
        question = PySurveyQuestion(**question_doc)

        normalized_survey = PdSurvey(
            id=None,
            study_id=None,
            survey_description=survey.description,
            survey_title=survey.title,
            config=SurveyConfig(
                language=survey.config.language,
                add_context=survey.config.add_context,
            ),
        )
        normalized_question = PdSurveyQuestion(
            question=question.question,
            description=question.description,
            config=QuestionConfig(
                probes=question.config.probes,
                max_probes=question.config.max_probes,
                quality_threshold=question.config.quality_threshold,
                gibberish_score=question.config.gibberish_score,
                add_context=question.config.add_context,
            ),
        )

        return normalized_survey, normalized_question, None

    def store_response(
        self,
        *,
        nsight_v2: Any,
        probe: Any,
        session_no: int,
        logger: Any = None,
    ) -> Any:
        """Store probe response in MongoDB."""
        from modules.MongoWrapper import monet_db_test  # type: ignore

        india = pytz.timezone("Asia/Kolkata")
        now_india = datetime.now(india)
        QnAs = monet_db_test.get_collection("QnAs")
        insert_one_res = QnAs.insert_one({
            **nsight_v2.model_dump(),
            "ended": probe.ended,
            "mo_id": probe.mo_id,
            "su_id": probe.su_id,
            "qs_id": probe.qs_id,
            "qs_no": probe.counter + 1,
            "created_at": now_india.isoformat(),
            "session_no": session_no,
        })
        if logger:
            logger.info("Inserted one doc successfully")
            logger.info(insert_one_res)
        return insert_one_res


class MySQLSurveyRepository:
    """MySQL data access for surveys and questions."""

    async def fetch_survey_question(
        self,
        survey_response: SurveyResponseLike,
        db: Any = None,
    ) -> FetchResult:
        """Fetch survey and question from MySQL via SQLAlchemy."""
        from sqlalchemy import text
        from modules.SQL_Wrapper import AsyncSessionLocal
        from models.Survey import (
            QuestionConfig,
            SurveyConfig,
            PdSurvey,
            PdSurveyQuestion,
        )

        async def _run_queries(session: Any) -> FetchResult:
            query_survey = text(
                "SELECT * FROM test_study WHERE study_id = :su_id"
            )
            result = await session.execute(
                query_survey,
                {
                    "su_id": survey_response.su_id,
                },
            )
            survey_row = result.mappings().first()
            if not survey_row:
                return None, None, {
                    "error": True,
                    "message": "Survey not found",
                    "code": 404,
                }

            global_flags = json.loads(survey_row.get("global_flags") or "{}")

            survey_config = SurveyConfig(
                language=global_flags.get("language", "English"),
            )

            survey = PdSurvey(
                id=survey_row.get("id"),
                study_id=survey_row.get("study_id"),
                cnt_id=survey_row.get("cnt_id"),
                survey_description=global_flags.get("survey_description", "-"),
                survey_title=survey_row.get("study_name")
                or survey_row.get("cell_name")
                or survey_row.get("survey_title"),
                config=survey_config,
            )

            query_question = text(
                "SELECT * FROM probe_survey_question "
                "WHERE qs_id = :qs_id AND su_id = :su_id"
            )
            result = await session.execute(
                query_question,
                {
                    "su_id": survey_response.su_id,
                    "qs_id": survey_response.qs_id,
                },
            )
            question_row = result.mappings().first()
            if not question_row:
                return None, None, {
                    "error": True,
                    "message": "Question not found",
                    "code": 404,
                }

            parse_config = json.loads(question_row.get("config") or "{}")
            question_config = QuestionConfig(**parse_config)
            question = PdSurveyQuestion(
                id=question_row.get("id"),
                su_id=question_row.get("su_id"),
                cnt_id=question_row.get("cnt_id"),
                question=question_row.get("question"),
                description=question_row.get("description"),
                seq_num=question_row.get("seq_num"),
                config=question_config,
            )
            return survey, question, None

        if db is not None:
            return await _run_queries(db)

        async with AsyncSessionLocal() as session:
            return await _run_queries(session)

    async def store_response(
        self,
        *,
        nsight_v2: Any,
        survey_response: Any,
        probe: Any,
        db: Any = None,
    ) -> Any:
        """Store probe response in MySQL."""
        from models.sql.models import SurveyResponseTest
        from modules.SQL_Wrapper import AsyncSessionLocal

        new_survey_response = SurveyResponseTest(
            su_id=survey_response.su_id,
            mo_id=survey_response.mo_id,
            qs_id=survey_response.qs_id,
            cnt_id=survey_response.cnt_id,
>>>>>>> 9288b782cee2c14fb72674c36545d94c1b069e05
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
<<<<<<< HEAD
        session.add(new_res)
        await session.commit()
=======
        if db is not None:
            db.add(new_survey_response)
            await db.commit()
            return new_survey_response

        async with AsyncSessionLocal() as session:
            session.add(new_survey_response)
            await session.commit()
            return new_survey_response


class DBSwitcher:
    """Fetch survey and question data from the configured database backend."""

    def __init__(
        self,
        logger: Any = None,
        redis_url: Optional[str] = None,
        redis_ttl_survey: int = int(os.environ.get("REDIS_TTL_SECONDS_SURVEY", 86400)) # 24 hours,
    ) -> None:
        """Initialize the switcher with an optional logger."""
        self._logger = logger
        self._mongo = MongoSurveyRepository()
        self._mysql = MySQLSurveyRepository()
        self._redis_url = redis_url or os.environ.get(
            "REDIS_URL",
            "redis://localhost:6379/0",
        )
        self._redis_ttl_survey = redis_ttl_survey

    def _normalize_db_type(self, db_type: Optional[str]) -> str:
        """Normalize DB type and default to mongo when not provided."""
        if db_type:
            return db_type.strip().lower()
        if self._logger:
            self._logger.error("db_type is not set. Defaulting to mongo.")
        return "mongo"

    async def fetch_survey_question(
        self,
        *,
        db_type: Optional[str],
        survey_response: SurveyResponseLike,
        db: Any = None,
    ) -> FetchResult:
        """
        Return (survey, question, error_dict).

        error_dict is None when both survey and question are found.
        """
        db_type_norm = self._normalize_db_type(db_type)

        if db_type_norm in {"mongo", "mongodb", ""}:
            return await self._mongo.fetch_survey_question(survey_response)

        if db_type_norm in {"mysql", "sql"}:
            return await self._mysql.fetch_survey_question(survey_response, db)

        raise ValueError(f"Unsupported db_type: {db_type}")

    def build_output(
        self,
        survey: PdSurvey,
        question: PdSurveyQuestion,
    ) -> Dict[str, Dict[str, Any]]:
        """Build the full output payload for caching or API responses."""
        return {
            "survey": {
                "survey_description": survey.survey_description,
                "language": survey.config.language,
                "add_context": survey.config.add_context,
            },
            "question": {
                "question": question.question,
                "question_description": question.description,
                "min_probe": question.config.probes,
                "max_probe": question.config.max_probes,
                "quality_threshold": question.config.quality_threshold,
                "gibberish_score": question.config.gibberish_score,
                "add_context": question.config.add_context,
            },
        }

    def save_output_to_redis(
        self,
        *,
        output: Dict[str, Dict[str, Any]],
        su_id: str,
        qs_id: str,
    ) -> str:
        """Save output payload to Redis and return the key used."""
        from redis import Redis

        redis_client = Redis.from_url(self._redis_url)
        redis_key = f"survey_details:{su_id}:{qs_id}"
        redis_client.setex(redis_key, self._redis_ttl_survey, json.dumps(output))
        return redis_key

    async def fetch_and_cache_survey_details(
        self,
        *,
        db_type: Optional[str],
        survey_response: SurveyResponseLike,
        db: Any = None,
    ) -> Tuple[Optional[Dict[str, Dict[str, Any]]], Optional[ErrorDict]]:
        """
        Fetch survey/question, build output payload, and cache it in Redis.

        Returns (output, error_dict). output is None on error.
        """
        survey, question, error = await self.fetch_survey_question(
            db_type=db_type,
            survey_response=survey_response,
            db=db,
        )
        if error or not survey or not question:
            return None, error

        output = self.build_output(survey, question)
        self.save_output_to_redis(
            output=output,
            su_id=survey_response.su_id,
            qs_id=survey_response.qs_id,
        )
        return output, None

    async def simple_store_response(
        self,
        *,
        db_type: Optional[str],
        nsight_v2: Any,
        survey_response: Any,
        probe: Any,
        session_no: int,
        db: Any = None,
    ) -> Any:
        """Store probe response in Mongo or MySQL depending on db_type."""
        db_type_norm = self._normalize_db_type(db_type)
        if db_type_norm in {"mongo", "mongodb"}:
            return self._mongo.store_response(
                nsight_v2=nsight_v2,
                probe=probe,
                session_no=session_no,
                logger=self._logger,
            )
        if db_type_norm in {"mysql", "sql"}:
            return await self._mysql.store_response(
                nsight_v2=nsight_v2,
                survey_response=survey_response,
                probe=probe,
                db=db,
            )
        raise ValueError(f"Unsupported db_type: {db_type}")


if __name__ == "__main__":
    import sys
    import asyncio
    import argparse
    from dotenv import load_dotenv


    # Ensure project root is on sys.path when running as a script
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from modules.ServerLogger import ServerLogger # type: ignore

    env_path = os.path.join(project_root, "server", ".env")
    load_dotenv(env_path)

    class _SurveyResponseStub:
        """Lightweight survey response holder for CLI testing."""

        def __init__(self, su_id: str, qs_id: str):
            """Initialize with survey and question IDs."""
            self.su_id = su_id
            self.qs_id = qs_id

    parser = argparse.ArgumentParser(description="Fetch survey and question by DB type.")
    parser.add_argument("--db-type", required=True, help="mongo or mysql")
    parser.add_argument("--su-id", required=True, help="Survey ID")
    parser.add_argument("--qs-id", required=True, help="Question ID")
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Store output in Redis using configured REDIS_URL/TTL.",
    )
    args = parser.parse_args()

    async def _main():
        """CLI entry point to fetch and print survey/question data."""
        survey_response = _SurveyResponseStub(args.su_id, args.qs_id)
        switcher = DBSwitcher(logger=ServerLogger())
        if args.cache:
            output, error = await switcher.fetch_and_cache_survey_details(
                db_type=args.db_type,
                survey_response=survey_response,
                db=None,
            )
            if error:
                print(error)
                return
        else:
            survey, question, error = await switcher.fetch_survey_question(
                db_type=args.db_type,
                survey_response=survey_response,
                db=None,
            )
            if error:
                print(error)
                return
            output = switcher.build_output(survey, question)
        print(output)

    asyncio.run(_main())
>>>>>>> 9288b782cee2c14fb72674c36545d94c1b069e05
