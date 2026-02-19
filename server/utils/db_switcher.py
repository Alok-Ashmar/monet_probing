import json
from typing import Any, Dict, Optional, Tuple

def _normalize_db_type(db_type: Optional[str], logger: Any = None) -> str:
    if db_type:
        return db_type.strip().lower()
    if logger:
        logger.warning("db_type is not set. Defaulting to mongo.")
    return "mongo"

async def fetch_survey_question(
    *,
    db_type: Optional[str],
    survey_response: Any,
    db: Any = None,
    logger: Any = None
) -> Tuple[Optional[Any], Optional[Any], Optional[Dict[str, Any]]]:
    """
    Returns (survey, question, error_dict).
    error_dict is None when both survey and question are found.
    """
    db_type_norm = _normalize_db_type(db_type, logger)

    if db_type_norm in {"mongo", "mongodb", ""}:
        from bson import ObjectId
        from server.models.MongoWrapper import monet_db
        from server.models.Survey import PySurvey, PySurveyQuestion, PdSurvey, PdSurveyQuestion, SurveyConfig, QuestionConfig

        db_survey = monet_db.get_collection("surveys")
        db_question = monet_db.get_collection("survey-questions")

        survey_doc = db_survey.find_one({"_id": ObjectId(survey_response.su_id)})
        if not survey_doc:
            return None, None, {
                "error": True, 
                "message": "Survey not found", 
                "code": 404
            }

        question_doc = db_question.find_one({"_id": ObjectId(survey_response.qs_id)})
        if not question_doc:
            return None, None, {
                "error": True, 
                "message": "Question not found",
                "code": 404
            }

        survey = PySurvey(**survey_doc)
        question = PySurveyQuestion(**question_doc)

        # Normalize to common structure using PdSurvey/PdSurveyQuestion
        normalized_survey = PdSurvey(
            id=None,
            study_id=None,
            survey_description=survey.description,
            survey_title=survey.title,
            config=SurveyConfig(
                language=survey.config.language
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
            ),
        )

        return normalized_survey, normalized_question, None

    if db_type_norm in {"mysql", "sql"}:
        from sqlalchemy import text
        from server.models.sql.db import AsyncSessionLocal
        from server.models.Survey import SurveyConfig, QuestionConfig, PdSurvey, PdSurveyQuestion

        async def _run_queries(session):
            query_survey = text(
                "SELECT * FROM test_study WHERE study_id = :su_id"
            )
            result = await session.execute(
                query_survey,
                {
                    "su_id": survey_response.su_id
                }
            )
            survey_row = result.mappings().first()
            if not survey_row:
                return None, None, {
                    "error": True,
                    "message": "Survey not found",
                    "code": 404
                }

            global_flags = json.loads(survey_row.get("global_flags") or "{}")

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
                id=survey_row.get("id"),
                study_id=survey_row.get("study_id"),
                cnt_id=survey_row.get("cnt_id"),
                survey_description=global_flags.get("survey_description", "-"),
                survey_title=survey_row.get("study_name") or survey_row.get("cell_name") or survey_row.get("survey_title"),
                # service=survey.service,
                # llm=global_flags.get("llm"),
                # language=global_flags.get("language"),
                # add_context=global_flags.get("add_context"),
                config=survey_config
            )

            query_question = text(
                "SELECT * FROM probe_survey_question WHERE qs_id = :qs_id AND su_id = :su_id"
            )
            result = await session.execute(
                query_question,
                {
                    "su_id": survey_response.su_id,
                    "qs_id": survey_response.qs_id
                },
            )
            question_row = result.mappings().first()
            if not question_row:
                return None, None, {
                    "error": True,
                    "message": "Question not found",
                    "code": 404
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

    raise ValueError(f"Unsupported db_type: {db_type}")


if __name__ == "__main__":
    import argparse
    import asyncio
    import os
    import sys
    from dotenv import load_dotenv

    # Ensure project root is on sys.path when running as a script
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    env_path = os.path.join(project_root, "server", ".env")
    load_dotenv(env_path)

    class _SurveyResponseStub:
        def __init__(self, su_id: str, qs_id: str):
            self.su_id = su_id
            self.qs_id = qs_id

    parser = argparse.ArgumentParser(description="Fetch survey and question by DB type.")
    parser.add_argument("--db-type", required=True, help="mongo or mysql")
    parser.add_argument("--su-id", required=True, help="Survey ID")
    parser.add_argument("--qs-id", required=True, help="Question ID")
    args = parser.parse_args()

    async def _main():
        survey_response = _SurveyResponseStub(args.su_id, args.qs_id)
        survey, question, error = await fetch_survey_question(
            db_type=args.db_type,
            survey_response=survey_response,
            db=None,
            logger=None,
        )
        if error:
            print(error)
            return
        
        output = {
            "survey": {
                "survey_description": survey.survey_description,
                "language": survey.config.language,
            },
            "question": {
                "question": question.question,
                "question_description": question.description,
                "min_probe": question.config.probes,
                "max_probe": question.config.max_probes,
                "quality_threshold": question.config.quality_threshold,
                "gibberish_score": question.config.gibberish_score,
            },
        }
        print(output)

    asyncio.run(_main())
