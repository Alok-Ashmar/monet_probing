from typing import Callable
from langchain_core.prompts import PromptTemplate


def _intent_key(survey_details: dict, logger) -> str:
    try:
        question_key = f"{survey_details.su_id}_{survey_details.qs_id}"
    except Exception as e:
        question_key = ""
        logger.error(f"Failed to create intent key from survey details: {e}")
    return question_key


def _get_intent(redis_client, survey_details: dict, ttl_seconds: int, logger) -> str | None:
    try:
        key = _intent_key(survey_details, logger)
        cached = redis_client.get(key)
        if cached is None:
            return None
        try:
            redis_client.expire(key, ttl_seconds)
        except Exception as exc:
            logger.error(f"refresh_intent_ttl failed: {exc}")
        if isinstance(cached, bytes):
            cached = cached.decode("utf-8", errors="ignore")
        return str(cached)
    except Exception as exc:
        logger.error(f"get_intent failed: {exc}")
        return None


def _store_intent(redis_client, ttl_seconds: int, survey_details: dict, intent: str, logger) -> None:
    try:
        key = _intent_key(survey_details, logger)
        redis_client.setex(key, ttl_seconds, intent)
    except Exception as exc:
        logger.error(f"store_intent failed: {exc}")


def extract_intent(
    question_description: str,
    question_text: str,
    survey_details: dict,
    invoke_fn: Callable[[PromptTemplate, dict[str, str]], str],
    logger,
    redis_client,
    ttl_seconds: int,
) -> str:

    intent = (question_description or "").strip()
    if not intent:
        return ""

    cached = _get_intent(redis_client, survey_details, ttl_seconds, logger)
    if cached:
        return cached

    prompt = PromptTemplate(
        template="""
            You are given a survey question and description explaining its purpose. Identify the underlying intent the question is trying to understand from the respondent.
            Write clear sentence that summarizes what the question aims to learn. Do not include quotes or extra text.
            
            Question: {question_text}
            
            Intent: {intent}
            
            Intent:
        """.strip()
    )

    try:
        intent = invoke_fn(prompt, {"intent": intent, "question_text": question_text})
    except Exception as exc:
        logger.error(f"extract_intent failed: {exc}")
        _store_intent(redis_client, ttl_seconds, survey_details, intent, logger)
        return intent

    if isinstance(intent, str):
        final_intent = intent.strip()
    else:
        final_intent = getattr(intent, "content", str(intent)).strip()

    _store_intent(redis_client, ttl_seconds, survey_details, final_intent, logger)
    return final_intent
