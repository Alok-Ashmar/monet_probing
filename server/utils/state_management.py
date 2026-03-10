import json
from utils.redis_pool import get_redis
from utils.ServerLogger import ServerLogger

logger = ServerLogger()


def _probe_state_key(su_id: str, qs_id: str, mo_id: str) -> str:
    """Build the Redis key for a probe's persisted state."""
    return f"probe_state:{su_id}:{qs_id}:{mo_id}"


async def load_probe_state(su_id: str, qs_id: str, mo_id: str) -> dict:
    """
    Load the persisted probe state from Redis.

    Returns an empty dict if the key does not exist or on error.
    """
    try:
        redis = get_redis()
        key = _probe_state_key(su_id, qs_id, mo_id)
        cached = await redis.get(key)
        if not cached:
            return {}
        return json.loads(cached)
    except Exception as e:
        logger.error("Failed to load probe state from Redis")
        logger.error(e)
        return {}


async def load_survey_details(su_id: str, qs_id: str) -> dict:
    """
    Load cached survey + question details from Redis.

    Returns the parsed dict, or raises on failure.
    """
    redis = get_redis()
    redis_key = f"survey_details:{su_id}:{qs_id}"
    raw = await redis.get(redis_key)
    return json.loads(raw)
