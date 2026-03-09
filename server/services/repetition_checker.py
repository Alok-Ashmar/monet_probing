import os
import re
import json
from redis import Redis
from models.schemas import SurveyResponse
from services.ServerLogger import ServerLogger

logger = ServerLogger()


class RepetitionChecker:
    """
    Checks whether a user's current response is a repetition of
    any of their previous responses stored in Redis.

    Redis key structure:
        message_store:{su_id}:{mo_id}:{qs_id}:{index}

    Only the key with the highest index (most recent snapshot) is read.
    """

    def __init__(self):
        """Initialise the Redis client from the REDIS_URL environment variable."""
        self.redis_client = Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        )

    def _read_key(self, key: bytes) -> list | dict | None:
        """
        Read a Redis key using the appropriate command for its data type.

        Supports: string, list, hash, set.

        Args:
            key: The raw Redis key in bytes.

        Returns:
            Parsed Python object (list, dict, or None).
        """
        key_type = self.redis_client.type(key).decode("utf-8")

        if key_type == "string":
            raw = self.redis_client.get(key)
            return json.loads(raw) if raw else None

        elif key_type == "list":
            items = self.redis_client.lrange(key, 0, -1)
            result = []
            for item in items:
                try:
                    result.append(json.loads(item))
                except Exception:
                    result.append(item.decode("utf-8") if isinstance(item, bytes) else item)
            return result

        elif key_type == "hash":
            raw = self.redis_client.hgetall(key)
            return {
                (k.decode("utf-8") if isinstance(k, bytes) else k): (
                    json.loads(v) if v else None
                )
                for k, v in raw.items()
            }

        elif key_type == "set":
            items = self.redis_client.smembers(key)
            return [json.loads(item) if item else None for item in items]

        else:
            logger.error(f"Unsupported Redis key type '{key_type}' for key: {key}")
            return None

    @staticmethod
    def _trailing_index(key: bytes) -> int:
        """
        Extract the trailing numeric index from a Redis key.

        Example:
            b'message_store:15461:1750137:101310:2'  →  2

        Args:
            key: The raw Redis key in bytes.

        Returns:
            Integer index, or -1 if the suffix is not numeric.
        """
        try:
            return int(key.decode("utf-8").rsplit(":", 1)[-1])
        except (ValueError, AttributeError):
            return -1

    @staticmethod
    def _extract_content(raw: str) -> str:
        """
        Strip the auto-injected 'Response N. ' prefix from a message content string.

        Example:
            'Response 7. the intensity of the fight'  →  'the intensity of the fight'

        Args:
            raw: The raw content string from the Redis message.

        Returns:
            Cleaned content string.
        """
        return re.sub(r"^Response\s+\d+\.\s*", "", raw).strip()

    def _check_repetition(self, survey_response: SurveyResponse, pattern: str) -> bool:
        """
        Determine whether the user's current response is a repeat of a
        previous response stored in Redis for the given pattern.

        Steps:\n
            1. Scan Redis for all keys matching the pattern.
            2. Select the key with the highest trailing index (most recent session).
            3. Read and parse that key.
            4. Filter to human-authored messages only.
            5. Compare the current response against cleaned content strings.

        Args:
            survey_response: The incoming survey response object.
            pattern: The Redis key pattern to scan.

        Returns:
            True  – if the current response already exists in the message history.
            False – if it is new, or if the message store could not be read.
        """
        # 1. Scan for matching keys
        try:
            matched_keys = list(self.redis_client.scan_iter(pattern))
        except Exception as e:
            logger.error(f"Failed to scan Redis keys for pattern: {pattern}")
            logger.error(str(e))
            return False

        if not matched_keys:
            return False

        # 2. Pick the most recent key (highest trailing index)
        latest_key = max(matched_keys, key=self._trailing_index)

        # 3. Read the key
        try:
            msgs = self._read_key(latest_key)
        except Exception as e:
            logger.error(f"Failed to read Redis key: {latest_key}")
            logger.error(str(e))
            return False

        # 4. Filter to human messages only
        human_msgs = [
            msg for msg in (msgs or [])
            if isinstance(msg, dict) and msg.get("type") == "human"
        ]

        # 5. Build cleaned list of past response contents
        past_responses = [
            self._extract_content(msg["data"]["content"])
            for msg in human_msgs
            if msg.get("data", {}).get("content")
        ]

        # 6. Check for repetition
        return survey_response.response in past_responses

    def survey_check_repetition(self, survey_response: SurveyResponse) -> bool:
        """
        Determine whether the user's current response is a repeat of a
        previous response stored in Redis at the user level across the survey.
        """
        pattern = f"message_store:{survey_response.su_id}:{survey_response.mo_id}:*"
        return self._check_repetition(survey_response, pattern)

    def question_check_repetition(self, survey_response: SurveyResponse) -> bool:
        """
        Determine whether the user's current response is a repeat of a
        previous response stored in Redis for the specific question.
        """
        pattern = f"message_store:{survey_response.su_id}:{survey_response.mo_id}:{survey_response.qs_id}:*"
        return self._check_repetition(survey_response, pattern)
