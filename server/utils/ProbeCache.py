import json
from typing import Optional, Dict, Any
from redis.asyncio import Redis
import logging

logger = logging.getLogger(__name__)


class ProbeCache:
    
    def __init__(self, redis_client: Redis, ttl: int = 3600):
        self.redis = redis_client
        self.ttl = ttl
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        try:
            data = await self.redis.get(f"probe:{key}")
            if data:
                return json.loads(data) 
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None
    
    async def set(self, key: str, state: Dict[str, Any]) -> bool:
        try:
            await self.redis.setex(
                f"probe:{key}",
                self.ttl,
                json.dumps(state)
            )
            return True
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete probe state from cache"""
        try:
            await self.redis.delete(f"probe:{key}")
            return True
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False
    
    async def total_probe(self) -> int:
        try:
            return len(await self.redis.keys("probe:*"))
        except Exception as e:
            logger.error(f"Cache total_probe error: {e}")
            return 0