from dotenv import load_dotenv
# Load environment variables FIRST
load_dotenv()

from fastapi import FastAPI
from contextlib import asynccontextmanager
from datetime import datetime
import os
from redis.asyncio import Redis
from fastapi.responses import FileResponse

from routes.websocket import websocket_router
from utils.ProbeCache import ProbeCache
from utils.ServerLogger import ServerLogger
from utils.ProbeCache import ProbeCache

logger = ServerLogger()

# Configuration
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
PROBE_TTL = int(os.environ.get("PROBE_TTL", "3600"))

# Global state
redis_client = None
probe_cache = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, probe_cache
    
    # Startup
    logger.info("booting application...", logger.boot)
    
    redis_client = Redis.from_url(REDIS_URL, decode_responses=False)
    await redis_client.ping()
    logger.info("Redis connected",logger.success)
    
    probe_cache = ProbeCache(redis_client, ttl=PROBE_TTL)
    logger.info(f"ProbeCache initialized (TTL: {PROBE_TTL}s)",logger.success)
    
    # Make available to other modules
    app.state.redis = redis_client
    app.state.probe_cache = probe_cache
    app.state.start_time = datetime.now()
    
    yield
    
    # Shutdown
    logger.info("Shutting down...",logger.danger)
    if redis_client:
        await redis_client.close()
    logger.info("Shutdown complete",logger.success)


app = FastAPI(lifespan=lifespan)
app.include_router(websocket_router)


@app.get("/")
def root():
    return {"status": "running"}

@app.get("/index")
def version():
    return FileResponse("view/index.html")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check for WebSocket handler"""
    # Inspect the probes dict from the websocket router for active probe sessions.
    try:
        total_probes = await probe_cache.total_probe()
        return {
            "websocket_status": f"healthy with {total_probes} active probe sessions",
            "active_probe_sessions": total_probes
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"websocket_status": "unhealthy", "active_probe_sessions": 0}
    