import os
from fastapi import FastAPI
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

# -- Load environment variables
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

# - routes
from routes.websocket import websocket_router, probes

description = """
Monet-Intern-Effort
Server function

This is *intern AI server that exposing various intelligent services**.
"""

# FastAPI app initialization
app = FastAPI(
    root_path="/v7/" if os.environ.get("ENV") == "production" else "",
    title="Monet Networks AI Server",
    description=description,
    summary="Monet Networks AI server that exposes interface to various AI based models.",
    version="2.0.0",
    terms_of_service="https://www.monetanalytics.com/#/terms-and-conditions",
    contact={
        "name": "Monet Networks Inc.",
        "url": "https://www.monetanalytics.com/#/contact-us",
        "email": "mayank.malkoti@ashmar.in",
    },
)

# CORS Middleware
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(websocket_router)

# Health check endpoint
@app.get("/health")
def health_check():
    """Health check for WebSocket handler"""
    # Inspect the probes dict from the websocket router for active probe sessions.
    total_probes = len(probes)
    websocket_status = "healthy" if total_probes >= 0 else "unhealthy"
    return {
        "websocket_status": websocket_status,
        "active_probe_sessions": total_probes
    }