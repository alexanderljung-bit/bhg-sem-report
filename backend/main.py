"""
BHG SEM Report – FastAPI Backend
"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import bq_data


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize BigQuery client on startup."""
    bq_data.init_client()
    yield


app = FastAPI(title="BHG SEM Report API", lifespan=lifespan)

# CORS — allow Vercel frontend
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and register routers
from .routers import kpi, portfolio, sources, dates  # noqa: E402

app.include_router(kpi.router, prefix="/api")
app.include_router(portfolio.router, prefix="/api")
app.include_router(sources.router, prefix="/api")
app.include_router(dates.router, prefix="/api")


@app.get("/api/health")
def health():
    return {"status": "ok"}
