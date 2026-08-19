import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from app.config import env
from app.routes import reconciliation, storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("object-storage-reconciler")

_PUBLIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "public")

app = FastAPI(title="Object Storage & MySQL Reconciler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def on_http_error(request: Request, error: HTTPException):
    return JSONResponse(status_code=error.status_code, content={"status": False, "message": error.detail})


@app.exception_handler(RequestValidationError)
async def on_validation_error(request: Request, error: RequestValidationError):
    return JSONResponse(status_code=400, content={"status": False, "message": str(error)})


@app.exception_handler(Exception)
async def on_error(request: Request, error: Exception):
    logger.exception("[onError] %s", error)
    return JSONResponse(status_code=500, content={"status": False, "message": str(error)})


@app.get("/")
async def dashboard():
    return FileResponse(os.path.join(_PUBLIC_DIR, "index.html"))


@app.get("/api")
async def api_root():
    return {"status": True, "message": "Object Storage & MySQL Reconciler"}


app.include_router(storage.router)
app.include_router(reconciliation.router)


@app.on_event("startup")
async def on_startup():
    logger.info("Server running at http://localhost:%s", env.port)
