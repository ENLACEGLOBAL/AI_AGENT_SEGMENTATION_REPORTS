from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.rta_api.api.v1 import reports, sector_ubicacion, cruces

app = FastAPI(
    title="Risk Reports API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(reports.router)
app.include_router(sector_ubicacion.router)
app.include_router(cruces.router)
