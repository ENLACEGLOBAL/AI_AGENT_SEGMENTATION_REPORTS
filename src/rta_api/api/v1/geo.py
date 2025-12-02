import json
import urllib.request
from fastapi import APIRouter, Depends
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/geo", tags=["geo"])

COLOMBIA_URL = "https://raw.githubusercontent.com/johnguerra/colombia-geojson/master/colombia.json"
WORLD_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"

def fetch(url: str):
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read().decode())

@router.get("/colombia")
def colombia(claims: dict = Depends(require_jwt)):
    return fetch(COLOMBIA_URL)

@router.get("/world")
def world(claims: dict = Depends(require_jwt)):
    return fetch(WORLD_URL)
