from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
import os

app = FastAPI()
DB_PATH = "storage/new_events.csv"


class FilmingEvent(BaseModel):
    lat: float
    lon: float
    year: int
    film_title: str


@app.get("/")
def health():
    return {"status": "cinematic-paris api online"}


@app.post("/ingest")
def ingest(events: List[FilmingEvent]):
    df = pd.DataFrame([e.model_dump() for e in events])

    os.makedirs("storage", exist_ok=True)

    df.to_csv(
        DB_PATH,
        mode="a",
        index=False,
        header=not os.path.exists(DB_PATH)
    )

    return {"inserted": len(df)}