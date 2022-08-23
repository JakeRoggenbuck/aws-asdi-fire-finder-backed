import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime
from pydantic import BaseModel
from pprint import pprint
from fastapi import FastAPI
import uvicorn

app = FastAPI()


DATASETS = ["noaa-ghe-pds"]


class PredictionRequest(BaseModel):
    time_start: datetime
    time_end: datetime
    lat: float
    lon: float
    dataset: str


s3 = boto3.client(
    's3',
    'us-west-1',
    config=Config(signature_version=UNSIGNED),
)

pager = s3.get_paginator("list_objects_v2").paginate(
    Bucket='noaa-ghe-pds',
    Delimiter='/*',
)


def predict(pred_req: PredictionRequest):
    message = (
        f"Here is a prediction for {pred_req.lon}, {pred_req.lat}"
        + "from {pred_req.time_start} to {pred_req.time_end}."
    )
    return message


@app.get("/")
def read_root():
    return {"Hello": "ASDI"}


@app.get("/datasets")
def read_datasets():
    return DATASETS


@app.post("/predict")
def post_predict(prediction_request: PredictionRequest):
    return predict(prediction_request)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
