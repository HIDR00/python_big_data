import pandas as pd
from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
import numpy as np
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

client = AsyncIOMotorClient('mongodb+srv://21011228:Hidr0%21%21%21@cluster0.gpwjpwu.mongodb.net/movies?retryWrites=true&w=majority&appName=Cluster0')
db = client['movies']  
movieVoteAverageCollection = db['movieVoteAverage']
AppMovieVote = db['movieVote']

@app.get("/")
async def read_root():
    return {"Hello": "World"}


pipelineAverageRatingByGenre = [
    {"$match": {
        "genres": {"$exists": True, "$ne": None},
        "vote_average": {"$exists": True, "$ne": None}
    }},
    {"$project": {
        "genres": {"$split": ["$genres", ", "]},  
        "vote_average": 1
    }},
    {"$unwind": "$genres"},
    {"$group": {
        "_id": {"$trim": {"input": "$genres"}},
        "average_rating": {"$avg": "$vote_average"}
    }},
    {"$project": {
        "genre": "$_id",
        "average_rating": {"$round": ["$average_rating", 2]},  
        "_id": 0
    }},
    {"$sort": {"genre": 1}} 
]

pipelineRatingDistribution = [
    {"$match": {
        "vote_average": {"$exists": True, "$ne": None}
    }},
    {"$project": {
        "rounded_vote": {"$ceil": "$vote_average"}
    }},
    {"$match": {
        "rounded_vote": {"$gte": 1, "$lte": 10}
    }},
    {"$group": {
        "_id": "$rounded_vote",
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}},
    {"$project": {
        "rating": "$_id",
        "count": 1,
        "_id": 0
    }}
]

pipelineRatingOverTime = [
    {"$match": {
        "release_date": {"$exists": True, "$ne": None, "$type": "date"}, 
        "vote_average": {"$exists": True, "$ne": None}
    }},
    {"$project": {
        "release_year": {"$year": "$release_date"},
        "vote_average": 1
    }},
    {"$group": {
        "_id": "$release_year",
        "average_rating": {"$avg": "$vote_average"}
    }},
    {"$sort": {"_id": 1}},
    {"$project": {
        "release_year": "$_id",
        "average_rating": {"$round": ["$average_rating", 2]},
        "_id": 0
    }}
]

pipelineAppAverageRatingByGenre = [
    {"$unwind": "$genres"},
    {"$group": {
        "_id": "$genres",
        "average_rating": {"$avg": "$myVote"}
    }},
    {"$sort": {"_id": 1}},
    {"$project": {
        "genre": "$_id",
        "average_rating": {"$round": ["$average_rating", 2]},
        "_id": 0
    }}
]

pipelineCountMyVote = [
    {"$match": {
        "myVote": {"$gte": 1, "$lte": 10}
    }},
    {"$group": {
        "_id": "$myVote",
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}},
    {"$project": {
        "rating": "$_id",
        "count": 1,
        "_id": 0
    }}
]


class GenreAverage(BaseModel):
    genre: str
    average_rating: float

class RatingByYear(BaseModel):
    release_year: int
    average_rating: float

class RatingDistribution(BaseModel):
    rating: float
    count: int

@app.get("/average-rating-by-genre/", response_model=List[GenreAverage])
async def get_average_rating_by_genre():
    result = await movieVoteAverageCollection.aggregate(pipelineAverageRatingByGenre).to_list(None)
    return [GenreAverage(**item) for item in result]

@app.get("/rating-distribution/", response_model=List[RatingDistribution])
async def get_rating_distribution():
    result = await movieVoteAverageCollection.aggregate(pipelineRatingDistribution).to_list(None)
    return [RatingDistribution(**item) for item in result]

@app.get("/rating-over-time/", response_model=List[RatingByYear])
async def get_rating_over_time():
    result = await movieVoteAverageCollection.aggregate(pipelineRatingOverTime).to_list(None)
    return [RatingByYear(**item) for item in result]

@app.get("/app-average-rating-by-genre/", response_model=List[GenreAverage])
async def get_average_rating_by_genre():
    result = await AppMovieVote.aggregate(pipelineAppAverageRatingByGenre).to_list(None)
    return [GenreAverage(**item) for item in result]

@app.get("/app-rating-distribution/", response_model=List[RatingDistribution])
async def get_rating_distribution():
    result = await AppMovieVote.aggregate(pipelineCountMyVote).to_list(None)
    return [RatingDistribution(**item) for item in result]