from sqlalchemy import Column, String, Float, Integer, UniqueConstraint
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from dataAccess.connect import connect

engine = create_engine('postgresql+psycopg2://', creator=connect)
Base = declarative_base()


class Management(Base):
    """"""
    __tablename__ = "management"

    description = Column(String, primary_key=True)
    value = Column(Float, nullable=False)

    # ----------------------------------------------------------------------
    def __init__(self, description, value):
        """"""
        self.description = description
        self.value = value


class AverageMovieRating(Base):
    """"""
    __tablename__ = "averageMovieRating"

    movieId = Column(Integer, primary_key=True)
    averageRating = Column(Float, nullable=False)

    # ----------------------------------------------------------------------
    def __init__(self, movieId, averageRating):
        """"""
        self.movieId = movieId
        self.averageRating = averageRating


class UsersSimilarity(Base):
    """"""
    __tablename__ = "usersSimilarity"
    usersSimilarityId = Column(Integer, primary_key=True)
    userId = Column(Integer, nullable=False)
    compareUserId = Column(Integer, nullable=False)
    similarity = Column(Float, nullable=False)
    UniqueConstraint(userId, compareUserId, name='users_similarity_ux_1')

    # ----------------------------------------------------------------------
    def __init__(self, userId, compareUserId, similarity):
        """"""
        self.userId = userId
        self.compareUserId = compareUserId
        self.similarity = similarity


class RatingsPredictions(Base):
    """"""
    __tablename__ = "ratingsPredictions"
    userId = Column(Integer, primary_key=True)
    movieId = Column(Integer, primary_key=True)
    rating = Column(Float, nullable=False)
    UniqueConstraint(userId, movieId, name='ratings_predictions_ux_1')

    # ----------------------------------------------------------------------
    def __init__(self, userId, movieId, rating):
        """"""
        self.userId = userId
        self.movieId = movieId
        self.rating = rating


# create tables
Base.metadata.create_all(engine)
