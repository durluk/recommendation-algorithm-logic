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
    __tablename__ = "average_movie_rating"

    movie_id = Column(Integer, primary_key=True)
    average_rating = Column(Float, nullable=False)

    # ----------------------------------------------------------------------
    def __init__(self, movie_id, average_rating):
        """"""
        self.movie_id = movie_id
        self.average_rating = average_rating


class UsersSimilarity(Base):
    """"""
    __tablename__ = "users_similarity"
    users_similarity_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    compare_user_id = Column(Integer, nullable=False)
    similarity = Column(Float, nullable=False)
    UniqueConstraint(user_id, compare_user_id, name='users_similarity_ux_1')

    # ----------------------------------------------------------------------
    def __init__(self, user_id, compare_user_id, similarity):
        """"""
        self.user_id = user_id
        self.compare_user_id = compare_user_id
        self.similarity = similarity


class RatingsPredictions(Base):
    """"""
    __tablename__ = "ratings_predictions"
    user_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, primary_key=True)
    rating = Column(Float, nullable=False)
    UniqueConstraint(user_id, movie_id, name='ratings_predictions_ux_1')

    # ----------------------------------------------------------------------
    def __init__(self, user_id, movie_id, rating):
        """"""
        self.user_id = user_id
        self.movie_id = movie_id
        self.rating = rating


class RatingsPredictionsBySVD(Base):
    """"""
    __tablename__ = "ratings_predictions_by_svd"
    user_id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, primary_key=True)
    rating = Column(Float, nullable=False)
    UniqueConstraint(user_id, movie_id, name='ratings_predictions_svd_1')

    # ----------------------------------------------------------------------
    def __init__(self, user_id, movie_id, rating):
        """"""
        self.user_id = user_id
        self.movie_id = movie_id
        self.rating = rating


# create tables
Base.metadata.create_all(engine)
