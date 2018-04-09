from sqlalchemy import Column, String, Float, Integer
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


# create tables
Base.metadata.create_all(engine)
