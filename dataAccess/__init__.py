import numpy
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

from dataAccess.connect import connect


class Ratings(object):
    user_id = None
    movie_id = None
    rating = None


class Movies(object):
    movie_id = None


class User(object):
    id = None


def adapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)


register_adapter(numpy.int32, adapt_numpy_int32)

engine = create_engine('postgresql+psycopg2://', creator=connect)

metadata = MetaData(engine)

moz_ratings = Table('ratings', metadata, autoload=True)
mapper(Ratings, moz_ratings)

moz_movies = Table('movies', metadata, autoload=True)
mapper(Movies, moz_movies)

moz_user = Table('user', metadata, autoload=True)
mapper(User, moz_user)

Session = sessionmaker(bind=engine)
