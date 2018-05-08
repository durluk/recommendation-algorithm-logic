from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.orm import mapper, sessionmaker, clear_mappers

from dataAccess.connect import connect
from dataAccess.entities import AverageMovieRating, Management


class Ratings(object):
    pass


class Movies(object):
    pass


def load_session():
    """"""
    engine = create_engine('postgresql+psycopg2://', creator=connect)

    metadata = MetaData(engine)

    moz_ratings = Table('ratings', metadata, autoload=True)
    mapper(Ratings, moz_ratings)

    moz_movies = Table('movies', metadata, autoload=True)
    mapper(Movies, moz_movies)

    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def get_top_scored_movies(number_of_movies):
    top_scored_movies = []
    session = load_session()
    top_scores = session.query(AverageMovieRating).order_by(AverageMovieRating.averageRating.desc()).limit(
        number_of_movies)

    for instance in top_scores:
        movie = session.query(Movies).filter(Movies.movieId == instance.movieId).first()
        top_scored_movies.append(movie)
    return top_scored_movies


if __name__ == "__main__":
    session = load_session()

    # calculate average rating from table ratings
    globalAverageRating = session.query(Ratings).value(func.avg(Ratings.rating))
    # Save average_rating to DB
    management = session.query(Management).filter(Management.description == "average_rating").first()
    if management is None:
        management = Management("average_rating", globalAverageRating)

    else:
        management.value = globalAverageRating

    session.add(management)
    # commit the record the database
    session.commit()

    minimumNumberOfRatings = 5
    for i in session.query(Ratings.movieId).distinct():
        movieId = i.movieId
        numberOfRatings = session.query(Ratings).filter(Ratings.movieId == i.movieId).value(func.count(Ratings.rating))
        averageMovieRatingValue = 0
        if numberOfRatings < minimumNumberOfRatings:
            sumOfMovieRatingFromDB = session.query(Ratings).filter(Ratings.movieId == i.movieId).value(
                func.sum(Ratings.rating))
            averageMovieRatingValue = (sumOfMovieRatingFromDB
                                       + (minimumNumberOfRatings - numberOfRatings)
                                       * globalAverageRating) / minimumNumberOfRatings
        else:
            averageMovieRatingValue = session.query(Ratings).filter(Ratings.movieId == i.movieId).value(
                func.avg(Ratings.rating))
        roundedAverageMovieRatingValue = round(averageMovieRatingValue, 2)
        averageMovieRating = session.query(AverageMovieRating).filter(AverageMovieRating.movieId == i.movieId).first()
        if averageMovieRating is None:
            averageMovieRating = AverageMovieRating(i.movieId, roundedAverageMovieRatingValue)
        else:
            averageMovieRating.averageRating = roundedAverageMovieRatingValue
        session.add(averageMovieRating)

    session.commit()

    session.close();
