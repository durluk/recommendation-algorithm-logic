from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.orm import mapper, sessionmaker

from dataAccess.connect import connect
from dataAccess.entities import Management, AverageMovieRating


class Ratings(object):
    pass


def loadSession():
    """"""
    engine = create_engine('postgresql+psycopg2://', creator=connect)

    metadata = MetaData(engine)
    moz_ratings = Table('ratings', metadata, autoload=True)
    mapper(Ratings, moz_ratings)

    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == "__main__":
    session = loadSession()

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
