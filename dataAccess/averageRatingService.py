from sqlalchemy import func

from dataAccess import Session, Movies, Ratings
from dataAccess.entities import AverageMovieRating
from dataAccess.managementService import calculate_and_save_global_average_rating, get_parameter


def get_top_scored_movies(number_of_movies):
    top_scored_movies = []
    session = Session()
    top_scores = session.query(AverageMovieRating).order_by(AverageMovieRating.average_rating.desc()).limit(
        number_of_movies)

    for instance in top_scores:
        movie = session.query(Movies).filter(Movies.movie_id == instance.movie_id).first()
        top_scored_movies.append(movie)
    return top_scored_movies


def calculate_average_rating():
    session = Session()

    number_of_movies_to_process = session.query(Movies).value(func.max(Movies.movie_id))
    processed_movie = 1

    global_average_rating = calculate_and_save_global_average_rating()
    minimum_number_of_ratings = get_parameter("minimum_number_of_ratings_for_average").value
    for i in session.query(Ratings.movie_id).distinct():
        number_of_ratings = session.query(Ratings).filter(Ratings.movie_id == i.movie_id).value(
            func.count(Ratings.rating))
        if number_of_ratings < minimum_number_of_ratings:
            sum_of_movie_rating_from_db = session.query(Ratings).filter(Ratings.movie_id == i.movie_id).value(
                func.sum(Ratings.rating))
            average_movie_rating_value = (sum_of_movie_rating_from_db
                                          + (minimum_number_of_ratings - number_of_ratings)
                                          * global_average_rating) / minimum_number_of_ratings
        else:
            average_movie_rating_value = session.query(Ratings).filter(Ratings.movie_id == i.movie_id).value(
                func.avg(Ratings.rating))
        rounded_average_movie_rating_value = round(average_movie_rating_value, 2)
        save_or_update_average_movie_rating(i.movie_id, rounded_average_movie_rating_value, session)
        print("Processed movie: ", processed_movie, " from: ", number_of_movies_to_process)
        processed_movie += 1
        session.commit()


def save_or_update_average_movie_rating(movie_id, rounded_average_movie_rating_value, session):
    average_movie_rating = session.query(AverageMovieRating).filter(AverageMovieRating.movie_id == movie_id).first()
    if average_movie_rating is None:
        average_movie_rating = AverageMovieRating(movie_id, rounded_average_movie_rating_value)
    else:
        average_movie_rating.averageRating = rounded_average_movie_rating_value
    session.add(average_movie_rating)


if __name__ == "__main__":
    calculate_average_rating()
