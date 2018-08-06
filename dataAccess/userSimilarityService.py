import numpy as np
from scipy.spatial.distance import cosine
from sqlalchemy import func

from dataAccess import Session, Movies, User, Ratings
from dataAccess.entities import UsersSimilarity


def calculate_normalized_rating_vector(user_ratings, number_of_movies):
    user_ratings_values_from_db = np.array(list(rating.rating for rating in user_ratings))
    normalized_rating_factor = user_ratings_values_from_db.sum() / user_ratings_values_from_db.size
    ratings_for_all_movies = np.zeros(number_of_movies)
    for rating in user_ratings:
        movie_id = rating.movieId
        normalized_rating = rating.rating - normalized_rating_factor
        ratings_for_all_movies[movie_id - 1] = normalized_rating
    return ratings_for_all_movies


def calculate_users_similarity():
    session = Session()
    # check how many movies there are, so you could know how big the comparision vector will be
    number_of_movies = session.query(Movies).value(func.count(Movies.movieId))

    users_in_users_table = session.query(User).all()
    list_of_users_ids_from_user_table = np.array(list(user.id for user in users_in_users_table))

    unique_users_in_ratings_table = session.query(Ratings.userId).distinct()
    list_of_users_ids_from_ratings_table = np.array(list(rating.userId for rating in unique_users_in_ratings_table))

    for x in list_of_users_ids_from_user_table:
        for y in list_of_users_ids_from_ratings_table:
            if x != y:
                compared_user = session.query(Ratings).filter(Ratings.userId == x).all()
                user_for_comparision = session.query(Ratings).filter(Ratings.userId == y).all()

                if compared_user and user_for_comparision:
                    normalized_rating_for_compared_user = \
                        calculate_normalized_rating_vector(compared_user, number_of_movies)
                    normalized_rating_for_user_for_comparision = calculate_normalized_rating_vector(
                        user_for_comparision, number_of_movies)
                    if any(normalized_rating_for_compared_user) and any(normalized_rating_for_user_for_comparision):
                        similarity = round(2 - cosine(normalized_rating_for_compared_user,
                                                      normalized_rating_for_user_for_comparision), 3)
                        users_similarity = session.query(UsersSimilarity) \
                            .filter(UsersSimilarity.userId == x, UsersSimilarity.compareUserId == y).first()
                        if users_similarity:
                            users_similarity.similarity = similarity
                        else:
                            users_similarity = UsersSimilarity(x, y, similarity)
                            session.add(users_similarity)
                        session.commit()


if __name__ == "__main__":
    calculate_users_similarity()
