from scipy.spatial.distance import cosine

from dataAccess import Session, Ratings
from dataAccess.entities import UsersSimilarity


def calculate_users_similarity(ratings_list):
    user_ids_to_movie_ratings = dict()
    for rating in ratings_list:
        if rating.user_id in user_ids_to_movie_ratings:
            user_ids_to_movie_ratings[rating.user_id][rating.movie_id] = rating.rating
        else:
            user_ids_to_movie_ratings[rating.user_id] = dict([(rating.movie_id, rating.rating)])

    clear_users_similarity_table()
    session = Session()
    user_similarities_ready_to_save = []
    progress = 0
    number_to_calculate = len(user_ids_to_movie_ratings)
    for compared_user_id, list_of_compared_user_ratings in user_ids_to_movie_ratings.items():
        for id_of_user_for_comparision, list_of_user_for_comparison_ratings in user_ids_to_movie_ratings.items():
            if compared_user_id != id_of_user_for_comparision:
                movie_ids_to_ratings_of_compared_user = dict()
                movie_ids_to_ratings_of_user_for_comparison = dict()
                prepare_vectors_for_comparison(list_of_compared_user_ratings, list_of_user_for_comparison_ratings,
                                               movie_ids_to_ratings_of_compared_user,
                                               movie_ids_to_ratings_of_user_for_comparison)

                normalized_rating_for_compared_user = calculate_normalized_rating_vector(
                    movie_ids_to_ratings_of_compared_user.values())
                normalized_rating_for_user_for_comparision = calculate_normalized_rating_vector(
                    movie_ids_to_ratings_of_user_for_comparison.values())
                users_similarity = round(2 - cosine(normalized_rating_for_compared_user,
                                                    normalized_rating_for_user_for_comparision), 3)

                user_similarities_ready_to_save.append(
                    {"user_id": compared_user_id, 'compare_user_id': id_of_user_for_comparision,
                     'similarity': users_similarity})
        session.bulk_insert_mappings(UsersSimilarity, user_similarities_ready_to_save)
        session.commit()
        user_similarities_ready_to_save.clear()
        progress += 1
        print('Progress: ', round(100 * (progress / number_to_calculate), 2), '%')


def clear_users_similarity_table():
    session = Session()
    session.query(UsersSimilarity).delete()
    session.commit()


def prepare_vectors_for_comparison(list_of_compared_user_ratings, list_of_user_for_comparison_ratings,
                                   movie_ids_to_ratings_of_compared_user, movie_ids_to_ratings_of_user_for_comparison):
    for movie_id, movie_rating in list_of_compared_user_ratings.items():
        movie_ids_to_ratings_of_compared_user[movie_id] = movie_rating
        movie_ids_to_ratings_of_user_for_comparison[movie_id] = 0.0
    for movie_id, movie_rating in list_of_user_for_comparison_ratings.items():
        if movie_id in movie_ids_to_ratings_of_user_for_comparison:
            movie_ids_to_ratings_of_user_for_comparison[movie_id] = movie_rating
        else:
            movie_ids_to_ratings_of_compared_user[movie_id] = 0.0
            movie_ids_to_ratings_of_user_for_comparison[movie_id] = movie_rating


def calculate_users_similarity_for_all_users():
    session = Session()
    ratings_list = session.query(Ratings)
    calculate_users_similarity(ratings_list)


def calculate_normalized_rating_vector(unnormalized_vector):
    normalized_rating_factor = sum(unnormalized_vector) / len(unnormalized_vector)
    ratings_for_all_movies = []
    for rating in unnormalized_vector:
        normalized_rating = rating - normalized_rating_factor
        ratings_for_all_movies.append(normalized_rating)
    return ratings_for_all_movies


if __name__ == "__main__":
    calculate_users_similarity_for_all_users()
