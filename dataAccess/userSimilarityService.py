from math import isnan

from numpy import count_nonzero
from scipy.spatial.distance import cosine

from dataAccess import Session
from dataAccess.entities import UsersSimilarity
from dataAccess.managementService import get_parameter


def calculate_users_similarity(ratings_list):
    user_ids_to_movie_ratings = dict()
    processed_ratings = 0
    for user_id, movie_id, rating in ratings_list:
        processed_ratings += 1
        if user_id in user_ids_to_movie_ratings:
            user_ids_to_movie_ratings[user_id][movie_id] = rating
        else:
            user_ids_to_movie_ratings[user_id] = dict([(movie_id, rating)])
        if processed_ratings % 100000 == 0:
            print('Mapping progress: ', processed_ratings / 1000, 'k')

    clear_users_similarity_table()
    session = Session()
    user_similarities_ready_to_save = []
    progress = 0
    number_to_calculate = len(user_ids_to_movie_ratings)
    similarity_range_factor = get_parameter("similarity_range_factor").value
    for compared_user_id, list_of_compared_user_ratings in user_ids_to_movie_ratings.items():
        inner_progress = 0
        for id_of_user_for_comparision, list_of_user_for_comparison_ratings in user_ids_to_movie_ratings.items():
            inner_progress += 1
            if inner_progress % 10000 == 0:
                print('Inner progress: ', inner_progress / 1000, 'k')
            if compared_user_id != id_of_user_for_comparision & id_of_user_for_comparision > compared_user_id:
                movie_ids_to_ratings_of_compared_user = dict()
                movie_ids_to_ratings_of_user_for_comparison = dict()
                prepare_vectors_for_comparison(list_of_compared_user_ratings, list_of_user_for_comparison_ratings,
                                               movie_ids_to_ratings_of_compared_user,
                                               movie_ids_to_ratings_of_user_for_comparison)
                normalized_rating_for_compared_user = calculate_normalized_rating_vector(
                    movie_ids_to_ratings_of_compared_user.values())
                normalized_rating_for_user_for_comparision = calculate_normalized_rating_vector(
                    movie_ids_to_ratings_of_user_for_comparison.values())
                users_similarity = round(1 - cosine(normalized_rating_for_compared_user,
                                                    normalized_rating_for_user_for_comparision), 3)
                if isnan(users_similarity):
                    continue
                if users_similarity < similarity_range_factor:
                    continue
                user_similarities_ready_to_save.append(
                    {"user_id": compared_user_id, 'compare_user_id': id_of_user_for_comparision,
                     'similarity': users_similarity})
                user_similarities_ready_to_save.append(
                    {"user_id": id_of_user_for_comparision, 'compare_user_id': compared_user_id,
                     'similarity': users_similarity})
                session.execute(
                    "INSERT INTO users_similarity(user_id, compare_user_id, similarity) VALUES (:user_id, :compare_user_id, :similarity)",
                    {'user_id': compared_user_id,
                     'compare_user_id': id_of_user_for_comparision,
                     'similarity': users_similarity})
                session.execute(
                    "INSERT INTO users_similarity(user_id, compare_user_id, similarity) VALUES (:user_id, :compare_user_id, :similarity)",
                    {'user_id': id_of_user_for_comparision,
                     'compare_user_id': compared_user_id,
                     'similarity': users_similarity})
        session.commit()
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


def calculate_normalized_rating_vector(unnormalized_vector):
    normalized_rating_factor = sum(unnormalized_vector) / count_nonzero(list(unnormalized_vector))
    ratings_for_all_movies = []
    for rating in unnormalized_vector:
        if rating != 0:
            normalized_rating = rating - normalized_rating_factor
            ratings_for_all_movies.append(normalized_rating)
        else:
            ratings_for_all_movies.append(0)
    return ratings_for_all_movies


def calculate_users_similarity_for_all_users():
    session = Session()
    ratings_list = session.execute("SELECT user_id, movie_id, rating FROM ratings")
    calculate_users_similarity(ratings_list)


if __name__ == "__main__":
    calculate_users_similarity_for_all_users()
