import numpy as np

from dataAccess import Session
from dataAccess.entities import RatingsPredictions


def calculate_predicted_ratings_based_on_user_similarity(ratings_list, users_similarity_list):
    user_ids_to_real_position = dict()
    movie_ids_to_real_position = dict()
    user_position = 0
    movie_position = 0
    for user_id, movie_id, rating in ratings_list:
        if user_id not in user_ids_to_real_position:
            user_ids_to_real_position[user_id] = user_position
            user_position += 1
        if movie_id not in movie_ids_to_real_position:
            movie_ids_to_real_position[movie_id] = movie_position
            movie_position += 1

    user_size = len(user_ids_to_real_position)
    movie_size = len(movie_ids_to_real_position)
    user_user_similarity_matrix = np.zeros((user_size, user_size))
    for user_id, compare_user_id, similarity in users_similarity_list:
        column_number = user_ids_to_real_position[user_id]
        row_number = user_ids_to_real_position[compare_user_id]
        user_user_similarity_matrix[row_number, column_number] = similarity

    items_users_ratings_matrix = np.zeros((movie_size, user_size))
    session = Session()
    ratings_list = session.execute("SELECT user_id, movie_id, rating FROM ratings")
    for user_id, movie_id, rating in ratings_list:
        column_number = user_ids_to_real_position[user_id]
        row_number = movie_ids_to_real_position[movie_id]
        items_users_ratings_matrix[row_number, column_number] = rating

    unnormalized_predicted_ratings = np.matmul(items_users_ratings_matrix, user_user_similarity_matrix)
    absolute_sum_of_similarities = np.absolute(user_user_similarity_matrix.sum(axis=0))
    y = np.diag(1 / absolute_sum_of_similarities)
    inf_values = np.isinf(y)
    y[inf_values] = 0
    z = np.dot(unnormalized_predicted_ratings, y)
    clear_rating_predictions_table()

    predictions = []
    progress = 0
    number_to_calculate = len(user_ids_to_real_position)
    for user_id, user_real_position in user_ids_to_real_position.items():
        for movie_id, movie_real_position in movie_ids_to_real_position.items():
            predicted_rating = round(z[movie_real_position, user_real_position], 1)
            if predicted_rating == 0:
                average_movie_rating = session.execute(
                    "SELECT average_rating FROM average_movie_rating WHERE movie_id = :param_movie_id",
                    {'param_movie_id': movie_id}).fetchone()
                predicted_rating = average_movie_rating[0]
            predictions.append(
                {'user_id': user_id, 'movie_id': movie_id,
                 'rating': predicted_rating})
        session.bulk_insert_mappings(RatingsPredictions, predictions)
        session.commit()
        predictions.clear()
        progress += 1
        print('Progress: ', round(100 * (progress / number_to_calculate), 2), '%')


def clear_rating_predictions_table():
    session = Session()
    session.query(RatingsPredictions).delete()
    session.commit()


def calculate_all_predictions():
    session = Session()
    ratings_list = session.execute("SELECT user_id, movie_id, rating FROM ratings")
    users_similarity_list = session.execute("SELECT user_id, compare_user_id, similarity FROM users_similarity")
    calculate_predicted_ratings_based_on_user_similarity(ratings_list, users_similarity_list)


if __name__ == "__main__":
    calculate_all_predictions()
