import numpy as np

from dataAccess import Session, Ratings
from dataAccess.entities import UsersSimilarity, RatingsPredictions


def calculate_predicted_ratings_based_on_user_similarity(ratings_list, users_similarity_list):
    user_ids_to_real_position = dict()
    movie_ids_to_real_position = dict()
    user_position = 0
    movie_position = 0
    for rating in ratings_list:
        if rating.user_id not in user_ids_to_real_position:
            user_ids_to_real_position[rating.user_id] = user_position
            user_position += 1
        if rating.movie_id not in movie_ids_to_real_position:
            movie_ids_to_real_position[rating.movie_id] = movie_position
            movie_position += 1
    user_size = len(user_ids_to_real_position)
    movie_size = len(movie_ids_to_real_position)
    user_user_similarity_matrix = np.zeros((user_size, user_size))
    for users_similarity in users_similarity_list:
        column_number = user_ids_to_real_position[users_similarity.user_id]
        row_number = user_ids_to_real_position[users_similarity.compare_user_id]
        user_user_similarity_matrix[row_number, column_number] = users_similarity.similarity
    items_users_ratings_matrix = np.zeros((movie_size, user_size))
    for rating in ratings_list:
        column_number = user_ids_to_real_position[rating.user_id]
        row_number = movie_ids_to_real_position[rating.movie_id]
        items_users_ratings_matrix[row_number, column_number] = rating.rating

    unnormalized_predicted_ratings = np.matmul(items_users_ratings_matrix, user_user_similarity_matrix)
    absolute_sum_of_similarities = np.absolute(user_user_similarity_matrix.sum(axis=0))
    y = np.diag(1 / absolute_sum_of_similarities)
    z = np.dot(unnormalized_predicted_ratings, y)

    clear_rating_predictions_table()
    session = Session()
    predictions = []
    progress = 0
    number_to_calculate = len(user_ids_to_real_position)
    for user_id, user_real_position in user_ids_to_real_position.items():
        for movie_id, movie_real_position in movie_ids_to_real_position.items():
            predictions.append(
                {'user_id': user_id, 'movie_id': movie_id,
                 'rating': round(z[movie_real_position, user_real_position], 1)})
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
    ratings_list = session.query(Ratings)
    users_similarity_list = session.query(UsersSimilarity)
    calculate_predicted_ratings_based_on_user_similarity(ratings_list, users_similarity_list)


if __name__ == "__main__":
    calculate_all_predictions()
