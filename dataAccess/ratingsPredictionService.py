import numpy as np
from sqlalchemy import func, distinct

from dataAccess import Session, Movies, Ratings
from dataAccess.entities import UsersSimilarity, RatingsPredictions


def calculate_predicted_ratings_based_on_user_similarity():
    session = Session()
    list_of_users_similarity = session.query(UsersSimilarity).all()
    number_of_users_for_prediction_calculation = session.query(Movies).value(
        func.count(distinct(UsersSimilarity.user_id)))
    number_of_users_with_calculated_similarity = session.query(Movies).value(
        func.count(distinct(UsersSimilarity.compare_user_id)))
    user_user_similarity_matrix = create_user_to_user_similarity_matrix(list_of_users_similarity,
                                                                        number_of_users_for_prediction_calculation,
                                                                        number_of_users_with_calculated_similarity)
    # print(user_user_similarity_matrix)
    highest_existing_movie_id = session.query(Movies).value(func.max(Movies.movie_id))
    movie_id = 1
    while True:
        list_of_users_ratings = session.query(Ratings).filter(Ratings.movie_id == movie_id).all()
        items_users_ratings_matrix = create_items_users_ratings_matrix(list_of_users_ratings,
                                                                       number_of_users_with_calculated_similarity)
        # print(items_users_ratings_matrix)
        unnormalized_predicted_ratings = np.matmul(items_users_ratings_matrix, user_user_similarity_matrix)
        # print(unnormalized_predicted_ratings)
        absolute_sum_of_similarities = np.absolute(user_user_similarity_matrix.sum(axis=0))
        # print(absolute_sum_of_similarities)
        y = np.diag(1 / absolute_sum_of_similarities)
        # print(y)
        z = np.dot(unnormalized_predicted_ratings, y)
        # print(z)
        save_predictions(movie_id, z)
        movie_id += 1
        if movie_id > highest_existing_movie_id:
            break


def create_items_users_ratings_matrix(list_of_users_ratings, number_of_users_with_calculated_similarity):
    x = 1
    items_users_ratings_matrix = np.zeros((x, number_of_users_with_calculated_similarity))
    for user_rating in list_of_users_ratings:
        row_number = x - 1
        column_number = user_rating.user_id - 1
        users_rating_value = user_rating.rating
        items_users_ratings_matrix[row_number, column_number] = users_rating_value
    return items_users_ratings_matrix


def create_user_to_user_similarity_matrix(list_of_users_similarity, number_of_columns,
                                          number_of_rows):
    user_user_similarity_matrix = np.zeros(
        (number_of_rows, number_of_columns))
    for users_similarity in list_of_users_similarity:
        x_element_position = users_similarity.user_id - 1
        y_element_position = users_similarity.compare_user_id - 1
        users_similarity_value = users_similarity.similarity
        user_user_similarity_matrix[x_element_position, y_element_position] = users_similarity_value
    return user_user_similarity_matrix


def translate(value):
    left_min = -5
    left_max = 5
    right_min = 0
    right_max = 5
    # Figure out how 'wide' each range is
    left_span = left_max - left_min
    right_span = right_max - right_min

    # Convert the left range into a 0-1 range (float)
    value_scaled = float(value - left_min) / float(left_span)

    # Convert the 0-1 range into a value in the right range.
    return right_min + (value_scaled * right_span)


def save_predictions(movie_id, rating_predictions):
    user_id = 1

    for movie_rating_predictions in rating_predictions:
        for movie_rating_prediction in movie_rating_predictions:
            save_prediction(user_id, movie_id, movie_rating_prediction)
            user_id += 1
    movie_id += 1


def save_prediction(user_id, movie_id, calculated_rating_prediction):
    session = Session()
    rating_prediction = session.query(RatingsPredictions) \
        .filter(RatingsPredictions.user_id == user_id, RatingsPredictions.movie_id == movie_id).first()
    if rating_prediction:
        rating_prediction.rating = round(calculated_rating_prediction, 1)
    else:
        session.add(RatingsPredictions(user_id, movie_id, round(calculated_rating_prediction, 1)))
    session.commit()


if __name__ == "__main__":
    calculate_predicted_ratings_based_on_user_similarity()
