import sys

import numpy as np
import progressbar

from dataAccess import Session, Ratings
from dataAccess.entities import RatingsPredictionsBySVD

session = Session()
# Remove limit to use all ratings
# ratings_list = session.query(Ratings).limit(1000).all()
ratings_list = session.query(Ratings).all()

user_to_index = {}
for rating in ratings_list:
    if rating.user_id not in user_to_index:
        user_to_index[rating.user_id] = len(user_to_index)

movie_to_index = {}
for rating in ratings_list:
    if rating.movie_id not in movie_to_index:
        movie_to_index[rating.movie_id] = len(movie_to_index)


class Residual:

    __slots__ = ('value', 'current_error', 'prev_error')

    def __init__(self, value, current_error, prev_error):
        self.value = value
        self.current_error = current_error
        self.prev_error = prev_error


def train(max_rank=25, min_epochs=0, max_epochs=100, learning_rate=0.001, regularizer=0.02):
    user_features = np.full((len(user_to_index), max_rank), 0.1)
    movie_features = np.full((len(movie_to_index), max_rank), 0.1)
    num_ratings = len(ratings_list)
    residuals = [Residual(rating.rating, 0.0, 0.0) for rating in ratings_list]

    for rank in progressbar.progressbar(range(max_rank)):
        errors = [0.0, sys.float_info.max, sys.float_info.max]
        for i in range(max_epochs):
            for j in range(num_ratings):
                rating, residual = ratings_list[j], residuals[j]
                movie_feature = movie_features[movie_to_index[rating.movie_id], rank]
                user_feature = user_features[user_to_index[rating.user_id], rank]
                residual.current_error = residual.value - user_feature * movie_feature
                error_diff = residual.prev_error - residual.current_error
                errors[0] += error_diff * error_diff
                residual.prev_error = residual.current_error
                movie_features[movie_to_index[rating.movie_id], rank] += learning_rate * (
                            residual.current_error * user_feature - regularizer * movie_feature)
                user_features[user_to_index[rating.user_id], rank] += learning_rate * (
                            residual.current_error * movie_feature - regularizer * user_feature)
            if i > min_epochs and errors[0] < errors[1] and errors[1] > errors[2]:
                break
            errors[0], errors[1], errors[2] = 0.0, errors[0], errors[1]
        for residual in residuals:
            residual.value = residual.current_error
            residual.prev_error = 0.0

    singular_values = [np.linalg.norm(user_features[:, rank]) * np.linalg.norm(movie_features[:, rank]) for rank in range(max_rank)]
    for rank in range(max_rank):
        user_features[:, rank] /= np.linalg.norm(user_features[:, rank])
        movie_features[:, rank] /= np.linalg.norm(movie_features[:, rank])

    return user_features, singular_values, movie_features


def get_predicted_rating(S, V, U, user, item):
    values = []
    for r in range(len(S)):
        values.append(U[user_to_index[user], r] * S[r] * V[movie_to_index[item], r])
    return sum(values)


U, S, V = train()

for user_id in progressbar.progressbar(user_to_index.keys()):
    for movie_id in movie_to_index.keys():
        predicted_rating = get_predicted_rating(S, V, U, user_id, movie_id)
        rating = RatingsPredictionsBySVD(user_id, movie_id, predicted_rating)
        session.add(rating)
        session.commit()



