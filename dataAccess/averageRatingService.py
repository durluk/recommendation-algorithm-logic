from dataAccess import Session, Movies
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


def calculate_average_rating(movie_rating_list):
    global_average_rating = calculate_and_save_global_average_rating()
    minimum_number_of_ratings = get_parameter("minimum_number_of_ratings_for_average").value
    movie_ids_to_ratings = dict()
    create_ratings_map(movie_ids_to_ratings, movie_rating_list)
    create_average_ratings_for_movies_with_ratings(global_average_rating, minimum_number_of_ratings,
                                                   movie_ids_to_ratings)


def create_average_ratings_for_movies_with_ratings(global_average_rating, minimum_number_of_ratings,
                                                   movie_ids_to_ratings):
    session = Session()
    clear_average_rating_table()
    average_ratings = []
    processed_movie = 0
    for movie_id, list_of_ratings in movie_ids_to_ratings.items():
        processed_movie += 1
        number_of_ratings = len(list_of_ratings)
        if number_of_ratings < minimum_number_of_ratings:
            average_movie_rating_value = (sum(list_of_ratings) + global_average_rating * (
                    minimum_number_of_ratings - number_of_ratings)) / minimum_number_of_ratings
        else:
            average_movie_rating_value = sum(list_of_ratings) / number_of_ratings
        average_ratings.append({'movie_id': movie_id, 'average_rating': round(average_movie_rating_value, 1)})
        if processed_movie % 10000 == 0:
            print('Average calculation progress: ', processed_movie / 1000, 'k')
    session.bulk_insert_mappings(AverageMovieRating, average_ratings)
    session.commit()


def clear_average_rating_table():
    session = Session()
    session.query(AverageMovieRating).delete()
    session.commit()


#
# def create_average_ratings_for_movies_without_ratings(global_average_rating, movie_ids_to_ratings, movie_list):
#     session = Session()
#     processed_movie = 0
#     number_of_movies_to_process = movie_list.count()
#     list_of_average_ratings_to_update = session.query(AverageMovieRating)
#     movie_ids_to_average_ratings = dict()
#     for average_movie_rating in list_of_average_ratings_to_update:
#         movie_ids_to_average_ratings[average_movie_rating.movie_id] = [average_movie_rating]
#
#     for movie in movie_list:
#         processed_movie += 1
#         print("Processed: ", processed_movie, " from: ", str(number_of_movies_to_process), " movie_id: ",
#               movie.movie_id)
#         if movie.movie_id in movie_ids_to_average_ratings:
#             continue
#         else:
#             average_movie_rating = AverageMovieRating(movie.movie_id, global_average_rating)
#             session.add(average_movie_rating)
#     session.commit()
#

def create_ratings_map(movie_ids_to_ratings, movie_rating_list):
    processed_movie = 0
    for movie_id, rating in movie_rating_list:
        processed_movie += 1
        if movie_id in movie_ids_to_ratings:
            movie_ids_to_ratings[movie_id].append(rating)
        else:
            movie_ids_to_ratings[movie_id] = [rating]
        if processed_movie % 10000 == 0:
            print('Mapping progress: ', processed_movie / 1000, 'k')


def calculate_average_ratings_for_all_movies():
    session = Session()
    ratings = session.execute("SELECT movie_id, rating FROM ratings")
    calculate_average_rating(ratings)


if __name__ == "__main__":
    calculate_average_ratings_for_all_movies()
