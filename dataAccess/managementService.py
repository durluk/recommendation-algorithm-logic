from sqlalchemy import func

from dataAccess import Session, Ratings
from dataAccess.entities import Management


def calculate_and_save_global_average_rating():
    session = Session()
    # calculate average rating from table ratings
    global_average_rating = round(session.query(Ratings).value(func.avg(Ratings.rating)), 3)
    # Save average_rating to DB
    save_parameter("average_rating", global_average_rating)
    return global_average_rating


def save_parameter(description, value):
    session = Session()
    management = session.query(Management).filter(Management.description == description).first()
    if management is None:
        management = Management(description, value)
    else:
        management.value = value
    session.add(management)
    # commit the record the database
    session.commit()


def get_parameter(description):
    session = Session()
    return session.query(Management).filter(Management.description == description).first()


def save_required_parameters():
    save_parameter("minimum_number_of_ratings_for_average", 5)
    save_parameter("similarity_range_factor", 0.5)


if __name__ == "__main__":
    calculate_and_save_global_average_rating()
    save_required_parameters()
