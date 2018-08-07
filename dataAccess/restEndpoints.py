#!flask/bin/python
from json import dumps

from flask import Flask

from dataAccess import averageRatingService
from dataAccess.averageRatingService import Movies

app = Flask(__name__)


@app.route('/resources/topScoreMovies/<int:number_of_movies>', methods=['GET'])
def top_score_movies(number_of_movies):
    return dumps(averageRatingService.get_top_scored_movies(number_of_movies), default=movie_jsonifier)


def movie_jsonifier(obj):
    if isinstance(obj, Movies):
        return {'movieId': obj.movie_id, 'title': obj.title}
    else:
        raise ValueError('%r is not JSON serializable' % obj)


if __name__ == '__main__':
    app.run(debug=True)
