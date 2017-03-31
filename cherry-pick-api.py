# coding=utf-8
import json
from random import shuffle

from flask import Flask
from flask import Response
from flask import jsonify, request

app = Flask(__name__)
app.debug = True


def json_response(obj_or_array):
    return Response(json.dumps(obj_or_array, sort_keys=True,
                               indent=4), mimetype='application/json')


def get_arg(key, default=None):
    if default is None:
        return request.args.get(key)
    else:
        return request.args.get(key, default)


@app.route('/')
def main():
    return 'pretty good, you started api!'


@app.errorhandler(400)
def bad_request(error):
    return jsonify({
        'code': '400',
        'message': error.description,
        'fields': [
        ]
    })


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'code': '404',
        'message': error.description,
        'fields': [
        ]
    })


def generate_song_mock(i, query=''):
    HOST = request.scheme + '://' + request.host
    return {
        'song': {
            'id': str(i),
            'title': 'Song ' + query + ' #' + str(i),
            'singers': [
                {
                    'id': str(1000000000 + i + 1),
                    'name': 'Singer #' + str(1000000000 + i + 1),
                },
                {
                    'id': str(1000000000 + i + 50),
                    'name': 'Singer #' + str(1000000000 + i + 50),
                }
            ]
        },
        'album': {
            'id': str(5000000000 + i + 61),
            'name': 'Album ' + query + ' #' + str(5000000000 + i + 61),
            'year': str(1990 + i % 20),
        },
        'url': HOST + '/api/v2/song/' + str(i) + '/info',
        'url_stream': HOST + '/api/v2/song/' + str(i) + '/stream',
        'favourite_count': (1007 * i) % 19,
        'is_favourite': False,
        'pieces': [
            {
                'begin': '1.720',
                'end': '10.356',
                'lines': [
                    u"Я от тебя письма не получаю " + query
                ]
            },
            {
                'begin': '11.720',
                'end': '17.356',
                'lines': [
                    u"Ты далеко и даже не скучаешь"
                ]
            },
        ]
    }


@app.route('/api/v2/search', methods=['GET'])
def search():
    query = get_arg('query', None)
    limit = get_arg('limit', str(10))
    page = get_arg('page', str(1))

    if query is None or query == '':
        return jsonify({
            'code': '400',
            'message': 'empty query',
            'fields': [
                'query',
            ]
        })

    try:
        limit = int(limit)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'limit',
            ]
        })

    try:
        page = int(page)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'page',
            ]
        })

    result = []

    id_from = (page - 1) * limit + 1
    for i in range(id_from, id_from + limit):
        result.append(generate_song_mock(i, query))

    return json_response(result)


@app.route('/api/v2/search/popular', methods=['GET'])
def search_popular():
    limit = get_arg('limit', str(10))

    try:
        limit = int(limit)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'limit',
            ]
        })

    result = []

    for i in range(1, limit + 1):
        result.append([18900000000000 + i*100, 'rand query ' + str(i)])

    shuffle(result)

    def gen(i , q):
        return [
                generate_song_mock(i+ 1, q),
                generate_song_mock(i+ 8, q),
                generate_song_mock(i+ 5, q),
                ]

    result = map(lambda x: {'query': x[1], 'songs' : gen(x[0], x[1])}, result)

    return json_response(result)


@app.route('/api/v2/song/<song_id>/info', methods=['GET'])
def song_id_info(song_id):
    try:
        song_id = int(song_id)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'id',
            ]
        })

    if song_id > 10000:
        return jsonify({
            'code': '404',
            'message': 'song not found',
            'fields': [
                'id',
            ]
        })

    return json_response(
        generate_song_mock(song_id, 'some song with id')
    )


if __name__ == '__main__':
    app.run()
