# coding=utf-8
import json
from random import shuffle

from flask import Flask
from flask import Response
from flask import jsonify, request

from config import genius, sphinx, postgres, cropper

app = Flask(__name__)
app.debug = True


def json_response(obj_or_array):
    return Response(json.dumps(obj_or_array, sort_keys=True, indent=4), mimetype='application/json')


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

def add_more_info_about_song(info):
    more_info = postgres.get_song_info_by_id(info['id'])
    info_map = genius.get_info(more_info['genius_id'])
    lyrics_map = postgres.get_lyrics_map(info['id'])
    if lyrics_map:
        info_map['timestamp_lyrics'] = lyrics_map

    all_info = info_map.copy()
    all_info.update(more_info)
    all_info.update(info)

    if all_info.get('album') is None:
        cover_num = (int(all_info.get('id')) % 6) + 1
        all_info['album'] = {
                'id': 0,
                'name': '',
                'cover_url': 'http://cherry.nksoff.ru/static/no_cover_' + str(cover_num) + '.png'
        }

    return all_info


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

    postgres.add_query_history(query)

    found_ids = sphinx.find_songs(query)
    if not found_ids:
        return jsonify({
            'code': '404',
            'message': 'Found nothing',
            'fields': [
                'id',
            ]
        })
    found_coordinates = postgres.get_all_song_ids_and_timestamps(found_ids)
    found_coordinates = list(map(add_more_info_about_song, found_coordinates))

    for song in found_coordinates:
        postgres.add_song_history(song['id'])

    return json_response(found_coordinates)


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

    result = postgres.get_popular_queries(limit)
    result = list(map(lambda query: {'query': query}, result))

    return json_response(result)


@app.route('/api/v2/song/popular', methods=['GET'])
def song_popular():
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

    result = postgres.get_popular_song_ids(limit)
    result = list(map(lambda _id: add_more_info_about_song({'id' : _id}), result))

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

    info = postgres.get_song_info_by_id(song_id)
    if not info:
        return jsonify({
                'code': '404',
                'message': 'song not found',
                'fields': [
                    'id',
                ]
            })

    postgres.add_song_history(song_id)

    info_map = genius.get_info(info['genius_id'])
    lyrics_map = postgres.get_lyrics_map(song_id)
    if lyrics_map:
        info_map['timestamp_lyrics'] = lyrics_map
    return json_response(info_map)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
