# coding=utf-8
import difflib
import io
import os
import tempfile
import json
from distutils.util import strtobool

from flask import Flask
from flask import Response
from flask import after_this_request, jsonify, request, send_file
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from config import mongo, sphinx, postgres, cropper
from external import utils
from external.mazafaka import translit_dict

UPLOAD_FOLDER = '/tmp/uploads'
NOT_FLAC_EXTENSIONS = {'mp3', 'aac', 'm4a', 'ogg'}
FLAC_EXTENSIONS = {'wav', 'flac'}
ALLOWED_EXTENSIONS = NOT_FLAC_EXTENSIONS | FLAC_EXTENSIONS

app = Flask(__name__)
app.debug = True
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024
limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=['60 per minute'],
)


def json_response(obj_or_array):
    return Response(json.dumps(obj_or_array, sort_keys=True, indent=4), mimetype='application/json')


def get_arg(key, default=None):
    if default is None:
        return request.args.get(key)
    else:
        return request.args.get(key, default)


# TODO: cache me
def get_user():
    api_token = request.args.get('api_token')

    if api_token is None or not len(api_token):
        api_token = request.cookies.get('api_token')

    if api_token is None or not len(api_token):
        return None

    return postgres.get_user_by_token(api_token)


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


@app.route('/api/v2/search/voice', methods=['GET', 'POST'])
@limiter.limit('150/minute')
def voice_search():
    if 'voice' not in request.files:
        return jsonify({
            'code': '400',
            'message': 'voice file missing'
        })
    file = request.files['voice']
    file_extension = utils.get_file_extension(app.logger, file.filename)
    if file_extension not in ALLOWED_EXTENSIONS:
        return jsonify({
            'code': '400',
            'message': 'wrong format for audio file expected {} got {}'.format(NOT_FLAC_EXTENSIONS, file.filename)
        })
    source_file_path = tempfile.NamedTemporaryFile(delete=False, suffix='-user-uploaded').name
    file.save(source_file_path)
    result_file_path = tempfile.NamedTemporaryFile(delete=False, suffix='.wav').name
    if file_extension in NOT_FLAC_EXTENSIONS:
        if not utils.convert_to_wav(app.logger, source_file_path, result_file_path):
            return jsonify({
                'code': '500',
                'message': 'failed to encode from {} to .wav format'.format(file_extension)
            })
    else:
        result_file_path = source_file_path
    result = utils.retrieve_phrase(app.logger, result_file_path)

    final_array = []
    for phrase in result:
        found_chunk_ids = sphinx.find_songs(phrase, percent='1.0')
        song_ids = postgres.get_found_songs_number(found_chunk_ids)
        final_array.append({
            'query': phrase,
            'songs': len(song_ids),
            'full_info': [song_full_pack_info({'id': id}) for id in song_ids]
        })

    @after_this_request
    def remove_file(response):
        try:
            os.remove(source_file_path)
            if source_file_path != result_file_path:
                os.remove(result_file_path)
        except Exception as error:
            app.logger.error('Error removing or closing downloaded file handle')
            app.logger.error(error)
        return response

    return json_response(final_array)


def song_full_pack_info(incoming_info):
    song_basic_info = postgres.get_song_info_by_id(incoming_info['id'])
    if not song_basic_info:
        return {}
    song_basic_info.update(incoming_info)
    album_basic_info = postgres.get_album_info(song_basic_info['album_id'])
    if not album_basic_info:
        return song_basic_info

    song_basic_info.update({
        'song': {
            'id': incoming_info['id'],
            'title': song_basic_info['title'],
            'singers': [
                {
                    'name': song_basic_info['author'],
                }
            ]
        },
        'album': {
            'id': song_basic_info['album_id'],
            'name': album_basic_info['title'],
            'cover_url': 'https://zsong.ru/api/v2/cover?path=' + album_basic_info['cover_id'],
            'year': album_basic_info['year']
        }
    })

    if song_basic_info.get('album') is None:
        cover_num = (int(song_basic_info.get('id')) % 6) + 1
        song_basic_info['album'] = {
            'id': 0,
            'name': '',
            'cover_url': 'https://zsong.ru/static/no_cover_' + str(cover_num) + '.png'
        }

    has_like = postgres.get_has_like_song(get_user(), incoming_info['id'])
    song_basic_info['like'] = has_like
    
    return song_basic_info


@app.route('/api/v2/cover', methods=['GET'])
def cover():
    path = get_arg('path', None)
    if path is None or path == '':
        return jsonify({
            'code': '400',
            'message': 'empty query',
            'fields': [
                'path',
            ]
        })
    buffer = io.BytesIO()
    cover_file = mongo.get_cover(path)
    if not cover_file:
        return jsonify({
            'code': '404',
            'message': 'Found nothing',
            'fields': [
                'id',
            ]
        })
    buffer.write(cover_file)
    buffer.seek(0)
    return send_file(buffer, attachment_filename='cover.jpg', mimetype='image/jpg')


@app.route('/api/v2/search', methods=['GET'])
@limiter.limit('300/minute')
def search():
    query = get_arg('query', None)
    limit = get_arg('limit', str(10))
    page = get_arg('page', str(1))
    strict = strtobool(get_arg('strict', 'false'))
    search_percent = '1.0' if not strict else '1.0'

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
    updated_q = []
    for word in query.split(' '):
        closest_word = difflib.get_close_matches(word, translit_dict.keys(), n=1, cutoff=0.7)
        if len(closest_word) > 0:
            updated_q.append(translit_dict.get(closest_word[0]))
    if len(updated_q) != 0:
        query = ' '.join(updated_q)

    app.logger.info(query)
    postgres.add_query_history(query)
    found_ids = sphinx.find_songs(query, percent=search_percent)
    if not found_ids:
        return jsonify({
            'code': '404',
            'message': 'Found nothing',
            'fields': [
                'id',
            ]
        })
    found_coordinates = postgres.get_all_song_ids_and_timestamps(found_ids)
    found_coordinates = postgres.get_relevant_rotation(found_ids, found_coordinates)
    found_coordinates = list(map(song_full_pack_info, found_coordinates))

    for song in found_coordinates:
        postgres.add_song_history(song.get('id'))

    return json_response(found_coordinates)


@app.route('/api/v2/search/popular', methods=['GET'])
@limiter.limit('30/minute')
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
    return json_response(result)


@app.route('/api/v2/song/popular', methods=['GET'])
@limiter.limit('300/minute')
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

    result = postgres.get_popular_songs(limit)
    result = list(map(song_full_pack_info, result))

    return json_response(result)


@app.route('/api/v2/song/all', methods=['GET'])
@limiter.limit('30/minute')
def song_all():
    limit = get_arg('limit', str(20))
    page = get_arg('page', str(1))

    try:
        limit = int(limit)
        page = int(page)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'limit',
            ]
        })
    offset = limit * (page - 1)
    result = postgres.get_all_songs(offset, limit)
    result = list(map(song_full_pack_info, result))

    return json_response(result)


@app.route('/api/v2/song/<song_id>/info', methods=['GET'])
@limiter.limit('30/minute')
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
    return song_full_pack_info({'id': song_id})


@app.route('/api/v2/song/<song_id>/stream/<from_ms>/<to_ms>', methods=['GET'])
@limiter.limit('30/minute')
def song_id_stream(song_id, from_ms, to_ms):
    try:
        song_id = int(song_id)
    except ValueError:
        return ''
    info = postgres.get_song_info_by_id(song_id)
    if not info:
        return ''
    res = cropper.get_song(info['mongo_id'], [[int(from_ms), int(to_ms)]])
    return Response(res.text, mimetype='audio/mpeg')


@app.route('/api/v2/likes', methods=['GET'])
def get_my_likes_songs():
    limit = get_arg('limit', str(20))
    page = get_arg('page', str(1))

    try:
        limit = int(limit)
        page = int(page)
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'limit',
            ]
        })
    offset = limit * (page - 1)
    result = postgres.get_user_likes_songs(get_user(), offset, limit)
    result = list(map(song_full_pack_info, result))

    return json_response(result)


@app.route('/api/v2/song/<song_id>/like', methods=['POST'])
def like_song(song_id):
    try:
        is_like = int(get_arg('up', 1)) == 1
    except ValueError:
        return jsonify({
            'code': '400',
            'message': 'wrong format',
            'fields': [
                'up',
            ]
        })

    user = get_user()

    if user is None or user.get('id') is None:
        return jsonify({
            'code': '403',
            'message': 'forbidden',
            'fields': []
        })

    if is_like:
        res = postgres.add_like_song(user, song_id)
    else:
        res = postgres.remove_like_song(user, song_id)

    return json_response({
        'result': res,
    })
