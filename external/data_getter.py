import logging
import json

import psycopg2
import pymysql.cursors
import requests

logger = logging.getLogger('data-getter')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def get_lengths(ts):
    if not ts:
        return
    res_list = []
    for sub_list in ts:
        if sub_list[1] - sub_list[0] > 19000:
            return [sub_list]
        if sub_list[1] - sub_list[0] > 8000:
            res_list.append(sub_list)

    res_list = res_list if res_list else ts
    res_list.sort(key=lambda x: x[0] - x[1])
    res_list = res_list[:3]
    res_list.sort(key=lambda x: x[0])

    if res_list[-1][1] - res_list[-1][0] < 8000:
        res_list[-1] = [res_list[-1][0], res_list[-1][1] + 8000]

    out_result = []
    for sub_list in res_list:
        if sub_list[1] - sub_list[0] > 8000:
            out_result.append(sub_list)
    return out_result


class SphinxSearch(object):
    def __init__(self, host, port, user, password):
        self.connection = pymysql.connect(host=host, port=port, user=user, passwd=password, charset='utf8', db='')

    def find_songs(self, key_word):
        with self.connection.cursor() as cursor:
            # supposed to be injection free
            # http://initd.org/psycopg/docs/usage.html#the-problem-with-the-query-parameters
            cursor.execute('select * from songs_search where match(%s);', (key_word,))
            result = cursor.fetchall()
        return [
            int(x[0])
            for x in result
            if x and len(x) > 0
        ]

    def close(self):
        self.connection.close()


class PsgClient(object):
    def __init__(self, host, user, password, db_name):
        self.select_unique_songs = '''
        SELECT t.songid, s.genius_id, s.file_id, array_agg(t.start_time_ms), array_agg(t.end_time_ms)
        FROM transcription AS t
        JOIN songs AS s
        ON s.id = t.songid
        WHERE t.id=any(%s)
        GROUP BY t.songid, s.genius_id, s.file_id;
        '''

        self.select_song = '''
        SELECT
            s.author, s.title, s.lyrics, s.file_id, s.genius_id
        FROM songs AS s
        WHERE s.id=%s;
        '''
        self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format
                                     (user, password, host, db_name))

    def get_all_song_ids_and_timestamps(self, ids):
        cur = self.conn.cursor()
        try:
            cur.execute(self.select_unique_songs, (ids,))
            ids_plus_chunks = cur.fetchall()
            if ids_plus_chunks:
                result = []
                for id in ids_plus_chunks:
                    song_id = id[0]
                    genius_id = id[1]
                    mongo_path = id[2]
                    ts = [list(i) for i in zip(id[3], id[4])]
                    res_list = get_lengths(ts)
                    if res_list:
                        result.append({
                            'id': song_id,
                            'genius_id': genius_id,
                            'mongo_path': mongo_path,
                            'chunks': res_list
                        })
                return result
        except Exception as e:
            logger.error('Failed to get songs info')
            logger.error(e)
        finally:
            cur.close()
        return []

    def get_song_info_by_id(self, id):
        cur = self.conn.cursor()
        try:
            cur.execute(self.select_song, (id,))
            value = cur.fetchone()
            if value and len(value) > 4:
                return {
                    'author': value[0],
                    'title': value[1],
                    'lyrics': value[2],
                    'mongo_id': value[3],
                    'genius_id': value[4]
                }
        except Exception as e:
            logger.error('Failed to get song with id=`{}`'.format(id))
            logger.error(e)
        finally:
            cur.close()
        return {}


class CropperDemon(object):
    def __init__(self, host, port):
        self.request_path = 'http://{}:{}/get_song/'.format(host, port)

    def get_song(self, object_id, intervals):
        request_json = {
            'objectId': object_id,
            'intervals': intervals
        }
        return requests.post(self.request_path, data=json.dumps(request_json))

