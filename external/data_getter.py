import collections
import logging
import json

import psycopg2
import pymysql.cursors
import requests
from gridfs import GridFS
from pymongo import MongoClient
from bson.objectid import ObjectId

logger = logging.getLogger('data-getter')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def get_lengths(ts):
    for sub_list in ts:
        if sub_list[1] - sub_list[0] > 19000:
            return [sub_list]

    ts.sort(key=lambda x: x[0] - x[1])
    ts = ts[:3]

    res = []
    for one in ts:
        if one[1] - one[0] < 6000:
            res.append([one[0] - 4000, one[1] + 4000, one[2], one[3]])
        else:
            res.append(one)
    return res


class SphinxSearch(object):
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connect()

    def connect(self):
        self.connection = pymysql.connect(
            host=self.host, port=self.port, user=self.user, passwd=self.password, charset='utf8', db='')

    def find_songs(self, key_word, recurse_on_fail=True):
        try:
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
        except:
            if recurse_on_fail:
                self.close()
                self.connect()
                return self.find_songs(key_word, recurse_on_fail=False)
            else:
                return []

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def find_songs_indirect(self, key_word, recurse_on_fail=True):
        try:
            with self.connection.cursor() as cursor:
                # supposed to be injection free
                # http://initd.org/psycopg/docs/usage.html#the-problem-with-the-query-parameters
                cursor.execute('select * from songs_search_indirect where match(%s);', (key_word,))
                result = cursor.fetchall()
            return [
                int(x[0])
                for x in result
                if x and len(x) > 0
            ]
        except:
            if recurse_on_fail:
                self.close()
                self.connect()
                return self.find_songs(key_word, recurse_on_fail=False)
            else:
                return []

    def close(self):
        if self.connection is not None:
            self.connection.close()



class PsgClient(object):
    def __init__(self, host, user, password, db_name):
        self.select_unique_songs = '''
        SELECT t.songid, s.album_id, s.file_id, array_agg(t.start_time_ms), array_agg(t.end_time_ms), array_agg(t.phrase), array_agg(t.id)
        FROM transcription AS t
        JOIN songs AS s
        ON s.id = t.songid
        WHERE t.id=any(%s)
        GROUP BY t.songid, s.album_id, s.file_id;
        '''

        self.select_album = '''
        SELECT title, cover_id, year
        FROM album
        where id=%s;
        '''

        self.select_song = '''
        SELECT
            s.author, s.title, s.lyrics, s.file_id, s.album_id
        FROM songs AS s
        WHERE s.id=%s;
        '''
        self.db_name = db_name
        self.db_host = host
        self.db_user = user
        self.db_password = password
        self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(user, password, host, db_name))

        self.get_ordered_lyrics_map = '''
        SELECT
            t.start_time_ms, t.phrase
        FROM transcription AS t
        WHERE t.songid=%s
        ORDER BY t.start_time_ms;
        '''

        self.add_to_query_history = '''
        INSERT INTO query_history (query) VALUES(%s);
        '''

        self.add_to_song_history = '''
        INSERT INTO song_history (songid) VALUES(%s);
        '''

        self.popular_queries = '''
        SELECT
            qh.query, COUNT(*)
        FROM query_history AS qh
        GROUP BY qh.query
        ORDER BY COUNT(*) DESC
        LIMIT {};
        '''

        self.popular_song_ids = '''
        SELECT
            sh.songid, COUNT(*)
        FROM song_history AS sh
        GROUP BY sh.songid
        ORDER BY COUNT(*) DESC
        LIMIT {};
        '''

    def get_lyrics_map(self, songid):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.get_ordered_lyrics_map, (songid,))
            phrase_and_timestamps = cur.fetchall()
            if phrase_and_timestamps:
                result = collections.OrderedDict()
                for phrase in phrase_and_timestamps:
                    if len(phrase) > 1:
                        result[phrase[0]] = phrase[1]
                return result
        except Exception as e:
            logger.error('Failed to get songs info')
            logger.error(e)
            return {}
        finally:
            cur.close()

    def get_all_song_ids_and_timestamps(self, ids):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.select_unique_songs, (ids,))
            ids_plus_chunks = cur.fetchall()
            if ids_plus_chunks:
                result = []
                for id in ids_plus_chunks:
                    song_id = id[0]
                    album_id = id[1]
                    mongo_path = id[2]
                    ts = [
                        list(i)
                        for i in zip(id[3], id[4], id[5], id[6])
                    ]
                    res_list = get_lengths(ts)
                    if res_list:
                        result.append({
                            'id': song_id,
                            'album_id': album_id,
                            'mongo_path': mongo_path,
                            'chunks': [i[:2] for i in res_list],
                            'lyrics_chunks': {
                                i[3]: i[2]
                                for i in res_list
                            },
                        })
                return result
        except Exception as e:
            logger.error('Failed to get songs info')
            logger.error(e)
            return []
        finally:
            cur.close()

    def get_song_info_by_id(self, id):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
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
                    'album_id': value[4]
                }
        except Exception as e:
            logger.error('Failed to get song with id=`{}`'.format(id))
            logger.error(e)
            return {}
        finally:
            cur.close()

    def add_query_history(self, query):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.add_to_query_history, (query,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error('Failed to add query to history: `{}`'.format(query))
            logger.error(e)
            return {}
        finally:
            cur.close()

    def add_song_history(self, _id):
        if not _id:
            return
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            logger.info('Add song to history: `{}`'.format(_id))
            cur.execute(self.add_to_song_history, (_id,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error('Failed to add song to history: `{}`'.format(_id))
            logger.error(e)
        finally:
            cur.close()

    def get_popular_queries(self, limit=10):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.popular_queries.format(limit))
            rows = cur.fetchall()
            if rows:
                result = []
                for row in rows:
                    query = row[0]
                    count = row[1]
                    result.append({'query': query, 'count': count})
                return result
            else:
                return []
        except Exception as e:
            logger.error('Failed to get popular queries')
            logger.error(e)
            return []
        finally:
            cur.close()

    def get_popular_songs(self, limit=10):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.popular_song_ids.format(limit))
            rows = cur.fetchall()
            if rows:
                result = []
                for row in rows:
                    query = row[0]
                    count = row[1]
                    result.append({'id': query, 'count': count})
                return result
            else:
                return []
        except Exception as e:
            logger.error('Failed to get popular songs')
            logger.error(e)
            return []
        finally:
            cur.close()

    def get_album_info(self, id):
        if self.conn.closed:
            self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                self.db_user, self.db_password, self.db_host, self.db_name))
        cur = self.conn.cursor()
        try:
            cur.execute(self.select_album, (id,))
            value = cur.fetchone()
            if value and len(value) > 2:
                return {
                    'title': value[0],
                    'cover_id': value[1],
                    'year': value[2]
                }
        except Exception as e:
            logger.error('Failed to get popular songs')
            logger.error(e)
            return []
        finally:
            cur.close()


class CropperDemon(object):
    def __init__(self, host, port):
        self.request_path = 'http://{}:{}/get_song/'.format(host, port)

    def get_song(self, object_id, intervals):
        request_json = {
            'objectId': object_id,
            'intervals': intervals
        }
        return requests.post(self.request_path, data=json.dumps(request_json))


class MongoC(object):
    def __init__(self, host, db_name, collection):
        self.connect_info = 'mongodb://{}:27017/'.format(host)
        self.db_name = db_name
        self.collection = collection

    def get_cover(self, cover_id):
        client = MongoClient(self.connect_info)
        fs = GridFS(client[self.db_name], self.collection)
        cover = fs.find_one({'_id': ObjectId(cover_id) })
        if cover:
            return cover.read()
