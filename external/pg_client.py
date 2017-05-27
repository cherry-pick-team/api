import collections

import psycopg2


class PsgClient(object):
    def __init__(self, logger, host, user, password, db_name):
        self.select_unique_songs = '''
        SELECT
            t.songid, s.album_id, s.file_id, array_agg(t.start_time_ms),
            array_agg(t.end_time_ms), array_agg(t.id)
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

        self.closest_lyrics = '''
        SELECT t.phrase
        FROM transcription AS t
        WHERE t.songid=%s AND t.id=ANY(%s)
        '''

        self.all_songs = '''
        SELECT
            DISTINCT s.id, SUBSTR(s.title, 2)
        FROM songs AS s
        ORDER BY SUBSTR(s.title, 2)
        LIMIT {} OFFSET {}
        '''

        self.sond_id_from_transcription = '''
        SELECT
            t.songid
        FROM transcription AS t
        WHERE t.id=%s
        '''

        self.logger = logger
        self.db_name = db_name
        self.db_host = host
        self.db_user = user
        self.db_password = password
        self.conn = psycopg2.connect(
            'postgres://{}:{}@{}:5432/{}'.format(user, password, host, db_name))

    def reconnect(func):
        def deco(self, *args, **kwargs):
            if self.conn.closed:
                self.conn = psycopg2.connect('postgres://{}:{}@{}:5432/{}'.format(
                    self.db_user, self.db_password, self.db_host, self.db_name))
            cur = self.conn.cursor()
            try:
                return func(self, cur, *args, **kwargs)
            except Exception as e:
                if not self.conn.closed:
                    self.conn.rollback()
                self.logger.error('Failed to get songs info')
                self.logger.error(e)
                return {}
            finally:
                if cur:
                    cur.close()
        return deco

    @reconnect
    def get_lyrics_map(self, cur, songid):
        cur.execute(self.get_ordered_lyrics_map, (songid,))
        phrase_and_timestamps = cur.fetchall()
        if phrase_and_timestamps:
            result = collections.OrderedDict()
            for phrase in phrase_and_timestamps:
                if len(phrase) > 1:
                    if phrase[1] and phrase[1] != "" and "chorus" not in phrase[1].lower():
                        result[phrase[0]] = phrase[1]
            return result


    @reconnect
    def get_relevant_rotation(self, cur, relevant_ids_seq, arr_to_rearrange):
        result_array = []
        songs_seq = []
        for id in relevant_ids_seq:
            cur.execute(self.sond_id_from_transcription, (id,))
            current_id = cur.fetchone()[0]
            if current_id not in songs_seq:
                songs_seq.append(current_id)
                for element in arr_to_rearrange:
                    if str(element['id']) == str(current_id):
                        result_array.append(element)
        return result_array if len(result_array) == len(arr_to_rearrange) else arr_to_rearrange


    @reconnect
    def get_all_song_ids_and_timestamps(self, cur, ids):
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
                    for i in zip(id[3], id[4], id[5])
                    ]
                res_list = get_lengths(ts)
                if res_list:
                    lir_dicts_list = []
                    for chunk in res_list:
                        lir_dicts_list.append({
                            'start': chunk[0],
                            'end': chunk[1],
                            'lyrics': [
                                i[0]
                                for i in self.get_closest_lyrics(chunk[2], song_id)
                                if i[0] and i[0] != "" and "chorus" not in i[0].lower()]
                        })
                    result.append({
                        'id': song_id,
                        'album_id': album_id,
                        'mongo_path': mongo_path,
                        'chunks': [i[:2] for i in res_list],
                        'lyrics_chunks': lir_dicts_list
                    })

            return result


    @reconnect
    def get_song_info_by_id(self, cur, id):
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


    @reconnect
    def add_query_history(self, cur, query):
        cur.execute(self.add_to_query_history, (query,))
        self.conn.commit()


    @reconnect
    def add_song_history(self, cur, _id):
        if not _id:
            return
        self.logger.info('Add song to history: `{}`'.format(_id))
        cur.execute(self.add_to_song_history, (_id,))
        self.conn.commit()


    @reconnect
    def get_popular_queries(self, cur, limit=10):
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


    @reconnect
    def get_popular_songs(self, cur, limit=10):
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


    @reconnect
    def get_all_songs(self, cur, offset, limit):
        cur.execute(self.all_songs.format(limit, offset))
        rows = cur.fetchall()
        if rows:
            result = []
            for row in rows:
                query = row[0]
                result.append({'id': query})
            return result
        else:
            return []


    @reconnect
    def get_album_info(self, cur, id):
        cur.execute(self.select_album, (id,))
        value = cur.fetchone()
        if value and len(value) > 2:
            return {
                'title': value[0],
                'cover_id': value[1],
                'year': value[2]
            }


    @reconnect
    def get_closest_lyrics(self, cur, lyr_id, song_id):
        cur.execute(self.closest_lyrics, (song_id, [lyr_id, lyr_id - 1, lyr_id + 1]))
        return cur.fetchall()


def get_lengths(ts):
    for sub_list in ts:
        if sub_list[1] - sub_list[0] > 19000:
            return [sub_list]

    ts.sort(key=lambda x: x[0] - x[1])
    ts = ts[:3]

    res = []
    for one in ts:
        if one[1] - one[0] < 6000:
            res.append([one[0] - 4000, one[1] + 4000, one[2]])
        else:
            res.append(one)

    return res
