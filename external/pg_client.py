import collections
import re

import psycopg2


class PsgClient(object):
    def __init__(self, logger, host, user, password, db_name):
        self._q_select_unique_songs = '''
        SELECT
            t.songid, s.album_id, s.file_id, array_agg(t.start_time_ms),
            array_agg(t.end_time_ms), array_agg(t.id)
        FROM transcription AS t
        JOIN songs AS s
        ON s.id = t.songid
        WHERE t.id=ANY(%s)
        GROUP BY t.songid, s.album_id, s.file_id;
        '''

        self._q_select_album = '''
        SELECT title, cover_id, year
        FROM album
        WHERE id=%s;
        '''

        self._q_select_song = '''
        SELECT
            s.author, s.title, s.lyrics, s.file_id, s.album_id
        FROM songs AS s
        WHERE s.id=%s;
        '''

        self._q_get_ordered_lyrics_map = '''
        SELECT
            t.start_time_ms, t.phrase
        FROM transcription AS t
        WHERE t.songid=%s
        ORDER BY t.start_time_ms;
        '''

        self._q_add_to_query_history = '''
        INSERT INTO query_history (query) VALUES(%s);
        '''

        self._q_add_to_song_history = '''
        INSERT INTO song_history (songid) VALUES(%s);
        '''

        self._q_popular_queries = '''
        SELECT
            qh.query, COUNT(*)
        FROM query_history AS qh
        GROUP BY qh.query
        ORDER BY COUNT(*) DESC
        LIMIT {};
        '''

        self._q_popular_song_ids = '''
        SELECT
            sh.songid, COUNT(*)
        FROM song_history AS sh
        INNER JOIN songs AS s ON s.id = sh.songid
        GROUP BY sh.songid
        ORDER BY COUNT(*) DESC
        LIMIT {};
        '''

        self._q_closest_lyrics = '''
        SELECT t.phrase
        FROM transcription AS t
        WHERE t.songid=%s AND t.id=ANY(%s)
        ORDER BY t.id;
        '''

        self._q_all_songs = '''
        SELECT
            DISTINCT s.id, SUBSTR(s.title, 2)
        FROM songs AS s
        ORDER BY SUBSTR(s.title, 2)
        LIMIT {} OFFSET {};
        '''

        self._q_sond_id_from_transcription = '''
        SELECT
            t.songid
        FROM transcription AS t
        WHERE t.id=%s;
        '''

        self._q_get_user_by_token_query = '''
        SELECT
            u.id
        FROM users AS u
        INNER JOIN user_tokens AS ut ON ut.user_id = u.id
        WHERE ut.token=%s;
        '''

        self._q_get_user_likes_songs_query = '''
        SELECT
            sl.song_id, MAX(sl.created_at)
        FROM song_likes AS sl
        WHERE sl.user_id=%s
        GROUP BY sl.song_id
        ORDER BY MAX(sl.created_at) DESC
        LIMIT {} OFFSET {};
        '''

        self._q_add_like_song_query = '''
        INSERT INTO song_likes (song_id, user_id) VALUES(%s, %s);
        '''

        self._q_remove_like_song_query = '''
        DELETE FROM song_likes WHERE song_id=%s AND user_id=%s;
        '''

        self._q_get_user_has_song_like_query = '''
        SELECT
            sl.song_id
        FROM song_likes AS sl
        WHERE sl.song_id=%s
        AND sl.user_id=%s;
        '''

        self._q_get_number_of_songs = '''
        SELECT
            COUNT(*)
        FROM (
            SELECT
                DISTINCT t.songid
            FROM transcription AS t
            WHERE t.id=ANY(%s)
        ) AS ttable;
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
                self.logger.error('Failed to execute psql query')
                self.logger.error(e)
                return tuple()
            finally:
                if cur:
                    cur.close()
        return deco

    @reconnect
    def get_lyrics_map(self, cur, songid):
        cur.execute(self._q_get_ordered_lyrics_map, (songid,))
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
            cur.execute(self._q_sond_id_from_transcription, (id,))
            current_id = cur.fetchone()[0]
            if current_id not in songs_seq:
                songs_seq.append(current_id)
                for element in arr_to_rearrange:
                    if str(element['id']) == str(current_id):
                        result_array.append(element)
        return result_array if len(result_array) == len(arr_to_rearrange) else arr_to_rearrange

    @reconnect
    def get_all_song_ids_and_timestamps(self, cur, ids):
        cur.execute(self._q_select_unique_songs, (ids,))
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
                            'lyrics': self.get_closest_lyrics(chunk[2], song_id)
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
        cur.execute(self._q_select_song, (id,))
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
        cur.execute(self._q_add_to_query_history, (query,))
        self.conn.commit()

    @reconnect
    def add_song_history(self, cur, _id):
        if not _id:
            return
        self.logger.info('Add song to history: `{}`'.format(_id))
        cur.execute(self._q_add_to_song_history, (_id,))
        self.conn.commit()

    @reconnect
    def get_popular_queries(self, cur, limit=10):
        cur.execute(self._q_popular_queries.format(limit))
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
        cur.execute(self._q_popular_song_ids.format(limit))
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
        cur.execute(self._q_all_songs.format(limit, offset))
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
        cur.execute(self._q_select_album, (id,))
        value = cur.fetchone()
        if value and len(value) > 2:
            return {
                'title': value[0],
                'cover_id': value[1],
                'year': value[2]
            }

    @reconnect
    def get_closest_lyrics(self, cur, lyr_id, song_id):
        try:
            cur.execute(self._q_closest_lyrics, (song_id, [lyr_id, lyr_id - 1, lyr_id + 1]))
            lyrics = cur.fetchall()
            messed_lyrics = [
                i[0]
                for i in lyrics
                if i[0] and i[0] != "" and "chorus" not in i[0].lower()
            ]
            result_array = []
            counter = 0
            for line in messed_lyrics:
                if len(result_array) == 3:
                    break
                if len(line) < 59:
                    result_array.append(line)
                else:
                    split_line = get_up_set(line)
                    if not split_line:
                        continue
                    if len(split_line) >= 3 and counter == 2:
                            return split_line
                    if len(split_line) > 0:
                        if counter == 1:
                            result_array.append(split_line[-1])
                        if counter == 2:
                            result_array.extend(split_line)
                        if counter == 3:
                            result_array.append(split_line[0])
            return result_array
        except Exception as e:
            self.logger.error(e)
            return []

    @reconnect
    def get_user_by_token(self, cur, token):
        cur.execute(self._q_get_user_by_token_query, (token,))
        user = cur.fetchone()

        if user:
            return {
                'id': user[0],
            }

        return None

    @reconnect
    def get_user_likes_songs(self, cur, user, offset, limit):
        if user is None or user.get('id') is None:
            return []

        cur.execute(self._q_get_user_likes_songs_query.format(limit, offset), (user.get('id'),))
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
    def add_like_song(self, cur, user, song_id):
        if user is None or user.get('id') is None:
            return False

        try:
            cur.execute(self._q_add_like_song_query, (song_id, user.get('id')))
            self.conn.commit()
            return True
        except:
            return False

    @reconnect
    def remove_like_song(self, cur, user, song_id):
        if user is None or user.get('id') is None:
            return False

        try:
            cur.execute(self._q_remove_like_song_query, (song_id, user.get('id')))
            self.conn.commit()
            return True
        except:
            return False

    @reconnect
    def get_has_like_song(self, cur, user, song_id):
        if user is None or user.get('id') is None:
            return False

        cur.execute(self._q_get_user_has_song_like_query, (song_id, user.get('id')))
        row = cur.fetchone()

        return row is not None

    @reconnect
    def get_found_songs_number(self, cur, trascription_ids):
        cur.execute(self._q_get_number_of_songs, (trascription_ids,))
        row = cur.fetchone()
        return row[0] if row and len(row) != 0 else None


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


def get_up_set(s):
    """
    Example:
    s = "So, I won’t let you close enough to hurt me No, I won’t rescue you to just desert me I can’t give you the heart you think you gave me It’s time to say goo
    dbye to turning tables"
    """

    if len(s) == 0:
        return []
    try:
        # biggest_ind -- array os capital letters in str
        biggest_ind = [i for i, c in enumerate(s) if c.isupper()]
        # biggest_ind = [0, 4, 46, 50, 89, 140]

        # diff_between_uppers array of arrays diff between capital letters
        #      first - is diff between curr and prev
        #      second - is index of capital letter in the string
        diff_between_uppers = [[0, 0]]
        for i, c in enumerate(biggest_ind):
            if i == 0:
                continue
            diff_between_uppers.append([c - diff_between_uppers[-1][1], c])
        diff_between_uppers.sort(key=lambda d: d[0], reverse=True)
        # diff_between_uppers = [[51, 140], [42, 46], [39, 89], [4, 4], [4, 50], [0, 0]]

        # result_indices -- indexes on which we will split string
        result_indices = [diff_between_uppers[0][1]]
        # these are const :)
        biggest_diff = 5
        smallest_str_len = 59
        for i, j in enumerate(diff_between_uppers):
            if i == 0:
                continue
            curr_diff = diff_between_uppers[i - 1][0] - diff_between_uppers[i][0]
            smallest_str_len = min(smallest_str_len, curr_diff)
            biggest_diff = min(biggest_diff, curr_diff)
            if curr_diff > biggest_diff * 2:
                break
            result_indices.append(diff_between_uppers[i][1])
        # result_indices = [140, 46, 89]
        if 0 not in result_indices:
            result_indices.append(0)
        result_indices.sort()

        # result_indices = [0, 46, 89, 140]
        if result_indices[1] - result_indices[0] < smallest_str_len / 2 and len(result_indices) > 2:
            del result_indices[1]
        if len(s) - result_indices[-1] < smallest_str_len / 2 and len(result_indices) > 2:
            del result_indices[-1]

        # final -- array of prepared split string
        final = []
        for i, j in enumerate(result_indices):
            if i == 0:
                continue
            final.append(s[result_indices[i - 1]: j].strip())
        final.append(s[result_indices[-1]:].strip())
        return final
    except Exception as e:
        # OKAY Exception -- let's just split into two
        l = len(s) / 2
        split_index_upper = 0
        split_index_space = len(s)
        for i, ch in enumerate(s[l:]):
            if ch.isupper():
                split_index_upper = l + i
                break
            if ch == ' ':
                split_index_space = min(split_index_space, l + i)

        return [s[0:split_index_upper].strip(), s[split_index_upper:].strip()] \
            if split_index_upper != 0 \
            else [s[0:split_index_space].strip(), s[split_index_space:].strip()]