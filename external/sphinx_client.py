import pymysql.cursors


class SphinxSearch(object):
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection = pymysql.connect(
            host=self.host, port=self.port, user=self.user, passwd=self.password, charset='utf8', db='')

    def reconnect(func):
        def deco(self, *args, **kwargs):
            if not self.connection.open:
                self.connection = pymysql.connect(
                    host=self.host, port=self.port, user=self.user, passwd=self.password, charset='utf8', db='')
            return func(self, *args, **kwargs)
        return deco

    def connect(self):
        self.connection = pymysql.connect(
            host=self.host, port=self.port, user=self.user, passwd=self.password, charset='utf8', db='')

    @reconnect
    def find_songs(self, key_word, percent='0.7', recurse_on_fail=True):
        try:
            with self.connection.cursor() as cursor:
                # supposed to be injection free
                # http://initd.org/psycopg/docs/usage.html#the-problem-with-the-query-parameters
                cursor.execute('select * from songs_search where match(%s);',
                               ('"' + key_word + '"/{}'.format(percent),))
                result = cursor.fetchall()
            return [
                int(x[0])
                for x in result
                if x and len(x) > 0
            ]
        except:
            if recurse_on_fail:
                return self.find_songs(key_word, recurse_on_fail=False)
            else:
                return []
