import requests


class Genius(object):
    def __init__(self, token):
        self.genius_key = token
        self.base_url = 'http://api.genius.com'
        self.headers = {'Authorization': 'Bearer %s' % self.genius_key}

    def get_info(self, id):
        search_url = self.base_url + '/songs/{}'.format(id)
        response = requests.get(search_url, headers=self.headers)
        json = response.json()
        print(id)
        if not json or json['meta']['status'] != 200 or not json['response']['song']:
            return {}
        info = {
            'song': {
                'id': id,
                'title': json['response']['song'].get('title', 'unknown'),
                'singers': [
                    {
                        'id': json['response']['song'].get('primary_artist', {}).get('id', 0),
                        'name': json['response']['song'].get('primary_artist', {}).get('name', 'unknown'),
                    }
                ]
            }
        }
        if json['response']['song'].get('album'):
            info['album'] = {
                'id': json['response']['song']['album'].get('id'),
                'name': json['response']['song']['album'].get('name'),
                'cover_url': json['response']['song']['album'].get('cover_art_url'),
            }
        for featured_artist in json['response']['song'].get('featured_artists'):
            info['song']['singers'].append({
                'id': featured_artist['id'],
                'name': featured_artist['name'],
            })
        return info
