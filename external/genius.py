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
        if not json or json['meta']['status'] != 200:
            return {}
        info = {
            'song': {
                'id': id,
                'title': json['response']['song']['title'],
                'singers': [
                    {
                        'id': json['response']['song']['primary_artist']['id'],
                        'name': json['response']['song']['primary_artist']['name'],
                    }
                ]
            },
            'album': {
                'id': json['response']['song']['album']['id'],
                'name': json['response']['song']['album']['name'],
                'cover_url': json['response']['song']['album']['cover_art_url'],
            }
        }
        for featured_artist in json['response']['song']['featured_artists']:
            info['song']['singers'].append({
                'id': featured_artist['id'],
                'name': featured_artist['name'],
            })
        return info
