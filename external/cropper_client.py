import json

import requests


class CropperDaemon(object):
    def __init__(self, host, port):
        self.request_path = 'http://{}:{}/get_song/'.format(host, port)

    def get_song(self, object_id, intervals):
        request_json = {
            'objectId': object_id,
            'intervals': intervals
        }
        return requests.post(self.request_path, data=json.dumps(request_json))
