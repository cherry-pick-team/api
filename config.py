import os

from external import data_getter
from external import genius


sphinx = data_getter.SphinxSearch('localhost', 9306, '', '')
postgres = data_getter.PsgClient('postgres', os.environ['PSQL_PASSWORD'], 'track_bar')
cropper = data_getter.CropperDemon('localhost', 8080)
genius = genius.Genius(os.environ['GENIUS_KEY'])