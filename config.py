import os
import sys

from external import data_getter

try:
    sphinx = data_getter.SphinxSearch(os.environ['SPHINX_HOST'], 9306, '', '')
    postgres = data_getter.PsgClient(
        os.environ['PSQL_HOST'], os.environ['PSQL_USER'], os.environ['PSQL_PASSWORD'], 'track_bar')
    cropper = data_getter.CropperDemon(os.environ['CROPPER_HOST'], 8880)
except Exception as e:
    print(e)
    sys.exit(1)

