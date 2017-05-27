import logging
import os
import sys

from external.cropper_client import CropperDaemon
from external.mongo_client import MongoC
from external.pg_client import PsgClient
from external.sphinx_client import SphinxSearch

logger = logging.getLogger('cherry-pick-api')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

try:
    sphinx = SphinxSearch(os.environ['SPHINX_HOST'], 9306, '', '')
    postgres = PsgClient(
        logger, os.environ['PSQL_HOST'], os.environ['PSQL_USER'], os.environ['PSQL_PASSWORD'], 'track_bar')
    cropper = CropperDaemon(os.environ['CROPPER_HOST'], 8880)
    mongo = MongoC(os.environ['MONGO_HOST'], 'test', 'fs')
except Exception as e:
    print(e)
    sys.exit(1)
