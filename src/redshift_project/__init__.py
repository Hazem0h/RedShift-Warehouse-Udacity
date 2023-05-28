import logging
import sys

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)