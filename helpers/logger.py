
import logging
import sys

log = logging.getLogger('logger')
log.setLevel(logging.INFO)

# Console handler with formater
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)

log.addHandler(ch)