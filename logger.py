import os
import logging

from configuration import CONFIG

cwd = os.getcwd()
log_filename = cwd + '/' + CONFIG.get('DEFAULT', 'LOG_OUTPUT_DIR')

logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s \n'
)

logger = logging.getLogger(__name__)