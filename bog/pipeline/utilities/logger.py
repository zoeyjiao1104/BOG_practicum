"""
logger.py
"""

import logging

# Configure stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# Create logger and attach handler
logger = logging.getLogger('BOG PIPELINE')
logger.setLevel(logging.INFO)
logger.addHandler(ch)