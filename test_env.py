import os
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("--- Starting test_env.py ---")
group_chat_id = os.environ.get("GROUP_CHAT_ID")
logger.info(f"GROUP_CHAT_ID: {group_chat_id}")

if group_chat_id is None:
    logger.error("GROUP_CHAT_ID is not set! Exiting.")
    sys.exit(1)

try:
    group_chat_id_int = int(group_chat_id)
    logger.info(f"GROUP_CHAT_ID (int): {group_chat_id_int}")
except ValueError:
    logger.error(f"Invalid GROUP_CHAT_ID value: {group_chat_id}. Exiting.")
    sys.exit(1)

logger.info("--- test_env.py completed ---")