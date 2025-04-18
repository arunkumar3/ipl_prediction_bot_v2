import os
import logging
import sys
import re  # Added import for regex

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("--- Starting test_env.py ---")

def resolve_shared_variable(value):
    """Replaces ${{ shared.VAR }} syntax with actual env var value."""
    match = re.search(r'\${{ shared\.(\w+) }}', value)
    if match:
        var_name = match.group(1)
        return os.environ.get(var_name)
    return value

bot_token = resolve_shared_variable(os.environ.get("BOT_TOKEN", ""))
group_chat_id = resolve_shared_variable(os.environ.get("GROUP_CHAT_ID", ""))

logger.info(f"BOT_TOKEN: {bot_token}")
logger.info(f"GROUP_CHAT_ID: {group_chat_id}")

if bot_token is None or group_chat_id is None:
    logger.error("BOT_TOKEN or GROUP_CHAT_ID is not set! Exiting.")
    sys.exit(1)

logger.info("--- test_env.py completed ---")