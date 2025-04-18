import asyncio
import logging
import time
import sys
from telegram import Bot

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Your bot token
BOT_TOKEN = "7897221989:AAHZoD6r03Qj21v4za2Zha3XFwW5o5Hw4h8"

async def delete_webhook_with_retries():
    """Delete webhook with multiple retries and verification."""
    bot = Bot(token=BOT_TOKEN)
    max_attempts = 5  # Increased from 3
    
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_attempts} to check webhook status")
            
            # Check current webhook status
            webhook_info = await bot.get_webhook_info()
            
            if webhook_info.url:
                logger.warning(f"Found active webhook: {webhook_info.url}")
                logger.info(f"Pending updates: {webhook_info.pending_update_count}")
                
                # Force drop pending updates
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook deletion requested")
                
                # Verify deletion with extra wait time
                time.sleep(3)  # Increased wait time
                webhook_info = await bot.get_webhook_info()
                
                if webhook_info.url:
                    logger.error(f"Webhook still active after deletion attempt: {webhook_info.url}")
                else:
                    logger.info("Successfully deleted webhook!")
                    return True
            else:
                logger.info("No active webhook found")
                return True
                
        except Exception as e:
            logger.error(f"Error during webhook deletion attempt {attempt}: {e}")
        
        # Wait before retrying with longer backoff
        wait_time = 5 * attempt  # Increased backoff time
        logger.info(f"Waiting {wait_time} seconds before next attempt...")
        time.sleep(wait_time)
    
    logger.error(f"Failed to delete webhook after {max_attempts} attempts")
    return False

if __name__ == "__main__":
    success = asyncio.run(delete_webhook_with_retries())
    if success:
        print("Webhook deletion successful")
        print("Successfully deleted webhook!")
        sys.exit(0)
    else:
        print("Webhook deletion failed")
        sys.exit(1)