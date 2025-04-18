import time
import subprocess
import logging
import sys
import os
from threading import Thread
from queue import Queue, Empty

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger("bot_manager")

# Paths
delete_webhook_script = "delete_webhook.py"
main_bot_script = "prediction_bot.py"
max_restarts = 10
max_consecutive_errors = 3
error_cooldown = 60  # Seconds to wait after consecutive errors

def enqueue_output(pipe, queue):
    """Read from pipe and add to queue."""
    for line in iter(pipe.readline, b''):
        queue.put(line)
    pipe.close()

def run_delete_webhook():
    """Run the webhook deletion script and return success status"""
    logger.info("Running webhook deletion script...")
    try:
        result = subprocess.run([sys.executable, delete_webhook_script], 
                                capture_output=True, 
                                text=True, 
                                check=True)
        logger.info(f"Webhook deletion output: {result.stdout.strip()}")
        return "Successfully deleted webhook" in result.stdout or "No active webhook found" in result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Webhook deletion script failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def run_bot(restart_count=0, consecutive_errors=0):
    """Run the main bot script with error handling and restart capability"""
    if restart_count >= max_restarts:
        logger.error(f"Reached maximum restarts ({max_restarts}). Exiting.")
        return

    # If we've had multiple consecutive errors, pause before continuing
    if consecutive_errors >= max_consecutive_errors:
        logger.warning(f"Multiple consecutive errors detected. Pausing for {error_cooldown} seconds...")
        time.sleep(error_cooldown)
        consecutive_errors = 0
    
    logger.info(f"Starting bot (attempt {restart_count + 1}/{max_restarts + 1})...")
    
    # Always run webhook deletion before starting
    if restart_count > 0:
        if not run_delete_webhook():
            logger.error("Failed to delete webhook before restart, waiting...")
            time.sleep(10)  # Wait a bit longer
            if not run_delete_webhook():
                logger.error("Still failed to delete webhook. Continuing anyway...")
    
    # Start the bot as a subprocess
    process = subprocess.Popen(
        [sys.executable, main_bot_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1  # Line buffered
    )
    
    # Set up queues and threads for non-blocking reads
    stdout_queue = Queue()
    stderr_queue = Queue()
    stdout_thread = Thread(target=enqueue_output, args=(process.stdout, stdout_queue))
    stderr_thread = Thread(target=enqueue_output, args=(process.stderr, stderr_queue))
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()
    
    # Monitor the process output
    webhook_error_detected = False
    run_time_start = time.time()
    
    # Keep monitoring until process ends
    while process.poll() is None:
        # Check stderr
        try:
            while True:  # Process all available lines
                line = stderr_queue.get_nowait()
                print(line, end='', flush=True)
                if "Webhook conflict detected" in line:
                    webhook_error_detected = True
                    logger.warning("Webhook conflict detected in bot process")
        except Empty:
            pass
        
        # Check stdout
        try:
            while True:  # Process all available lines
                line = stdout_queue.get_nowait()
                print(line, end='', flush=True)
        except Empty:
            pass
            
        # If webhook error was detected, break the loop
        if webhook_error_detected:
            break
            
        # Brief sleep to avoid CPU spin
        time.sleep(0.1)
    
    # Process ended or webhook error detected
    run_time = time.time() - run_time_start
    
    if webhook_error_detected:
        logger.info("Terminating bot process due to webhook conflict")
        process.terminate()
        
        # Wait for process to terminate
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("Bot process didn't terminate gracefully, forcing termination")
            process.kill()
        
        # Run webhook deletion
        if run_delete_webhook():
            logger.info("Webhook successfully deleted, restarting bot")
            # Wait a moment before restarting
            time.sleep(5)  # Increased wait time
            return run_bot(restart_count + 1, consecutive_errors + 1)
        else:
            logger.error("Failed to delete webhook, waiting longer...")
            time.sleep(15)
            return run_bot(restart_count + 1, consecutive_errors + 1)
    else:
        # Process ended normally or due to error
        returncode = process.wait()
        
        # Drain any remaining output
        try:
            while True:
                line = stdout_queue.get_nowait()
                print(line, end='', flush=True)
        except Empty:
            pass
            
        try:
            while True:
                line = stderr_queue.get_nowait()
                print(line, end='', flush=True)
        except Empty:
            pass
        
        logger.info(f"Bot process ended with return code {returncode} after running for {run_time:.1f} seconds")
        
        if returncode != 0:
            logger.error(f"Bot process failed with error")
            # If the process ran for less than 10 seconds, consider it an error
            if run_time < 10:
                logger.warning("Bot crashed quickly after start, treating as consecutive error")
                return run_bot(restart_count + 1, consecutive_errors + 1)
            else:
                # If it ran for a while before error, reset consecutive error count
                return run_bot(restart_count + 1, 0)
        
        return

if __name__ == "__main__":
    # Always make sure webhook is deleted before starting
    run_delete_webhook()
    
    # Start the bot with monitoring
    run_bot()