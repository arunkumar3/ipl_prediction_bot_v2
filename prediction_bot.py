import logging
import asyncio
from datetime import datetime
import pytz
import pandas as pd
import time
import signal
import sys
from telegram import Update, Bot
from telegram.ext import (
    Application,
    CommandHandler,
    PollAnswerHandler,
    ContextTypes,
    CallbackContext,
    MessageHandler,
    filters,
    ConversationHandler
)
from telegram.error import Conflict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import nest_asyncio
from google.oauth2 import service_account
import gspread
import json
import os

nest_asyncio.apply()

# === Configuration ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
GROUP_CHAT_ID = int(os.environ.get("GROUP_CHAT_ID"))  # Important: Convert to int!
PREDICTIONS_SHEET_ID = os.environ.get("PREDICTIONS_SHEET_ID")
POLL_MAP_SHEET_ID = os.environ.get("POLL_MAP_SHEET_ID")
SCHEDULE_CSV = "ipl_schedule.csv"
ADMIN_USER_IDS = [384743804]  # Add admin user IDs here

# === Logging Setup ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
def log_all_env_vars():
    """Logs all relevant environment variables."""
    logger.info("--- Environment Variables ---")
    logger.info(f"BOT_TOKEN: {os.environ.get('BOT_TOKEN')}")
    logger.info(f"GROUP_CHAT_ID: {os.environ.get('GROUP_CHAT_ID')}")
    logger.info(f"PREDICTIONS_SHEET_ID: {os.environ.get('PREDICTIONS_SHEET_ID')}")
    logger.info(f"POLL_MAP_SHEET_ID: {os.environ.get('POLL_MAP_SHEET_ID')}")
    
    # Log GOOGLE_CREDENTIALS_JSON carefully (it might be very long)
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if creds_json:
        logger.info("GOOGLE_CREDENTIALS_JSON is set (length: " + str(len(creds_json)) + ")")
        # If you want to see a snippet (for debugging ONLY, not in production!), 
        # be very careful not to log the entire secret!
        # logger.info("GOOGLE_CREDENTIALS_JSON (first 100 chars): " + creds_json[:100]) 
    else:
        logger.info("GOOGLE_CREDENTIALS_JSON is NOT set")

    logger.info(f"ADMIN_USER_IDS: {os.environ.get('ADMIN_USER_IDS')}")
    logger.info("--- End Environment Variables ---")

# === Google Sheets Interaction ===
def authorize_gspread():
    """Authorizes Google Sheets API client."""
    try:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")  # Get from env variable
        if not creds_json:
            raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set")

        creds_data = json.loads(creds_json)  # Load JSON string
        creds = service_account.Credentials.from_service_account_info(
            creds_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Error authorizing Google Sheets: {e}")
        raise
    
def get_sheet(gc, sheet_id):
    """Retrieves a Google Sheet by its ID."""
    try:
        return gc.open_by_key(sheet_id).sheet1
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error accessing sheet {sheet_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Generic error accessing sheet {sheet_id}: {e}")
        raise

def get_predictions_df(sheet):
    """Retrieves predictions data from the Google Sheet."""
    try:
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error getting predictions data: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error getting predictions data: {e}")
        return pd.DataFrame()

def save_predictions_df(sheet, df):
    """Saves predictions data to the Google Sheet."""
    try:
        # Log the dataframe state before saving
        logger.info(f"Saving dataframe with {len(df)} rows")
        
        # Ensure proper data types before converting to strings for saving
        if 'MatchNo' in df.columns:
            df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce').fillna(0).astype(int)
        if 'Correct' in df.columns:
            df['Correct'] = pd.to_numeric(df['Correct'], errors='coerce').fillna(0).astype(int)
        
        sheet.clear()
        
        # Convert dataframe to list for writing to sheet
        data_to_write = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        
        result = sheet.update(data_to_write)
        
        # Log the result of the update operation
        logger.info(f"Sheet update result: {result}")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error saving predictions data: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving predictions data: {e}")
        raise

def get_poll_map(sheet):
    """Retrieves the poll ID to match number mapping from the Google Sheet."""
    try:
        rows = sheet.get_all_records()
        return {
            str(row["poll_id"]): int(row["MatchNo"])
            for row in rows
            if "poll_id" in row and row.get("poll_id") and "MatchNo" in row and row.get("MatchNo")
        }
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error getting poll map: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error getting poll map: {e}")
        return {}

def save_poll_id(sheet, poll_id, match_no):
    """Saves a poll ID and its corresponding match number to the Google Sheet."""
    try:
        sheet.append_row([str(poll_id), match_no])
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error saving poll ID: {e}")
        raise
    except Exception as e:
        logger.error(f"Error saving poll ID: {e}")
        raise

# === Helper: Load Schedule ===
def load_schedule_mapping(csv_file):
    """Loads the match schedule from a CSV file."""
    schedule_mapping = {}
    try:
        df = pd.read_csv(csv_file)
        df.columns = df.columns.str.strip()

        required_cols = ["MatchNo", "Date", "Day", "Teams", "MatchTime", "Venue", "PollStartTime", "PollEndTime"]
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV missing one or more required columns: {required_cols}")

        for index, row in df.iterrows():
            try:
                match_no = int(str(row["MatchNo"]).strip())
                schedule_mapping[match_no] = {
                    "Date": str(row["Date"]).strip(),
                    "Day": str(row["Day"]).strip(),
                    "Teams": str(row["Teams"]).strip(),
                    "MatchTime": str(row["MatchTime"]).strip(),
                    "Venue": str(row["Venue"]).strip(),
                    "PollStartTime": str(row["PollStartTime"]).strip(),
                    "PollEndTime": str(row["PollEndTime"]).strip(),
                }
                teams_str = schedule_mapping[match_no]["Teams"]
                if " vs " not in teams_str and not (
                        "Qualifier" in teams_str or "Eliminator" in teams_str or "Final" in teams_str):
                    logger.warning(
                        f"Match {match_no}: Teams format '{teams_str}' might be incorrect (expected 'Team A vs Team B')."
                    )
                try:
                    datetime.strptime(schedule_mapping[match_no]["Date"], "%d %b %Y")
                    datetime.strptime(schedule_mapping[match_no]["PollStartTime"], "%I:%M %p")
                except ValueError as time_e:
                    logger.error(f"Invalid date/time format in CSV for Match {match_no}: {time_e}. Row: {row.to_dict()}")
                    del schedule_mapping[match_no]
                    continue

            except (ValueError, TypeError, KeyError) as e:
                logger.error(f"Error processing CSV row {index + 2}: {row.to_dict()}. Exception: {e}")
                continue

    except FileNotFoundError:
        logger.error(f"Fatal Error: Schedule CSV file not found at {csv_file}")
        raise
    except ValueError as e:
        logger.error(f"Fatal Error loading schedule from CSV: {e}")
        raise
    except Exception as e:
        logger.error(f"Fatal Error loading schedule from CSV: {e}")
        raise
    logger.info(f"Successfully loaded {len(schedule_mapping)} matches from schedule.")
    return schedule_mapping

# === Webhook Handling ===
async def ensure_webhook_deleted():
    """Ensure webhook is deleted before starting the bot."""
    bot = Bot(token=BOT_TOKEN)
    max_attempts = 3
    
    for attempt in range(max_attempts):
        try:
            # Check webhook status
            webhook_info = await bot.get_webhook_info()
            
            if not webhook_info.url:
                logger.info("No active webhook found. Ready for polling.")
                return True
                
            logger.warning(f"Found active webhook: {webhook_info.url} - Attempting to delete...")
            await bot.delete_webhook(drop_pending_updates=True)
            
            # Verify deletion
            time.sleep(1)  # Brief pause to ensure deletion processed
            webhook_info = await bot.get_webhook_info()
            
            if not webhook_info.url:
                logger.info("Successfully deleted webhook. Ready for polling.")
                return True
            else:
                logger.error(f"Webhook still active after deletion attempt: {webhook_info.url}")
                
            # Wait before retrying
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error during webhook check/deletion (attempt {attempt+1}): {e}")
            time.sleep(2)
    
    logger.error(f"Failed to delete webhook after {max_attempts} attempts.")
    return False

# === Bot Commands and Logic ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a welcome message when the command /start is issued."""
    if not update.effective_user:
        logger.warning("start called without effective_user.")
        return
        
    user = update.effective_user
    await update.message.reply_html(
        f"Hi {user.mention_html()}! I'm the IPL Prediction Bot.\n"
        f"I'll help you predict match outcomes and track your predictions.\n"
        f"Use /help to see available commands."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /help is issued."""
    help_text = (
        "🏏 *IPL Prediction Bot Commands* 🏏\n\n"
        "/startpoll <match_no> - Start a prediction poll for a match\n"
        "/leaderboard - Show the current prediction leaderboard\n"
        "/score <match_no> <winner> - Score a match (admin only)\n"
        "/get_chat_id - Get the current chat ID\n"
        "/help - Show this help message\n\n"
        "Make your predictions by voting in the polls!"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the chat ID to the group."""
    if not update.effective_chat:
        logger.warning("get_chat_id called without effective_chat.")
        return
    chat_id_to_send = update.effective_chat.id
    message_text = f"Chat ID: `{chat_id_to_send}`"
    try:
        await context.bot.send_message(
            chat_id=chat_id_to_send,
            text=message_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Error in get_chat_id sending message: {e}")

async def scheduled_poll_callback(context: CallbackContext):
    """Callback for scheduled polls."""
    job = context.job
    match_no = job.data["match_no"]
    match_info = job.data["match_info"]
    
    logger.info(f"Running scheduled poll job for Match {match_no}")
    
    try:
        match_name = match_info["Teams"]
        venue = match_info["Venue"]

        if "Qualifier" in match_name or "Eliminator" in match_name or "Final" in match_name:
            await context.bot.send_message(
                GROUP_CHAT_ID,
                f"Match {match_no}: {match_name}\nThe teams for this match are not yet decided. The poll will be created later.",
            )
            return  # Skip poll creation

        options = match_name.split(" vs ")
        if len(options) != 2:
            logger.error(
                f"Cannot create poll for Match {match_no}. Incorrect number of teams after splitting: {options}"
            )
            return

        poll_message = await context.bot.send_poll(
            GROUP_CHAT_ID, 
            f"Match {match_no}: {match_name}\nVenue: {venue}\nWho will win?", 
            options, 
            is_anonymous=False
        )
        logger.info(f"Poll sent for Match {match_no}. Poll ID: {poll_message.poll.id}")

        try:
            # Get Google Sheets client from application context
            gc = context.application.bot_data["gc"]
            poll_map_sheet_id = context.application.bot_data["poll_map_sheet_id"]
            poll_map_sheet = get_sheet(gc, poll_map_sheet_id)
            save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
            logger.info(f"Poll ID {poll_message.poll.id} mapped to Match {match_no} in Google Sheet.")
        except KeyError as ke:
            logger.error(
                f"Error accessing sheet ID or gc from bot_data while saving poll ID {poll_message.poll.id}: {ke}"
            )
        except Exception as sheet_e:
            logger.error(
                f"Error saving poll ID {poll_message.poll.id} to Google Sheet for Match {match_no}: {sheet_e}"
            )
    except Exception as e:
        logger.error(f"Error in scheduled_poll for Match {match_no}: {e}")

async def startpoll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts a poll manually."""
    if not update.message or not update.effective_chat:
        logger.warning("startpoll called without message or effective_chat.")
        return

    try:
        if not context.args:
            await update.message.reply_text("Usage: /startpoll <match_no>")
            return

        try:
            match_no = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid match number. Please provide an integer.")
            return

        if 'schedule_mapping' not in globals():
            logger.error("Schedule mapping not loaded.")
            await update.message.reply_text("Error: Schedule data is not available.")
            return
        match_info = schedule_mapping.get(match_no)

        if not match_info:
            await update.message.reply_text(f"Match number {match_no} not found in the schedule.")
            return

        match_name = match_info["Teams"]
        venue = match_info["Venue"]
        if "Qualifier" in match_name or "Eliminator" in match_name or "Final" in match_name:
            await update.message.reply_text(
                f"Match {match_no}: {match_name}\nThe teams for this match are not yet decided. The poll will be created later.")
            return

        options = match_name.split(" vs ")
        if len(options) != 2:
            logger.error(f"Cannot create manual poll for Match {match_no}. Invalid team format: {match_name}")
            await update.message.reply_text(
                f"Error: Invalid team format for Match {match_no} ('{match_name}'). Cannot create poll.")
            return

        poll_message = await context.bot.send_poll(
            update.effective_chat.id, 
            f"Match {match_no}: {match_name}\nVenue: {venue}\nWho will win?", 
            options, 
            is_anonymous=False
        )
        logger.info(
            f"Manual poll sent for Match {match_no} by {update.effective_user.name if update.effective_user else 'Unknown'}. Poll ID: {poll_message.poll.id}")

        try:
            gc = context.bot_data["gc"]
            poll_map_sheet = get_sheet(gc, POLL_MAP_SHEET_ID)
            save_poll_id(poll_map_sheet, poll_message.poll.id, match_no)
            logger.info(f"Manual Poll ID {poll_message.poll.id} mapped to Match {match_no} in Google Sheet.")
        except Exception as sheet_e:
            logger.error(
                f"Error saving manual poll ID {poll_message.poll.id} to Google Sheet for Match {match_no}: {sheet_e}")

    except Exception as e:
        logger.exception(f"Error in startpoll command: {e}")

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's poll answer."""
    if not update.poll_answer:
        logger.warning("handle_poll_answer called without poll_answer.")
        return

    answer = update.poll_answer
    poll_id = answer.poll_id
    user = answer.user
    option_ids = answer.option_ids

    if not user:
        logger.warning(f"Received poll answer for poll {poll_id} without user information. Skipping.")
        return

    logger.info(f"Received poll answer from {user.full_name} (ID: {user.id}) for poll {poll_id}. Options: {option_ids}")

    try:
        gc = context.bot_data["gc"]
        try:
            poll_map_sheet = get_sheet(gc, POLL_MAP_SHEET_ID)
            pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
            poll_map = get_poll_map(poll_map_sheet)
        except Exception as sheet_e:
            logger.error(f"Failed to get sheet data in handle_poll_answer: {sheet_e}")
            return

        match_no = poll_map.get(str(poll_id))

        if match_no is None:
            logger.warning(f"No match found for poll_id: {poll_id} in the poll map. Maybe an old poll?")
            return

        if 'schedule_mapping' not in globals():
            logger.error(f"Schedule mapping not loaded while handling answer for poll {poll_id}.")
            return
        match_info = schedule_mapping.get(match_no)

        if not match_info:
            logger.warning(f"No match info found for match_no: {match_no} (from poll {poll_id})")
            return

        # Handle vote retraction (empty option_ids)
        if not option_ids:
            logger.info(
                f"{user.full_name} retracted vote for poll {poll_id} (Match {match_no}). Removing prediction.")
            try:
                df = get_predictions_df(pred_sheet)
                if df.empty:
                    logger.info("Prediction sheet is empty, nothing to remove.")
                    return

                # Define types for filtering
                df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')
                username = user.full_name  # Or user.username if preferred and available

                # Use case-insensitive matching for username
                row_mask = (df["MatchNo"] == match_no) & (df["Username"].str.strip().str.lower() == username.strip().lower())
                if row_mask.any():
                    df = df[~row_mask]  # Remove rows matching the condition
                    save_predictions_df(pred_sheet, df)
                    logger.info(f"Removed prediction for {username} for Match {match_no}.")
                else:
                    logger.info(
                        f"No existing prediction found for {username} for Match {match_no} to remove.")
            except Exception as e:
                logger.error(f"Error removing prediction for {username}, Match {match_no}: {e}")
            return  # Stop processing after retraction

        # --- Process valid vote ---
        chosen_option_index = option_ids[0]  # Assuming single choice poll
        options = match_info["Teams"].split(" vs ")
        if chosen_option_index >= len(options):
            logger.error(
                f"Invalid option index {chosen_option_index} for poll {poll_id}, Match {match_no}. Options: {options}")
            return

        chosen_team = options[chosen_option_index].strip()
        username = user.full_name  # Or user.username

        try:
            df = get_predictions_df(pred_sheet)

            # Ensure columns exist, create if first vote ever
            expected_cols = ["MatchNo", "Match", "Username", "Prediction", "Correct"]
            if df.empty:
                df = pd.DataFrame(columns=expected_cols)
            else:
                # Add missing columns if necessary (e.g., if sheet was manually cleared)
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None if col != "Correct" else 0  # Default Correct to 0

            # Ensure MatchNo is numeric for comparison
            df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')
            
            # Ensure Correct is numeric
            if 'Correct' in df.columns:
                df['Correct'] = pd.to_numeric(df['Correct'], errors='coerce').fillna(0).astype(int)

            # Find if user already voted for this match - use case-insensitive comparison
            row_mask = (df["MatchNo"] == match_no) & (df["Username"].str.strip().str.lower() == username.strip().lower())
            match_display_name = match_info["Teams"]  # Use the full team string

            if not row_mask.any():  # Changed from df[row_mask].empty for clarity
                # Add new prediction row
                new_row_dict = {
                    "MatchNo": int(match_no),
                    "Match": match_display_name,
                    "Username": username,  # Use consistent username format
                    "Prediction": chosen_team,
                    "Correct": 0  # Initialize Correct column as integer
                }
                # Ensure new row aligns with DataFrame columns
                new_row_df = pd.DataFrame([new_row_dict], columns=df.columns)
                df = pd.concat([df, new_row_df], ignore_index=True)
                logger.info(f"Recorded new vote: {username} voted {chosen_team} for Match {match_no}.")
            else:
                # Add debug logging to understand what's happening
                logger.info(f"Found existing vote for {username} (Match {match_no}). Current prediction: {df.loc[row_mask, 'Prediction'].values}")
                
                # Update existing prediction
                df.loc[row_mask, "Prediction"] = chosen_team
                df.loc[row_mask, "Correct"] = 0  # Reset correctness score on vote change as integer
                logger.info(f"Updated vote: {username} changed vote to {chosen_team} for Match {match_no}.")

            # Save the prediction data
            save_predictions_df(pred_sheet, df)
            logger.info(f"Successfully saved prediction data to sheet for {username}, Match {match_no}")

        except Exception as e:
            logger.error(f"Error processing vote for {username}, Match {match_no}: {e}")

    except Exception as e:
        logger.exception(f"General error in handle_poll_answer for poll {poll_id}: {e}")

async def score_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scores a match and updates predictions."""
    if not update.message or not update.effective_chat:
        logger.warning("score_match called without message or effective_chat.")
        return

    # Check if user is admin
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("Sorry, only admins can use this command.")
        return

    try:
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /score <match_no> <Winning Team Name>")
            return

        try:
            match_no = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid match number. Please provide an integer.")
            return

        # Join all remaining args to allow for team names with spaces
        winner = " ".join(context.args[1:]).strip()
        if not winner:
            await update.message.reply_text("Please provide the winning team name.")
            return

        logger.info(
            f"Scoring Match {match_no}. Declared winner: {winner}. Initiated by {update.effective_user.name if update.effective_user else 'Unknown'}")

        # Access schedule_mapping to validate winner name against teams
        if 'schedule_mapping' not in globals():
            logger.error("Schedule mapping not loaded during scoring.")
            await update.message.reply_text("Error: Schedule data is not available for validation.")
            return

        match_info = schedule_mapping.get(match_no)
        if not match_info:
            await update.message.reply_text(f"Match number {match_no} not found in the schedule. Cannot score.")
            return

        if match_no > 70:
            await update.message.reply_text(f"Match number {match_no} cannot be scored until the teams are decided.")
            return

        # Validate winner name (case-insensitive check against the two teams)
        teams = [team.strip() for team in match_info["Teams"].split(" vs ")]
        if winner.lower() not in [team.lower() for team in teams]:
            await update.message.reply_text(
                f"Invalid winner '{winner}'. For Match {match_no}, expected one of: {teams[0]}, {teams[1]}")
            return
        # Standardize winner name to match the case in the schedule/predictions
        winner = teams[0] if winner.lower() == teams[0].lower() else teams[1]

        try:
            gc = context.bot_data["gc"]
            pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
            df = get_predictions_df(pred_sheet)

            if df.empty:
                await update.message.reply_text(f"No predictions found for Match {match_no}. Nothing to score.")
                return

            # Ensure MatchNo is numeric and Correct exists
            df['MatchNo'] = pd.to_numeric(df['MatchNo'], errors='coerce')
            if 'Correct' not in df.columns:
                df['Correct'] = 0
            
            # Properly convert Correct to numeric values
            df['Correct'] = pd.to_numeric(df['Correct'], errors='coerce').fillna(0).astype(int)

            # Filter for the specific match
            match_mask = df["MatchNo"] == match_no

            if not match_mask.any():
                await update.message.reply_text(
                    f"No predictions found specifically for Match {match_no} in the sheet.")
                return

            # Update 'Correct' column: 1 if Prediction matches winner, 0 otherwise
            # Apply only to rows matching the match_no
            df.loc[match_mask, "Correct"] = df.loc[match_mask, "Prediction"].apply(
                lambda prediction: 1 if str(prediction).strip().lower() == winner.lower() else 0
            )

            # Fill NaN in 'Correct' just in case, ensure int type
            df['Correct'] = df['Correct'].fillna(0).astype(int)

            # Log the update before saving
            logger.info(f"Updating scores for Match {match_no}. Setting 'Correct' column for matching predictions.")
            
            save_predictions_df(pred_sheet, df)
            logger.info(f"Successfully updated scores for Match {match_no}. Winner: {winner}")
            await update.message.reply_text(
                f"Score updated for Match {match_no}. Winner: {winner}. Predictions marked."
            )

        except Exception as e:
            logger.error(f"Error accessing sheet or saving scores for Match {match_no}: {e}")
            await update.message.reply_text(f"An error occurred while updating scores for Match {match_no}.")

    except Exception as e:
        logger.exception(f"Error in score_match command: {e}")

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the leaderboard."""
    if not update.message:
        logger.warning("leaderboard called without message.")
        return
    try:
        gc = context.bot_data["gc"]
        pred_sheet = get_sheet(gc, PREDICTIONS_SHEET_ID)
        df = get_predictions_df(pred_sheet)

        if df.empty:
            await update.message.reply_text("No predictions found yet to generate a leaderboard.")
            return

        # Ensure Correct column exists and is numeric
        if 'Correct' not in df.columns:
            await update.message.reply_text("Leaderboard cannot be generated: 'Correct' column missing.")
            logger.warning("Leaderboard generation failed: Missing 'Correct' column.")
            return
        df["Correct"] = pd.to_numeric(df["Correct"], errors="coerce").fillna(0).astype(int)

        # Ensure Username column exists
        if 'Username' not in df.columns:
            await update.message.reply_text("Leaderboard cannot be generated: 'Username' column missing.")
            logger.warning("Leaderboard generation failed: Missing 'Username' column.")
            return
        df = df.dropna(subset=['Username'])  # Remove rows with no username

        # Calculate leaderboard
        lb = (
            df.groupby("Username")["Correct"]
                .sum()
                .reset_index()
                .sort_values("Correct", ascending=False)
        )

        if lb.empty:
            await update.message.reply_text("No scores recorded yet for the leaderboard.")
            return

        msg = "🏆 *Leaderboard* 🏆\n\n"
        rank = 1
        for index, row in lb.iterrows():
            # Handle potential non-string usernames just in case
            username = str(row['Username'])
            score = int(row['Correct'])
            # Add emoji for top ranks
            rank_emoji = ""
            if rank == 1:
                rank_emoji = "🥇 "
            elif rank == 2:
                rank_emoji = "🥈 "
            elif rank == 3:
                rank_emoji = "🥉 "

            msg += f"{rank_emoji}{rank}. {username}: {score} points\n"
            rank += 1

        await update.message.reply_text(msg, parse_mode="Markdown")

    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API Error generating leaderboard: {e}")
        await update.message.reply_text("Error accessing prediction data for leaderboard.")
    except Exception as e:
        logger.exception(f"Error in leaderboard command: {e}")
        await update.message.reply_text("An error occurred while generating the leaderboard.")

async def error_handler(update: object, context: CallbackContext):
    """Logs errors without sending to group chat."""
    if isinstance(context.error, Conflict) and "webhook is active" in str(context.error):
        logger.error("Webhook conflict detected. The bot is likely configured with a webhook.")
    else:
        logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

async def set_commands(application: Application) -> None:
    """Set bot commands in Telegram."""
    commands = [
        ("start", "Get started with IPL Prediction Bot"),
        ("help", "Show available commands"),
        ("startpoll", "Start a poll for a match"),
        ("leaderboard", "View current leaderboard"),
        ("score", "Score a match (admin only)"),
        ("get_chat_id", "Get current chat ID")
    ]
    
    try:
        await application.bot.set_my_commands(commands)
        logger.info("Bot commands registered with Telegram")
    except Exception as e:
        logger.error(f"Error setting bot commands: {e}")

def schedule_polls(application: Application) -> None:
    """Schedule all polls using the job queue."""
    # Get job queue
    job_queue = application.job_queue
    
    # Schedule polls based on the loaded schedule
    timezone = pytz.timezone("Asia/Kolkata")
    for match_no, match_info in schedule_mapping.items():
        try:
            poll_start_time_str = match_info["PollStartTime"]
            poll_start_date_str = match_info["Date"]
            dt_str = f"{poll_start_date_str} {poll_start_time_str}"
            poll_start_datetime_ist = datetime.strptime(dt_str, "%d %b %Y %I:%M %p")
            poll_start_datetime_utc = poll_start_datetime_ist.replace(tzinfo=timezone).astimezone(pytz.utc)
            
            # Schedule job with context data
            context_data = {"match_no": match_no, "match_info": match_info}
            job_queue.run_once(
                scheduled_poll_callback,
                poll_start_datetime_utc,
                data=context_data,
                name=f"poll_match_{match_no}"
            )
            logger.info(f"Scheduled poll for Match {match_no} at {poll_start_datetime_utc} (UTC)")
        except Exception as e:
            logger.error(f"Error scheduling poll for Match {match_no}: {e}")

def main() -> None:
    """Starts the bot."""
    # Set up signal handler for clean shutdown
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, exiting...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # First, ensure no webhook is active
    if not asyncio.run(ensure_webhook_deleted()):
        logger.error("Cannot start bot with active webhook. Please run webhook deletion script first.")
        return
    
    # Create the Application with explicit request parameters
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(15.0)
        .pool_timeout(15.0)
        .read_timeout(15.0)
        .connection_pool_size(16)
        .build()
    )
    
    # Set bot commands in Telegram
    asyncio.run(set_commands(application))
    
    # Get the Google Sheets client
    try:
        gc = authorize_gspread()
    except Exception as e:
        logger.error(f"Failed to authorize Google Sheets: {e}")
        return  # Exit if Sheets authorization fails

    # Store the gc instance in bot_data
    application.bot_data["gc"] = gc
    application.bot_data["poll_map_sheet_id"] = POLL_MAP_SHEET_ID
    application.bot_data["predictions_sheet_id"] = PREDICTIONS_SHEET_ID

    # Load match schedule
    global schedule_mapping
    try:
        schedule_mapping = load_schedule_mapping(SCHEDULE_CSV)
    except Exception as e:
        logger.error(f"Failed to load schedule: {e}")
        return

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("get_chat_id", get_chat_id))
    application.add_handler(CommandHandler("startpoll", startpoll))
    application.add_handler(CommandHandler("score", score_match))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(PollAnswerHandler(handle_poll_answer))

    # Set up error handler
    application.add_error_handler(error_handler)

    # Schedule all polls
    schedule_polls(application)

    # Run the bot until the user presses Ctrl-C
    logger.info("Starting bot...")
    # Log environment variables at the very start of main()
    log_all_env_vars()
    try:
        # Run polling with appropriate settings
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            close_loop=False
        )
    except Exception as e:
        logger.error(f"Error in bot polling: {e}")

if __name__ == "__main__":
    main()