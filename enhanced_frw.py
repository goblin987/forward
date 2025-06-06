import os
import sqlite3
import threading
import logging
import re
import uuid
from datetime import datetime, timedelta
import sys
import types
import filetype
import asyncio
import time
import signal
import random
import json

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler, ConversationHandler, Filters
)
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from telethon.errors import SessionPasswordNeededError, FloodWaitError, ChatSendMediaForbiddenError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
import pytz

# Constants
CLIENT_TIMEOUT = 30
CHECK_TASKS_INTERVAL = 60

# Fake 'imghdr' module for Python 3.11 compatibility
imghdr_module = types.ModuleType('imghdr')
def what(file, h=None):
    """Determine the file type based on its header."""
    buf = file.read(32) if hasattr(file, 'read') else open(file, 'rb').read(32) if isinstance(file, str) else file[:32]
    kind = filetype.guess(buf)
    return kind.extension if kind else None
imghdr_module.what = what
sys.modules['imghdr'] = imghdr_module

# Data directory setup for persistent storage
DATA_DIR = os.environ.get('DATA_DIR', './data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
DB_PATH = os.path.join(DATA_DIR, 'telegram_bot.db')
SESSION_DIR = os.path.join(DATA_DIR, 'sessions')
if not os.path.exists(SESSION_DIR):
    os.makedirs(SESSION_DIR)

# Configuration from environment variables with validation
def load_env_var(name, required=True, cast=str):
    """Load an environment variable with type casting and validation."""
    value = os.environ.get(name)
    if required and not value:
        raise ValueError(f"Environment variable {name} is not set.")
    return cast(value) if value else None

API_ID = load_env_var('API_ID', cast=int)
API_HASH = load_env_var('API_HASH')
BOT_TOKEN = load_env_var('BOT_TOKEN')
ADMIN_IDS = [int(id_) for id_ in load_env_var('ADMIN_IDS', False, str).split(',') if id_] if load_env_var('ADMIN_IDS', False) else []

# Database setup with persistent storage
db = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = db.cursor()
db_lock = threading.RLock()

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'bot.log')),
        logging.StreamHandler()
    ]
)

# Signal handler for graceful shutdown
def shutdown(signum, frame):
    """Handle shutdown signals to close resources gracefully."""
    logging.info("Shutting down...")
    db.close()
    with userbots_lock:
        for phone, (client, loop, lock, thread) in userbots.items():
            asyncio.run_coroutine_threadsafe(client.disconnect(), loop)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            if thread.is_alive():
                logging.warning(f"Thread for {phone} did not stop gracefully.")
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)

# Bot setup
updater = Updater(BOT_TOKEN)
dp = updater.dispatcher

# Userbots management
userbots = {}
userbots_lock = threading.Lock()

# Enhanced conversation states
(
    WAITING_FOR_CODE, WAITING_FOR_PHONE, WAITING_FOR_API_ID, WAITING_FOR_API_HASH,
    WAITING_FOR_CODE_USERBOT, WAITING_FOR_PASSWORD, WAITING_FOR_SUB_DETAILS,
    WAITING_FOR_GROUP_URLS, WAITING_FOR_MESSAGE_LINK, WAITING_FOR_START_TIME,
    WAITING_FOR_TARGET_GROUP, WAITING_FOR_FOLDER_CHOICE, WAITING_FOR_FOLDER_NAME,
    WAITING_FOR_FOLDER_SELECTION, TASK_SETUP, WAITING_FOR_LANGUAGE,
    WAITING_FOR_EXTEND_CODE, WAITING_FOR_EXTEND_DAYS,
    WAITING_FOR_ADD_USERBOTS_CODE, WAITING_FOR_ADD_USERBOTS_COUNT, SELECT_TARGET_GROUPS,
    WAITING_FOR_USERBOT_SELECTION, WAITING_FOR_GROUP_LINKS, WAITING_FOR_FOLDER_ACTION,
    WAITING_FOR_PRIMARY_MESSAGE_LINK, WAITING_FOR_FALLBACK_MESSAGE_LINK,
    # Enhanced admin states
    ADMIN_TASK_MANAGEMENT, ADMIN_CLIENT_SELECTION, ADMIN_TASK_CREATION,
    ADMIN_TASK_EDITING, WAITING_FOR_TASK_NAME, WAITING_FOR_CLIENT_CODE,
    ADMIN_BULK_OPERATIONS, WAITING_FOR_TEMPLATE_NAME, ADMIN_TEMPLATE_MANAGEMENT
) = range(32)

# Enhanced translations dictionary
translations = {
    'en': {
        'welcome': "Welcome! To activate your account, please send your invitation code now (e.g., a565ae57).",
        'invalid_code': "Invalid or expired code.",
        'client_menu': "Client Menu (Code: {code})\nAssigned Userbots: {count}\nSubscription ends: {end_date}\n",
        'set_language': "Set Language",
        'select_language': "Select your preferred language:",
        'language_set': "Language set to {lang}.",
        'account_activated': "Account activated! Your userbots will join target groups as you add them.",
        'setup_tasks': "Setup Tasks",
        'manage_folders': "Manage Folders",
        'back_to_menu': "Back to Menu",
        'select_target_groups': "Select Target Groups",
        'select_folder': "Select Folder",
        'send_to_all_groups': "Send to All Groups",
        'join_target_groups': "Join Target Groups",
        'logs': "Logs",
        'admin_panel': "Admin Panel",
        'manage_client_tasks': "Manage Client Tasks",
        'bulk_operations': "Bulk Operations",
        'task_templates': "Task Templates",
        'system_overview': "System Overview",
        'create_task_for_client': "Create Task for Client",
        'edit_client_tasks': "Edit Client Tasks",
        'view_all_tasks': "View All Active Tasks",
        'pause_all_tasks': "Pause All Tasks",
        'resume_all_tasks': "Resume All Tasks",
        'delete_completed_tasks': "Delete Completed Tasks",
    },
    'uk': {},
    'pl': {},
    'lt': {},
    'ru': {}
}

def get_text(user_id, key, **kwargs):
    """Retrieve translated text based on user's language preference."""
    with db_lock:
        cursor.execute("SELECT language FROM clients WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        lang = result[0] if result else 'en'
    text = translations.get(lang, translations['en']).get(key, translations['en'].get(key, key))
    return text.format(**kwargs)

# Enhanced database initialization with new tables
try:
    with db_lock:
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS clients (
                invitation_code TEXT PRIMARY KEY,
                user_id INTEGER UNIQUE,
                subscription_end INTEGER NOT NULL,
                dedicated_userbots TEXT,
                folder_name TEXT,
                forwards_count INTEGER DEFAULT 0,
                groups_reached INTEGER DEFAULT 0,
                total_messages_sent INTEGER DEFAULT 0,
                language TEXT DEFAULT 'en',
                created_by INTEGER,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                status TEXT DEFAULT 'active'
            );

            CREATE TABLE IF NOT EXISTS userbots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT UNIQUE NOT NULL,
                session_file TEXT NOT NULL,
                status TEXT CHECK(status IN ('active', 'inactive')) DEFAULT 'active',
                assigned_client TEXT,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                username TEXT,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            );

            CREATE TABLE IF NOT EXISTS target_groups (
                group_id INTEGER,
                group_name TEXT,
                group_link TEXT,
                added_by TEXT,
                folder_id INTEGER,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (group_id, added_by)
            );

            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                UNIQUE(name, created_by)
            );

            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                event TEXT NOT NULL,
                details TEXT,
                client_id TEXT,
                task_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                client_id TEXT NOT NULL,
                userbot_phone TEXT NOT NULL,
                message_link TEXT,
                fallback_message_link TEXT,
                start_time INTEGER,
                end_time INTEGER,
                repetition_interval INTEGER,
                status TEXT CHECK(status IN ('active', 'paused', 'completed', 'failed')) DEFAULT 'active',
                folder_id INTEGER,
                send_to_all_groups INTEGER DEFAULT 0,
                last_run INTEGER,
                total_runs INTEGER DEFAULT 0,
                successful_runs INTEGER DEFAULT 0,
                failed_runs INTEGER DEFAULT 0,
                created_by INTEGER,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now')),
                template_id INTEGER,
                config_json TEXT
            );

            CREATE TABLE IF NOT EXISTS task_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                config_json TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                is_public INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS admin_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                target_id TEXT,
                details TEXT,
                timestamp INTEGER DEFAULT (strftime('%s', 'now'))
            );

            -- Drop old userbot_settings table if exists and migrate data
            INSERT OR IGNORE INTO tasks (client_id, userbot_phone, message_link, fallback_message_link, 
                                       start_time, repetition_interval, status, folder_id, send_to_all_groups, last_run, name)
            SELECT client_id, userbot_phone, message_link, fallback_message_link, 
                   start_time, repetition_interval, status, folder_id, send_to_all_groups, last_run,
                   'Migrated Task - ' || userbot_phone
            FROM userbot_settings WHERE EXISTS (SELECT name FROM sqlite_master WHERE type='table' AND name='userbot_settings');
            
            DROP TABLE IF EXISTS userbot_settings;
        ''')
        db.commit()
except sqlite3.Error as e:
    logging.error(f"Database setup failed: {e}")
    raise

# Time zone setup
lithuania_tz = pytz.timezone('Europe/Vilnius')
utc_tz = pytz.utc

# Utility functions
def is_admin(user_id):
    return user_id in ADMIN_IDS

def notify_admins(bot, message_text):
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, message_text)
        except Exception as e:
            logging.error(f"Failed to notify admin {admin_id}: {e}")

def log_event(event, details, client_id=None, task_id=None):
    timestamp = int(datetime.now(utc_tz).timestamp())
    with db_lock:
        cursor.execute("INSERT INTO logs (timestamp, event, details, client_id, task_id) VALUES (?, ?, ?, ?, ?)", 
                      (timestamp, event, details, client_id, task_id))
        db.commit()
    logging.info(f"{event}: {details}")

def log_admin_action(admin_id, action_type, target_id=None, details=None):
    """Log admin actions for audit trail."""
    with db_lock:
        cursor.execute("INSERT INTO admin_actions (admin_id, action_type, target_id, details) VALUES (?, ?, ?, ?)",
                      (admin_id, action_type, target_id, details))
        db.commit()

def get_current_lithuanian_time():
    return datetime.now(lithuania_tz).strftime('%d/%m/%y %H:%M')

def parse_lithuanian_time(time_str):
    now = datetime.now(lithuania_tz)
    try:
        time_obj = datetime.strptime(time_str, '%H:%M')
        time_obj = lithuania_tz.localize(time_obj.replace(year=now.year, month=now.month, day=now.day))
        if time_obj < now:
            time_obj += timedelta(days=1)
        return int(time_obj.astimezone(utc_tz).timestamp())
    except ValueError:
        return None

def format_lithuanian_time(timestamp):
    return datetime.fromtimestamp(timestamp, utc_tz).astimezone(lithuania_tz).strftime('%H:%M') if timestamp else "Not set"

def format_interval(minutes):
    if minutes is None:
        return "Not set"
    if minutes % 60 == 0:
        hours = minutes // 60
        return f"Every {hours} hour{'s' if hours > 1 else ''}"
    return f"Every {minutes} minute{'s' if minutes > 1 else ''}"

# Enhanced Admin Panel with Task Management
def enhanced_admin_panel(update: Update, context):
    """Display the enhanced admin panel for authorized users."""
    try:
        if not is_admin(update.effective_user.id):
            update.message.reply_text("Unauthorized")
            return ConversationHandler.END
        
        # Get system overview stats
        with db_lock:
            cursor.execute("SELECT COUNT(*) FROM clients WHERE status = 'active'")
            active_clients = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM userbots WHERE status = 'active'")
            active_userbots = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'active'")
            active_tasks = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'paused'")
            paused_tasks = cursor.fetchone()[0]
        
        stats_text = (f"📊 System Overview:\n"
                     f"Active Clients: {active_clients}\n"
                     f"Active Userbots: {active_userbots}\n"
                     f"Active Tasks: {active_tasks}\n"
                     f"Paused Tasks: {paused_tasks}\n\n")
        
        keyboard = [
            [InlineKeyboardButton("🎯 Manage Client Tasks", callback_data="admin_manage_client_tasks")],
            [InlineKeyboardButton("📋 View All Tasks", callback_data="admin_view_all_tasks")],
            [InlineKeyboardButton("⚡ Bulk Operations", callback_data="admin_bulk_operations")],
            [InlineKeyboardButton("📝 Task Templates", callback_data="admin_task_templates")],
            [InlineKeyboardButton("👥 Add Userbot", callback_data="admin_add_userbot")],
            [InlineKeyboardButton("🗑️ Remove Userbot", callback_data="admin_remove_userbot")],
            [InlineKeyboardButton("🎫 Generate Invitation", callback_data="admin_generate_invite")],
            [InlineKeyboardButton("📈 View Subscriptions", callback_data="admin_view_subs")],
            [InlineKeyboardButton("📋 View Logs", callback_data="admin_view_logs")],
            [InlineKeyboardButton("⏰ Extend Subscription", callback_data="admin_extend_sub")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(stats_text + "Enhanced Admin Panel:", reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        log_event("Admin Panel Error", f"User: {update.effective_user.id}, Error: {e}")
        update.message.reply_text("An error occurred in the admin panel.")
        return ConversationHandler.END

def admin_manage_client_tasks(update: Update, context):
    """Manage tasks for specific clients."""
    try:
        query = update.callback_query
        query.answer()
        
        with db_lock:
            cursor.execute("""
                SELECT c.invitation_code, c.user_id, COUNT(t.id) as task_count
                FROM clients c 
                LEFT JOIN tasks t ON c.invitation_code = t.client_id 
                WHERE c.status = 'active'
                GROUP BY c.invitation_code, c.user_id
                ORDER BY task_count DESC
            """)
            clients = cursor.fetchall()
        
        if not clients:
            query.edit_message_text("No active clients found.")
            return ConversationHandler.END
        
        message = "Select a client to manage their tasks:\n\n"
        keyboard = []
        
        for code, user_id, task_count in clients:
            message += f"🔑 Code: {code} | User: {user_id} | Tasks: {task_count}\n"
            keyboard.append([InlineKeyboardButton(f"{code} ({task_count} tasks)", 
                                                callback_data=f"admin_client_{code}")])
        
        keyboard.append([InlineKeyboardButton("➕ Create New Task", callback_data="admin_create_new_task")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ADMIN_CLIENT_SELECTION
    except Exception as e:
        log_event("Admin Client Tasks Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_view_all_tasks(update: Update, context):
    """View all active tasks across all clients."""
    try:
        query = update.callback_query
        query.answer()
        
        with db_lock:
            cursor.execute("""
                SELECT t.id, t.name, t.client_id, t.userbot_phone, t.status, 
                       t.last_run, t.total_runs, t.successful_runs, t.failed_runs,
                       c.user_id
                FROM tasks t
                JOIN clients c ON t.client_id = c.invitation_code
                ORDER BY t.status, t.created_at DESC
                LIMIT 20
            """)
            tasks = cursor.fetchall()
        
        if not tasks:
            query.edit_message_text("No tasks found.")
            return ConversationHandler.END
        
        message = "📋 All Tasks (Last 20):\n\n"
        keyboard = []
        
        for task_id, name, client_id, phone, status, last_run, total_runs, success, failed, user_id in tasks:
            status_emoji = {"active": "🟢", "paused": "⏸️", "completed": "✅", "failed": "❌"}.get(status, "⚪")
            last_run_str = datetime.fromtimestamp(last_run).strftime('%m/%d %H:%M') if last_run else "Never"
            
            message += f"{status_emoji} {name}\n"
            message += f"   Client: {client_id} | Phone: {phone}\n"
            message += f"   Runs: {success}/{total_runs} | Last: {last_run_str}\n\n"
            
            keyboard.append([InlineKeyboardButton(f"Edit: {name[:20]}...", 
                                                callback_data=f"admin_edit_task_{task_id}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        log_event("Admin View Tasks Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_bulk_operations(update: Update, context):
    """Perform bulk operations on tasks."""
    try:
        query = update.callback_query
        query.answer()
        
        keyboard = [
            [InlineKeyboardButton("⏸️ Pause All Active Tasks", callback_data="bulk_pause_all")],
            [InlineKeyboardButton("▶️ Resume All Paused Tasks", callback_data="bulk_resume_all")],
            [InlineKeyboardButton("🗑️ Delete Completed Tasks", callback_data="bulk_delete_completed")],
            [InlineKeyboardButton("📊 Generate Task Report", callback_data="bulk_generate_report")],
            [InlineKeyboardButton("🔄 Restart Failed Tasks", callback_data="bulk_restart_failed")],
            [InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")]
        ]
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text("🔧 Bulk Operations:", reply_markup=markup)
        return ADMIN_BULK_OPERATIONS
    except Exception as e:
        log_event("Admin Bulk Operations Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_task_templates(update: Update, context):
    """Manage task templates."""
    try:
        query = update.callback_query
        query.answer()
        
        with db_lock:
            cursor.execute("SELECT id, name, description, created_at FROM task_templates ORDER BY created_at DESC")
            templates = cursor.fetchall()
        
        message = "📝 Task Templates:\n\n"
        keyboard = []
        
        if templates:
            for template_id, name, description, created_at in templates:
                created_date = datetime.fromtimestamp(created_at).strftime('%m/%d/%y')
                message += f"📄 {name}\n   {description or 'No description'}\n   Created: {created_date}\n\n"
                keyboard.append([InlineKeyboardButton(f"Use: {name}", callback_data=f"use_template_{template_id}")])
        else:
            message += "No templates found.\n\n"
        
        keyboard.append([InlineKeyboardButton("➕ Create Template", callback_data="create_template")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ADMIN_TEMPLATE_MANAGEMENT
    except Exception as e:
        log_event("Admin Templates Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Async helper functions (keeping the existing ones and adding new ones)
async def async_connect_and_check(client, phone):
    await client.connect()
    return "already_authorized" if await client.is_user_authorized() else await client.send_code_request(phone)

async def async_sign_in(client, phone, code):
    await client.sign_in(phone, code)

async def async_sign_in_with_password(client, password):
    await client.sign_in(password=password)

async def async_disconnect(client):
    await client.disconnect()

async def create_client(session_file, api_id, api_hash, loop):
    client = TelegramClient(session_file, api_id, api_hash, timeout=CLIENT_TIMEOUT, loop=loop)
    return client

def parse_telegram_url(url):
    if url.startswith("https://t.me/"):
        path = url[len("https://t.me/"):].strip()
        if path.startswith("+") or path.startswith("joinchat/"):
            return "private", path[1:] if path.startswith("+") else path[len("joinchat/"):]
        elif path.startswith("addlist/"):
            return "addlist", path[len("addlist/"):]
        return "public", path.split('/')[0]
    raise ValueError("Invalid Telegram URL")

async def get_message_from_link(client, link):
    logging.info(f"Parsing message link: {link}")
    parts = link.split('/')
    if link.startswith("https://t.me/c/") and len(parts) == 6 and parts[4].isdigit() and parts[5].isdigit():
        group_id = -1000000000000 - int(parts[4])
        message_id = int(parts[5])
        return PeerChannel(group_id), message_id
    elif link.startswith("https://t.me/") and len(parts) == 5 and parts[4].isdigit():
        try:
            chat = await client.get_entity(parts[3])
            return chat, int(parts[4])
        except Exception as e:
            logging.error(f"Failed to get entity for {parts[3]}: {e}")
            raise ValueError(f"Failed to get entity: {e}")
    logging.error(f"Invalid message link: {link}")
    raise ValueError("Invalid message link")

def get_userbot_client(phone_number):
    """Retrieve or create a TelegramClient instance for a userbot with its own event loop."""
    try:
        with db_lock:
            cursor.execute("SELECT api_id, api_hash, session_file FROM userbots WHERE phone_number = ?", (phone_number,))
            result = cursor.fetchone()
        if result:
            api_id, api_hash, session_file = result
            with userbots_lock:
                if phone_number not in userbots:
                    # Create a new event loop for this client
                    loop = asyncio.new_event_loop()
                    # Start the loop in a separate thread
                    def run_loop():
                        asyncio.set_event_loop(loop)
                        loop.run_forever()
                    thread = threading.Thread(target=run_loop, daemon=True)
                    thread.start()
                    # Create the client with this loop
                    future = asyncio.run_coroutine_threadsafe(
                        create_client(os.path.join(SESSION_DIR, f"{phone_number}.session"), api_id, api_hash, loop), loop
                    )
                    client = future.result(timeout=10)
                    lock = asyncio.Lock()
                    userbots[phone_number] = (client, loop, lock, thread)
                return userbots[phone_number]
        return None, None, None, None
    except Exception as e:
        log_event("Get Userbot Client Error", f"Phone: {phone_number}, Error: {e}")
        return None, None, None, None

# Enhanced task execution system
async def execute_enhanced_task(task_id):
    """Execute a task with enhanced error handling and logging."""
    try:
        with db_lock:
            cursor.execute("""
                SELECT t.*, c.user_id, c.dedicated_userbots 
                FROM tasks t 
                JOIN clients c ON t.client_id = c.invitation_code 
                WHERE t.id = ? AND t.status = 'active'
            """, (task_id,))
            task_data = cursor.fetchone()
        
        if not task_data:
            return False, "Task not found or not active"
        
        # Extract task details
        (task_id, name, client_id, userbot_phone, message_link, fallback_message_link,
         start_time, end_time, repetition_interval, status, folder_id, send_to_all_groups,
         last_run, total_runs, successful_runs, failed_runs, created_by, created_at,
         updated_at, template_id, config_json, user_id, dedicated_userbots) = task_data
        
        # Check if it's time to run
        current_time = int(datetime.now(utc_tz).timestamp())
        if start_time and current_time < start_time:
            return False, "Not yet time to start"
        
        if end_time and current_time > end_time:
            # Mark task as completed
            with db_lock:
                cursor.execute("UPDATE tasks SET status = 'completed' WHERE id = ?", (task_id,))
                db.commit()
            return False, "Task has ended"
        
        if last_run and repetition_interval:
            next_run_time = last_run + (repetition_interval * 60)
            if current_time < next_run_time:
                return False, "Not yet time for next repetition"
        
        # Get userbot client
        client, loop, lock, thread = get_userbot_client(userbot_phone)
        if not client:
            with db_lock:
                cursor.execute("UPDATE tasks SET failed_runs = failed_runs + 1 WHERE id = ?", (task_id,))
                db.commit()
            log_event("Task Failed", f"Task {name}: Userbot client not available", client_id, task_id)
            return False, "Userbot client not available"
        
        # Execute the actual forwarding
        success = await execute_forwarding_task(client, lock, task_id, message_link, 
                                              fallback_message_link, folder_id, 
                                              send_to_all_groups, client_id)
        
        # Update task statistics
        with db_lock:
            if success:
                cursor.execute("""
                    UPDATE tasks 
                    SET last_run = ?, total_runs = total_runs + 1, successful_runs = successful_runs + 1,
                        updated_at = ?
                    WHERE id = ?
                """, (current_time, current_time, task_id))
                cursor.execute("""
                    UPDATE clients 
                    SET total_messages_sent = total_messages_sent + 1 
                    WHERE invitation_code = ?
                """, (client_id,))
            else:
                cursor.execute("""
                    UPDATE tasks 
                    SET last_run = ?, total_runs = total_runs + 1, failed_runs = failed_runs + 1,
                        updated_at = ?
                    WHERE id = ?
                """, (current_time, current_time, task_id))
            db.commit()
        
        status_text = "Success" if success else "Failed"
        log_event(f"Task {status_text}", f"Task {name} executed", client_id, task_id)
        return success, f"Task executed: {status_text}"
        
    except Exception as e:
        with db_lock:
            cursor.execute("UPDATE tasks SET failed_runs = failed_runs + 1 WHERE id = ?", (task_id,))
            db.commit()
        log_event("Task Execution Error", f"Task ID {task_id}: {e}")
        return False, f"Execution error: {e}"

async def execute_forwarding_task(client, lock, task_id, message_link, fallback_message_link, 
                                folder_id, send_to_all_groups, client_id):
    """Execute the actual message forwarding."""
    try:
        async with lock:
            await client.start()
            
            # Get target groups
            if send_to_all_groups:
                with db_lock:
                    cursor.execute("SELECT group_id FROM target_groups WHERE added_by = ?", (client_id,))
                    target_groups = [row[0] for row in cursor.fetchall()]
            else:
                with db_lock:
                    cursor.execute("SELECT group_id FROM target_groups WHERE folder_id = ?", (folder_id,))
                    target_groups = [row[0] for row in cursor.fetchall()]
            
            if not target_groups:
                return False
            
            # Try primary message link first
            success_count = 0
            try:
                if message_link:
                    chat, message_id = await get_message_from_link(client, message_link)
                    message_obj = await client.get_messages(chat, ids=message_id)
                    
                    for group_id in target_groups:
                        try:
                            await client.forward_messages(PeerChannel(group_id), message_obj)
                            success_count += 1
                            await asyncio.sleep(random.uniform(1, 3))  # Random delay
                        except Exception as e:
                            logging.warning(f"Failed to forward to group {group_id}: {e}")
                            continue
            except Exception as e:
                logging.warning(f"Primary message failed: {e}")
                # Try fallback message
                if fallback_message_link:
                    try:
                        chat, message_id = await get_message_from_link(client, fallback_message_link)
                        message_obj = await client.get_messages(chat, ids=message_id)
                        
                        for group_id in target_groups:
                            try:
                                await client.forward_messages(PeerChannel(group_id), message_obj)
                                success_count += 1
                                await asyncio.sleep(random.uniform(1, 3))
                            except Exception as e:
                                logging.warning(f"Failed to forward fallback to group {group_id}: {e}")
                                continue
                    except Exception as e:
                        logging.error(f"Fallback message also failed: {e}")
            
            return success_count > 0
            
    except Exception as e:
        logging.error(f"Forwarding task error: {e}")
        return False
    finally:
        try:
            await client.disconnect()
        except:
            pass

# Enhanced task checking system
async def check_enhanced_tasks():
    """Enhanced task checking with better performance and error handling."""
    while True:
        try:
            with db_lock:
                cursor.execute("""
                    SELECT id FROM tasks 
                    WHERE status = 'active' 
                    ORDER BY COALESCE(last_run, 0) ASC
                """)
                active_tasks = [row[0] for row in cursor.fetchall()]
            
            # Execute tasks concurrently but with limits
            semaphore = asyncio.Semaphore(5)  # Limit concurrent executions
            
            async def run_task_with_semaphore(task_id):
                async with semaphore:
                    return await execute_enhanced_task(task_id)
            
            if active_tasks:
                tasks = [run_task_with_semaphore(task_id) for task_id in active_tasks]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                successful_executions = sum(1 for result in results if isinstance(result, tuple) and result[0])
                if successful_executions > 0:
                    log_event("Task Batch Completed", f"Executed {successful_executions}/{len(active_tasks)} tasks")
            
        except Exception as e:
            log_event("Task Check Error", f"Error in task checking: {e}")
        
        await asyncio.sleep(CHECK_TASKS_INTERVAL)

def run_async_loop():
    """Run the async event loop for task checking."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(check_enhanced_tasks())

# Start the async task checking thread
task_thread = threading.Thread(target=run_async_loop, daemon=True)
task_thread.start()

def start(update: Update, context):
    """Handle the /start command to activate the account or show client menu."""
    try:
        user_id = update.effective_user.id
        logging.info(f"Start command received from user {user_id}")
        
        # Check if user is admin
        if is_admin(user_id):
            return enhanced_admin_panel(update, context)
        
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        if result and result[0]:
            logging.info(f"User {user_id} has userbots, redirecting to client menu")
            return client_menu(update, context)
        else:
            if 'prompted_for_code' not in context.user_data:
                context.user_data['prompted_for_code'] = True
                update.message.reply_text(get_text(user_id, 'welcome'))
                return WAITING_FOR_CODE
            else:
                update.message.reply_text(get_text(user_id, 'invalid_code'))
                return ConversationHandler.END
    except Exception as e:
        log_event("Start Command Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again later.")
        return ConversationHandler.END

def client_menu(update: Update, context):
    """Show the client menu with userbot and subscription details."""
    try:
        user_id = update.effective_user.id
        with db_lock:
            cursor.execute("SELECT invitation_code, dedicated_userbots, subscription_end FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        if not result:
            update.message.reply_text(get_text(user_id, 'invalid_code'))
            return ConversationHandler.END
        code, userbots_str, sub_end = result
        end_date = datetime.fromtimestamp(sub_end).strftime('%Y-%m-%d')
        userbot_phones = userbots_str.split(",") if userbots_str else []
        message = get_text(user_id, 'client_menu', code=code, count=len(userbot_phones), end_date=end_date)
        for i, phone in enumerate(userbot_phones, 1):
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone,))
                result = cursor.fetchone()
                username = result[0] if result and result[0] else None
            display_name = f"@{username}" if username else f"{phone} (no username set)"
            message += f"{i}. {display_name}\n"
        keyboard = [
            [InlineKeyboardButton(get_text(user_id, 'setup_tasks'), callback_data="client_setup_tasks")],
            [InlineKeyboardButton(get_text(user_id, 'manage_folders'), callback_data="client_manage_folders")],
            [InlineKeyboardButton(get_text(user_id, 'join_target_groups'), callback_data="client_join_target_groups")],
            [InlineKeyboardButton("Already Joined Groups", callback_data="client_joined_groups")],
            [InlineKeyboardButton(get_text(user_id, 'logs'), callback_data="client_view_logs")],
            [InlineKeyboardButton(get_text(user_id, 'set_language'), callback_data="client_set_language")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(message, reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        log_event("Client Menu Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again or contact support.")
        return ConversationHandler.END

# Add placeholder handlers for the rest of the functionality
# ... (rest of the handlers would continue here)

if __name__ == "__main__":
    # Set up conversation handlers
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            # Add all the conversation states here
            # This is a simplified version - you would add all the handlers
        },
        fallbacks=[CommandHandler('start', start)]
    )
    
    dp.add_handler(conv_handler)
    
    # Start the bot
    updater.start_polling()
    logging.info("Enhanced Auto Forwarding Bot started!")
    updater.idle() 