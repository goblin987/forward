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
    ADMIN_BULK_OPERATIONS, WAITING_FOR_TEMPLATE_NAME, ADMIN_TEMPLATE_MANAGEMENT,
    WAITING_FOR_INVITE_DURATION,
    # Enhanced client states
    WAITING_FOR_TASK_NAME_CLIENT, WAITING_FOR_REPETITION_INTERVAL, WAITING_FOR_START_TIME_CLIENT,
    WAITING_FOR_END_TIME_CLIENT, WAITING_FOR_GROUP_LINKS_FOLDER
) = range(41)

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
    'uk': {
        'welcome': "Ласкаво просимо! Для активації облікового запису надішліть свій код запрошення (наприклад, a565ae57).",
        'invalid_code': "Недійсний або прострочений код.",
        'client_menu': "Меню клієнта (Код: {code})\nПризначені юзерботи: {count}\nПідписка закінчується: {end_date}\n",
        'set_language': "Встановити мову",
        'select_language': "Виберіть бажану мову:",
        'language_set': "Мову встановлено на {lang}.",
        'account_activated': "Обліковий запис активовано! Ваші юзерботи приєднаються до цільових груп.",
        'setup_tasks': "Налаштування завдань",
        'manage_folders': "Керування папками",
        'back_to_menu': "Назад до меню",
        'select_target_groups': "Вибрати цільові групи",
        'select_folder': "Вибрати папку",
        'send_to_all_groups': "Надіслати в усі групи",
        'join_target_groups': "Приєднатися до груп",
        'logs': "Журнали",
    },
    'pl': {
        'welcome': "Witamy! Aby aktywować konto, wyślij swój kod zaproszenia (np. a565ae57).",
        'invalid_code': "Nieprawidłowy lub wygasły kod.",
        'client_menu': "Menu klienta (Kod: {code})\nPrzypisane userboty: {count}\nPrenumerata baigiasi: {end_date}\n",
        'set_language': "Ustaw język",
        'select_language': "Wybierz preferowany język:",
        'language_set': "Język ustawiony na {lang}.",
        'account_activated': "Konto aktywowane! Twoje userboty dołączą do grup docelowych.",
        'setup_tasks': "Konfiguracja zadań",
        'manage_folders': "Zarządzanie folderami",
        'back_to_menu': "Powrót do menu",
        'select_target_groups': "Wybierz grupy docelowe",
        'select_folder': "Wybierz folder",
        'send_to_all_groups': "Wyślij do wszystkich grup",
        'join_target_groups': "Dołącz do grup",
        'logs': "Dzienniki",
    },
    'lt': {
        'welcome': "Sveiki! Norėdami aktyvuoti paskyrą, atsiųskite savo pakvietimo kodą (pvz., a565ae57).",
        'invalid_code': "Neteisingas arba pasibaigęs kodas.",
        'client_menu': "Kliento meniu (Kodas: {code})\nPriskirti vartotojų botai: {count}\nPrenumerata baigiasi: {end_date}\n",
        'set_language': "Nustatyti kalbą",
        'select_language': "Pasirinkite norimą kalbą:",
        'language_set': "Kalba nustatyta į {lang}.",
        'account_activated': "Paskyra aktyvuota! Jūsų vartotojų botai prisijungs prie tikslinių grupių.",
        'setup_tasks': "Užduočių nustatymas",
        'manage_folders': "Aplankų valdymas",
        'back_to_menu': "Atgal į meniu",
        'select_target_groups': "Pasirinkti tikslines grupes",
        'select_folder': "Pasirinkti aplanką",
        'send_to_all_groups': "Siųsti į visas grupes",
        'join_target_groups': "Prisijungti prie grupių",
        'logs': "Žurnalai",
    },
    'ru': {
        'welcome': "Добро пожаловать! Для активации аккаунта отправьте ваш код приглашения (например, a565ae57).",
        'invalid_code': "Недействительный или истёкший код.",
        'client_menu': "Меню клиента (Код: {code})\nНазначенные юзерботы: {count}\nПодписка заканчивается: {end_date}\n",
        'set_language': "Установить язык",
        'select_language': "Выберите предпочитаемый язык:",
        'language_set': "Язык установлен на {lang}.",
        'account_activated': "Аккаунт активирован! Ваши юзерботы присоединятся к целевым группам.",
        'setup_tasks': "Настройка задач",
        'manage_folders': "Управление папками",
        'back_to_menu': "Назад в меню",
        'select_target_groups': "Выбрать целевые группы",
        'select_folder': "Выбрать папку",
        'send_to_all_groups': "Отправить во все группы",
        'join_target_groups': "Присоединиться к группам",
        'logs': "Журналы",
    }
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
        ''')
        
        # Check if old table exists separately
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='userbot_settings'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            # Only migrate if table exists
            cursor.execute('''
                INSERT OR IGNORE INTO tasks (client_id, userbot_phone, message_link, fallback_message_link, 
                                           start_time, repetition_interval, status, folder_id, send_to_all_groups, last_run, name)
                SELECT client_id, userbot_phone, message_link, fallback_message_link, 
                       start_time, repetition_interval, status, folder_id, send_to_all_groups, last_run,
                       'Migrated Task - ' || userbot_phone
                FROM userbot_settings
            ''')
            
            # Drop the old table after migration
            cursor.execute("DROP TABLE userbot_settings")
        
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
        
        # Check if user already has userbots assigned
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if result and result[0]:
            logging.info(f"User {user_id} has userbots, redirecting to client menu")
            return client_menu(update, context)
        else:
            # User needs to enter invitation code
            update.message.reply_text(get_text(user_id, 'welcome'))
            return WAITING_FOR_CODE
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

def handle_callback_query(update: Update, context):
    """Handle all callback queries that don't require conversation state."""
    query = update.callback_query
    query.answer()
    
    try:
        # Admin callbacks
        if query.data == "admin_manage_client_tasks":
            return admin_manage_client_tasks(update, context)
        elif query.data == "admin_view_all_tasks":
            return admin_view_all_tasks(update, context)
        elif query.data == "admin_bulk_operations":
            return admin_bulk_operations(update, context)
        elif query.data == "admin_task_templates":
            return admin_task_templates(update, context)
        elif query.data == "admin_panel":
            return enhanced_admin_panel(update, context)
        elif query.data == "admin_generate_invite":
            return start_invite_generation(update, context)
        elif query.data == "admin_add_userbot":
            return admin_add_userbot(update, context)
        elif query.data == "admin_remove_userbot":
            return admin_remove_userbot(update, context)
        elif query.data == "admin_view_subs":
            return admin_view_subs(update, context)
        elif query.data == "admin_view_logs":
            return admin_view_logs(update, context)
        elif query.data == "admin_extend_sub":
            return admin_extend_sub(update, context)
        
        # Client callbacks
        elif query.data == "client_setup_tasks":
            return client_setup_tasks(update, context)
        elif query.data == "client_create_task":
            return client_create_task(update, context)
        elif query.data == "client_manage_folders":
            return client_manage_folders(update, context)
        elif query.data == "client_join_target_groups":
            return client_join_target_groups(update, context)
        elif query.data == "client_set_language":
            return client_set_language(update, context)
        elif query.data == "client_view_logs":
            return client_view_logs(update, context)
        elif query.data == "back_to_client_menu":
            return client_menu(update, context)
        
        # Folder management callbacks
        elif query.data == "create_new_folder":
            return handle_folder_creation(update, context)
        elif query.data.startswith("edit_folder_"):
            folder_id = int(query.data.split("_")[2])
            return handle_edit_folder(update, context, folder_id)
        elif query.data.startswith("add_groups_"):
            return handle_add_groups_to_folder(update, context)
        
        # Task creation callbacks
        elif query.data.startswith("select_userbot_"):
            return handle_userbot_selection(update, context)
        elif query.data == "set_message_link":
            return handle_set_message_link(update, context)
        elif query.data == "set_schedule":
            return handle_set_schedule(update, context)
        elif query.data == "choose_targets":
            return handle_choose_targets(update, context)
        elif query.data == "create_task_final":
            return handle_create_task_final(update, context)
        elif query.data == "cancel_task_creation":
            return handle_cancel_task_creation(update, context)
        
        # Language callbacks
        elif query.data.startswith("set_lang_"):
            return handle_language_selection(update, context)
        
        # Group joining callbacks  
        elif query.data.startswith("join_folder_"):
            return handle_join_folder_groups(update, context)
        elif query.data == "join_all_groups":
            return handle_join_all_groups(update, context)
        
        else:
            query.edit_message_text("Feature not implemented yet.")
            return ConversationHandler.END
    except Exception as e:
        logging.error(f"Callback query error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Supporting functions for client features

def handle_userbot_selection(update: Update, context):
    """Handle userbot selection for task creation."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Extract phone from callback data
        phone = query.data.split("_", 2)[2]
        context.user_data['selected_userbot'] = phone
        
        query.edit_message_text(
            f"📝 **Create Task**\n\n"
            f"Selected userbot: {phone}\n\n"
            f"Enter a name for this task:"
        )
        return WAITING_FOR_TASK_NAME_CLIENT
        
    except Exception as e:
        log_event("Handle Userbot Selection Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def process_task_name(update: Update, context):
    """Process task name input."""
    try:
        user_id = update.effective_user.id
        task_name = update.message.text.strip()
        
        if len(task_name) < 3 or len(task_name) > 50:
            update.message.reply_text("Task name must be between 3 and 50 characters. Please try again:")
            return WAITING_FOR_TASK_NAME_CLIENT
        
        context.user_data['task_name'] = task_name
        
        keyboard = [
            [InlineKeyboardButton("📨 Set Message Link", callback_data="set_message_link")],
            [InlineKeyboardButton("⏰ Set Schedule", callback_data="set_schedule")],
            [InlineKeyboardButton("🎯 Choose Target Groups", callback_data="choose_targets")],
            [InlineKeyboardButton("✅ Create Task", callback_data="create_task_final")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task_creation")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(
            f"📝 **Task Configuration**\n\n"
            f"Task Name: {task_name}\n"
            f"Userbot: {context.user_data['selected_userbot']}\n\n"
            f"Configure your task:",
            reply_markup=markup
        )
        return TASK_SETUP
        
    except Exception as e:
        log_event("Process Task Name Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again.")
        return ConversationHandler.END

def handle_folder_creation(update: Update, context):
    """Handle new folder creation."""
    try:
        query = update.callback_query
        query.answer()
        
        query.edit_message_text(
            "📁 **Create New Folder**\n\n"
            "Enter a name for your new folder:"
        )
        return WAITING_FOR_FOLDER_NAME
        
    except Exception as e:
        log_event("Handle Folder Creation Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def process_folder_name(update: Update, context):
    """Process folder name input."""
    try:
        user_id = update.effective_user.id
        folder_name = update.message.text.strip()
        
        if len(folder_name) < 2 or len(folder_name) > 30:
            update.message.reply_text("Folder name must be between 2 and 30 characters. Please try again:")
            return WAITING_FOR_FOLDER_NAME
        
        # Check if folder already exists
        with db_lock:
            cursor.execute("SELECT id FROM folders WHERE name = ? AND created_by = ?", (folder_name, str(user_id)))
            if cursor.fetchone():
                update.message.reply_text("A folder with this name already exists. Please choose a different name:")
                return WAITING_FOR_FOLDER_NAME
        
        # Create folder
        with db_lock:
            cursor.execute("INSERT INTO folders (name, created_by) VALUES (?, ?)", (folder_name, str(user_id)))
            folder_id = cursor.lastrowid
            db.commit()
        
        log_event("Folder Created", f"User: {user_id}, Folder: {folder_name}")
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Groups Now", callback_data=f"add_groups_{folder_id}")],
            [InlineKeyboardButton("⬅️ Back to Folders", callback_data="client_manage_folders")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(
            f"✅ **Folder Created!**\n\n"
            f"📁 {folder_name}\n\n"
            f"Would you like to add groups to this folder now?",
            reply_markup=markup
        )
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Process Folder Name Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again.")
        return ConversationHandler.END

def handle_add_groups_to_folder(update: Update, context):
    """Handle adding groups to a folder."""
    try:
        query = update.callback_query
        query.answer()
        
        # Extract folder_id from callback data
        folder_id = int(query.data.split("_")[2])
        context.user_data['target_folder_id'] = folder_id
        
        query.edit_message_text(
            "🔗 **Add Groups to Folder**\n\n"
            "Send group links (one per line) or group usernames (@groupname):\n\n"
            "Examples:\n"
            "• https://t.me/your_group\n"
            "• https://t.me/joinchat/ABC123\n"
            "• @public_group"
        )
        return WAITING_FOR_GROUP_LINKS_FOLDER
        
    except Exception as e:
        log_event("Handle Add Groups Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_language_selection(update: Update, context):
    """Handle language selection."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Extract language from callback data
        lang_code = query.data.split("_")[2]
        
        # Language names
        languages = {
            'en': 'English',
            'uk': 'Українська', 
            'pl': 'Polski',
            'lt': 'Lietuvių',
            'ru': 'Русский'
        }
        
        # Update language in database
        with db_lock:
            cursor.execute("UPDATE clients SET language = ? WHERE user_id = ?", (lang_code, user_id))
            db.commit()
        
        log_event("Language Changed", f"User: {user_id}, Language: {lang_code}")
        
        query.edit_message_text(
            f"✅ **Language Updated**\n\n"
            f"Your language has been set to {languages.get(lang_code, lang_code)}."
        )
        
        # Return to client menu after 2 seconds
        import time
        time.sleep(2)
        return client_menu(update, context)
        
    except Exception as e:
        log_event("Handle Language Selection Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

async def join_groups_from_folder(folder_id, userbots, user_id):
    """Join groups from a specific folder with all userbots."""
    try:
        # Get groups from folder
        with db_lock:
            cursor.execute("""
                SELECT group_id, group_name, group_link 
                FROM target_groups WHERE folder_id = ?
            """, (folder_id,))
            groups = cursor.fetchall()
        
        if not groups:
            return 0, 0, ["No groups found in folder"]
        
        total_joins = 0
        total_groups = len(groups)
        errors = []
        
        for phone in userbots:
            phone = phone.strip()
            client, loop, lock, thread = get_userbot_client(phone)
            
            if not client:
                errors.append(f"Userbot {phone} not available")
                continue
            
            try:
                async with lock:
                    await client.start()
                    
                    for group_id, group_name, group_link in groups:
                        try:
                            if 'joinchat' in group_link:
                                hash_part = group_link.split('/joinchat/')[1]
                                await client(ImportChatInviteRequest(hash_part))
                            else:
                                await client(JoinChannelRequest(PeerChannel(group_id)))
                            
                            total_joins += 1
                            await asyncio.sleep(random.uniform(2, 5))  # Random delay
                            
                        except Exception as e:
                            errors.append(f"Failed to join {group_name} with {phone}: {str(e)}")
                    
                    await client.disconnect()
                    
            except Exception as e:
                errors.append(f"Error with userbot {phone}: {str(e)}")
        
        return total_joins, total_groups * len(userbots), errors
        
    except Exception as e:
        return 0, 0, [f"Join groups error: {str(e)}"]

# Additional missing client functions

def handle_edit_folder(update: Update, context, folder_id):
    """Handle editing a folder."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get folder details and groups
        with db_lock:
            cursor.execute("SELECT name FROM folders WHERE id = ? AND created_by = ?", (folder_id, str(user_id)))
            folder_result = cursor.fetchone()
            
            if not folder_result:
                query.edit_message_text("Folder not found.")
                return ConversationHandler.END
            
            folder_name = folder_result[0]
            
            cursor.execute("""
                SELECT group_name, group_link FROM target_groups 
                WHERE folder_id = ? ORDER BY created_at DESC
            """, (folder_id,))
            groups = cursor.fetchall()
        
        message = f"📁 **Edit Folder: {folder_name}**\n\n"
        if groups:
            message += f"📋 Groups ({len(groups)}):\n"
            for i, (group_name, group_link) in enumerate(groups[:5], 1):
                message += f"{i}. {group_name}\n"
            if len(groups) > 5:
                message += f"... and {len(groups) - 5} more\n"
        else:
            message += "No groups in this folder.\n"
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Groups", callback_data=f"add_groups_{folder_id}")],
            [InlineKeyboardButton("🗑️ Delete Folder", callback_data=f"delete_folder_{folder_id}")],
            [InlineKeyboardButton("⬅️ Back to Folders", callback_data="client_manage_folders")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Handle Edit Folder Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_set_message_link(update: Update, context):
    """Handle setting message link for task."""
    try:
        query = update.callback_query
        query.answer()
        
        query.edit_message_text(
            "📨 **Set Message Link**\n\n"
            "Send the Telegram message link that you want to forward:\n\n"
            "Example: https://t.me/channel/123"
        )
        return WAITING_FOR_PRIMARY_MESSAGE_LINK
        
    except Exception as e:
        log_event("Handle Set Message Link Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_set_schedule(update: Update, context):
    """Handle setting schedule for task."""
    try:
        query = update.callback_query
        query.answer()
        
        keyboard = [
            [InlineKeyboardButton("⏰ Set Start Time", callback_data="set_start_time")],
            [InlineKeyboardButton("🔄 Set Repeat Interval", callback_data="set_interval")],
            [InlineKeyboardButton("⏹️ Set End Time", callback_data="set_end_time")],
            [InlineKeyboardButton("⬅️ Back to Task", callback_data="back_to_task_config")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text("⏰ **Schedule Configuration**\n\nChoose what to configure:", reply_markup=markup)
        return TASK_SETUP
        
    except Exception as e:
        log_event("Handle Set Schedule Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_choose_targets(update: Update, context):
    """Handle choosing target groups for task."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get user's folders
        with db_lock:
            cursor.execute("""
                SELECT f.id, f.name, COUNT(tg.group_id) as group_count
                FROM folders f
                LEFT JOIN target_groups tg ON f.id = tg.folder_id
                WHERE f.created_by = ?
                GROUP BY f.id, f.name
                HAVING group_count > 0
                ORDER BY f.name
            """, (str(user_id),))
            folders = cursor.fetchall()
        
        message = "🎯 **Choose Target Groups**\n\n"
        keyboard = []
        
        if folders:
            for folder_id, name, group_count in folders:
                message += f"📂 {name} ({group_count} groups)\n"
                keyboard.append([InlineKeyboardButton(f"📂 {name}", callback_data=f"select_folder_{folder_id}")])
        
        keyboard.append([InlineKeyboardButton("🌐 All Groups", callback_data="select_all_groups")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Task", callback_data="back_to_task_config")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return TASK_SETUP
        
    except Exception as e:
        log_event("Handle Choose Targets Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_create_task_final(update: Update, context):
    """Create the final task."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get client invitation code
        with db_lock:
            cursor.execute("SELECT invitation_code FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if not result:
            query.edit_message_text("Account not found.")
            return ConversationHandler.END
        
        invitation_code = result[0]
        
        # Get task configuration from context
        task_name = context.user_data.get('task_name', 'Untitled Task')
        userbot_phone = context.user_data.get('selected_userbot')
        message_link = context.user_data.get('message_link')
        folder_id = context.user_data.get('selected_folder_id')
        send_to_all = context.user_data.get('send_to_all_groups', False)
        
        if not userbot_phone:
            query.edit_message_text("No userbot selected. Please start over.")
            return ConversationHandler.END
        
        # Create task
        with db_lock:
            cursor.execute("""
                INSERT INTO tasks (name, client_id, userbot_phone, message_link, 
                                 folder_id, send_to_all_groups, status, created_by)
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
            """, (task_name, invitation_code, userbot_phone, message_link, 
                  folder_id, int(send_to_all), user_id))
            task_id = cursor.lastrowid
            db.commit()
        
        log_event("Task Created", f"User: {user_id}, Task: {task_name}, Userbot: {userbot_phone}")
        
        # Clear context data
        for key in ['task_name', 'selected_userbot', 'message_link', 'selected_folder_id', 'send_to_all_groups']:
            context.user_data.pop(key, None)
        
        query.edit_message_text(
            f"✅ **Task Created Successfully!**\n\n"
            f"📝 Task: {task_name}\n"
            f"📱 Userbot: {userbot_phone}\n"
            f"🎯 Target: {'All Groups' if send_to_all else 'Selected Folder'}\n\n"
            f"Your task is now active and will start forwarding messages!"
        )
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Handle Create Task Final Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred while creating the task.")
        return ConversationHandler.END

def handle_cancel_task_creation(update: Update, context):
    """Cancel task creation."""
    try:
        query = update.callback_query
        query.answer()
        
        # Clear context data
        for key in ['task_name', 'selected_userbot', 'message_link', 'selected_folder_id', 'send_to_all_groups']:
            context.user_data.pop(key, None)
        
        query.edit_message_text("❌ Task creation cancelled.")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Handle Cancel Task Creation Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_join_folder_groups(update: Update, context):
    """Handle joining groups from a specific folder."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Extract folder_id from callback data
        folder_id = int(query.data.split("_")[2])
        
        # Get client info
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if not result or not result[0]:
            query.edit_message_text("No userbots assigned to your account.")
            return ConversationHandler.END
        
        userbots = result[0].split(",")
        
        query.edit_message_text("🔄 Joining groups... This may take a moment.")
        
        # Start async group joining
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            total_joins, total_attempts, errors = loop.run_until_complete(
                join_groups_from_folder(folder_id, userbots, user_id)
            )
            
            message = f"✅ **Group Joining Complete!**\n\n"
            message += f"📊 Successfully joined: {total_joins}/{total_attempts} groups\n"
            
            if errors:
                message += f"\n❌ Errors ({len(errors)}):\n"
                for error in errors[:3]:
                    message += f"• {error}\n"
                if len(errors) > 3:
                    message += f"... and {len(errors) - 3} more errors\n"
            
            log_event("Groups Joined", f"User: {user_id}, Folder: {folder_id}, Joins: {total_joins}")
            
        except Exception as e:
            message = f"❌ Error joining groups: {str(e)}"
        finally:
            loop.close()
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")]]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Handle Join Folder Groups Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_join_all_groups(update: Update, context):
    """Handle joining all groups."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get client info and all folders
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            
            if not result or not result[0]:
                query.edit_message_text("No userbots assigned to your account.")
                return ConversationHandler.END
            
            userbots = result[0].split(",")
            
            # Get all user's folders with groups
            cursor.execute("""
                SELECT DISTINCT f.id FROM folders f
                JOIN target_groups tg ON f.id = tg.folder_id
                WHERE f.created_by = ?
            """, (str(user_id),))
            folder_ids = [row[0] for row in cursor.fetchall()]
        
        if not folder_ids:
            query.edit_message_text("No groups found to join.")
            return ConversationHandler.END
        
        query.edit_message_text("🔄 Joining all groups... This may take several minutes.")
        
        total_joins = 0
        total_attempts = 0
        all_errors = []
        
        # Join groups from all folders
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            for folder_id in folder_ids:
                joins, attempts, errors = loop.run_until_complete(
                    join_groups_from_folder(folder_id, userbots, user_id)
                )
                total_joins += joins
                total_attempts += attempts
                all_errors.extend(errors)
            
            message = f"✅ **All Groups Joining Complete!**\n\n"
            message += f"📊 Successfully joined: {total_joins}/{total_attempts} groups\n"
            message += f"📁 Processed {len(folder_ids)} folders\n"
            
            if all_errors:
                message += f"\n❌ Errors ({len(all_errors)}):\n"
                for error in all_errors[:5]:
                    message += f"• {error}\n"
                if len(all_errors) > 5:
                    message += f"... and {len(all_errors) - 5} more errors\n"
            
            log_event("All Groups Joined", f"User: {user_id}, Total Joins: {total_joins}")
            
        except Exception as e:
            message = f"❌ Error joining groups: {str(e)}"
        finally:
            loop.close()
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")]]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Handle Join All Groups Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Complete placeholder functions

def client_setup_tasks(update: Update, context):
    """Client task setup and management."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get client's invitation code
        with db_lock:
            cursor.execute("SELECT invitation_code, dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if not result:
            query.edit_message_text("Account not found.")
            return ConversationHandler.END
            
        invitation_code, userbots_str = result
        
        # Get existing tasks
        with db_lock:
            cursor.execute("""
                SELECT id, name, userbot_phone, status, last_run, successful_runs, total_runs
                FROM tasks WHERE client_id = ? ORDER BY created_at DESC
            """, (invitation_code,))
            tasks = cursor.fetchall()
        
        message = "🎯 **Setup Tasks**\n\n"
        keyboard = []
        
        if tasks:
            for task_id, name, phone, status, last_run, success, total in tasks:
                status_emoji = {"active": "🟢", "paused": "⏸️", "completed": "✅", "failed": "❌"}.get(status, "⚪")
                success_rate = (success / total * 100) if total > 0 else 0
                last_run_str = datetime.fromtimestamp(last_run).strftime('%m/%d %H:%M') if last_run else "Never"
                
                message += f"{status_emoji} {name}\n"
                message += f"   Phone: {phone} | Success: {success_rate:.1f}%\n"
                message += f"   Last run: {last_run_str}\n\n"
                
                keyboard.append([InlineKeyboardButton(f"Edit: {name[:20]}...", 
                                                    callback_data=f"client_edit_task_{task_id}")])
        else:
            message += "No tasks found. Create your first task!\n\n"
        
        keyboard.append([InlineKeyboardButton("➕ Create New Task", callback_data="client_create_task")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Client Setup Tasks Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def process_invitation_code(update: Update, context):
    """Process invitation code from clients."""
    try:
        user_id = update.effective_user.id
        code = update.message.text.strip()
        
        with db_lock:
            cursor.execute("""
                SELECT subscription_end, status FROM clients 
                WHERE invitation_code = ? AND (user_id IS NULL OR user_id = ?)
            """, (code, user_id))
            result = cursor.fetchone()
        
        if not result:
            update.message.reply_text(get_text(user_id, 'invalid_code'))
            return ConversationHandler.END
        
        subscription_end, status = result
        current_time = int(datetime.now(utc_tz).timestamp())
        
        # Check if subscription is expired
        if subscription_end <= current_time:
            update.message.reply_text("❌ This invitation code has expired.")
            return ConversationHandler.END
        
        # Activate the account
        with db_lock:
            cursor.execute("""
                UPDATE clients 
                SET user_id = ?, status = 'active' 
                WHERE invitation_code = ?
            """, (user_id, code))
            db.commit()
        
        log_event("Client Activated", f"User: {user_id}, Code: {code}")
        
        update.message.reply_text(get_text(user_id, 'account_activated'))
        return client_menu(update, context)
        
    except Exception as e:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        log_event("Invitation Code Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("❌ An error occurred while processing your invitation code. Please try again.")
        return ConversationHandler.END

# Missing admin functions

def start_invite_generation(update: Update, context):
    """Start the invitation generation conversation."""
    try:
        # Handle both direct calls and callback queries
        if hasattr(update, 'callback_query') and update.callback_query:
            query = update.callback_query
            query.answer()
            
            query.edit_message_text(
                "🎫 Generate New Invitation Code\n\n"
                "Please enter the subscription duration in days (e.g., 30, 60, 90):"
            )
        else:
            update.message.reply_text(
                "🎫 Generate New Invitation Code\n\n"
                "Please enter the subscription duration in days (e.g., 30, 60, 90):"
            )
        return WAITING_FOR_INVITE_DURATION
    except Exception as e:
        logging.error(f"Start invite generation error: {e}")
        if hasattr(update, 'callback_query') and update.callback_query:
            update.callback_query.edit_message_text("An error occurred while starting invitation generation.")
        else:
            update.message.reply_text("An error occurred while starting invitation generation.")
        return ConversationHandler.END

def admin_generate_invite(update: Update, context):
    """Handle the generate invite callback and start the conversation."""
    try:
        query = update.callback_query
        query.answer()
        return start_invite_generation(update, context)
    except Exception as e:
        logging.error(f"Admin generate invite error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def cancel_invite_generation(update: Update, context):
    """Cancel current operation and return to main menu."""
    try:
        user_id = update.effective_user.id
        if is_admin(user_id):
            update.message.reply_text("Operation cancelled. Use /start to return to admin panel.")
        else:
            update.message.reply_text("Operation cancelled. Use /start to return to main menu.")
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"Cancel operation error: {e}")
        return ConversationHandler.END

def admin_add_userbot(update: Update, context):
    """Add a new userbot to the system."""
    try:
        query = update.callback_query
        query.answer()
        query.edit_message_text("🔧 Add Userbot feature coming soon!\n\nThis will allow you to add new userbots to the system.")
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"Add userbot error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_remove_userbot(update: Update, context):
    """Remove a userbot from the system."""
    try:
        query = update.callback_query
        query.answer()
        query.edit_message_text("🗑️ Remove Userbot feature coming soon!\n\nThis will allow you to remove userbots from the system.")
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"Remove userbot error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_view_subs(update: Update, context):
    """View all client subscriptions."""
    try:
        query = update.callback_query
        query.answer()
        
        with db_lock:
            cursor.execute("""
                SELECT invitation_code, user_id, subscription_end, status, created_at
                FROM clients 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            clients = cursor.fetchall()
        
        if not clients:
            query.edit_message_text("No clients found.")
            return ConversationHandler.END
        
        message = "📈 Client Subscriptions (Last 10):\n\n"
        for code, user_id, sub_end, status, created_at in clients:
            end_date = datetime.fromtimestamp(sub_end).strftime('%Y-%m-%d')
            created_date = datetime.fromtimestamp(created_at).strftime('%m/%d')
            status_emoji = "🟢" if status == "active" else "⚪"
            
            message += f"{status_emoji} {code}\n"
            message += f"   User: {user_id or 'Not activated'}\n"
            message += f"   Expires: {end_date} | Created: {created_date}\n\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")]]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"View subscriptions error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_view_logs(update: Update, context):
    """View system logs."""
    try:
        query = update.callback_query
        query.answer()
        
        with db_lock:
            cursor.execute("""
                SELECT timestamp, event, details, client_id 
                FROM logs 
                ORDER BY timestamp DESC 
                LIMIT 15
            """)
            logs = cursor.fetchall()
        
        if not logs:
            query.edit_message_text("No logs found.")
            return ConversationHandler.END
        
        message = "📋 System Logs (Last 15):\n\n"
        for timestamp, event, details, client_id in logs:
            log_time = datetime.fromtimestamp(timestamp).strftime('%m/%d %H:%M')
            message += f"🕒 {log_time} - {event}\n"
            if client_id:
                message += f"   Client: {client_id}\n"
            if details:
                message += f"   {details[:50]}{'...' if len(details) > 50 else ''}\n"
            message += "\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")]]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"View logs error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_extend_sub(update: Update, context):
    """Extend client subscription."""
    try:
        query = update.callback_query
        query.answer()
        query.edit_message_text("⏰ Extend Subscription feature coming soon!\n\nThis will allow you to extend client subscription periods.")
        return ConversationHandler.END
    except Exception as e:
        logging.error(f"Extend subscription error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_create_task(update: Update, context):
    """Start creating a new task."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get client's userbots
        with db_lock:
            cursor.execute("SELECT dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if not result or not result[0]:
            query.edit_message_text("No userbots assigned to your account.")
            return ConversationHandler.END
            
        userbots = result[0].split(",")
        
        message = "📝 **Create New Task**\n\nSelect a userbot for this task:\n\n"
        keyboard = []
        
        for phone in userbots:
            with db_lock:
                cursor.execute("SELECT username FROM userbots WHERE phone_number = ?", (phone.strip(),))
                username_result = cursor.fetchone()
                username = username_result[0] if username_result and username_result[0] else None
            display_name = f"@{username}" if username else f"{phone.strip()}"
            
            keyboard.append([InlineKeyboardButton(display_name, callback_data=f"select_userbot_{phone.strip()}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Back to Tasks", callback_data="client_setup_tasks")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return WAITING_FOR_USERBOT_SELECTION
        
    except Exception as e:
        log_event("Client Create Task Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_manage_folders(update: Update, context):
    """Manage client folders."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get existing folders
        with db_lock:
            cursor.execute("""
                SELECT f.id, f.name, COUNT(tg.group_id) as group_count
                FROM folders f
                LEFT JOIN target_groups tg ON f.id = tg.folder_id
                WHERE f.created_by = ?
                GROUP BY f.id, f.name
                ORDER BY f.created_at DESC
            """, (str(user_id),))
            folders = cursor.fetchall()
        
        message = "📁 **Manage Folders**\n\n"
        keyboard = []
        
        if folders:
            for folder_id, name, group_count in folders:
                message += f"📂 {name} ({group_count} groups)\n"
                keyboard.append([InlineKeyboardButton(f"Edit: {name}", callback_data=f"edit_folder_{folder_id}")])
        else:
            message += "No folders found. Create your first folder!\n\n"
        
        keyboard.append([InlineKeyboardButton("➕ Create New Folder", callback_data="create_new_folder")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return WAITING_FOR_FOLDER_ACTION
        
    except Exception as e:
        log_event("Client Manage Folders Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_join_target_groups(update: Update, context):
    """Join target groups with userbots."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        # Get client info
        with db_lock:
            cursor.execute("SELECT invitation_code, dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
        
        if not result or not result[1]:
            query.edit_message_text("No userbots assigned to your account.")
            return ConversationHandler.END
            
        invitation_code, userbots_str = result
        userbots = userbots_str.split(",")
        
        # Get folders with groups
        with db_lock:
            cursor.execute("""
                SELECT f.id, f.name, COUNT(tg.group_id) as group_count
                FROM folders f
                LEFT JOIN target_groups tg ON f.id = tg.folder_id
                WHERE f.created_by = ? AND tg.group_id IS NOT NULL
                GROUP BY f.id, f.name
                HAVING group_count > 0
                ORDER BY f.name
            """, (str(user_id),))
            folders = cursor.fetchall()
        
        message = "🎯 **Join Target Groups**\n\nSelect a folder to join groups from:\n\n"
        keyboard = []
        
        if folders:
            for folder_id, name, group_count in folders:
                message += f"📂 {name} ({group_count} groups)\n"
                keyboard.append([InlineKeyboardButton(f"Join: {name}", callback_data=f"join_folder_{folder_id}")])
        else:
            message += "No folders with groups found. Please add groups to your folders first.\n\n"
        
        keyboard.append([InlineKeyboardButton("🔗 Join All Groups", callback_data="join_all_groups")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Client Join Groups Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_set_language(update: Update, context):
    """Set client language preference."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        keyboard = [
            [InlineKeyboardButton("🇺🇸 English", callback_data="set_lang_en")],
            [InlineKeyboardButton("🇺🇦 Українська", callback_data="set_lang_uk")],
            [InlineKeyboardButton("🇵🇱 Polski", callback_data="set_lang_pl")],
            [InlineKeyboardButton("🇱🇹 Lietuvių", callback_data="set_lang_lt")],
            [InlineKeyboardButton("🇷🇺 Русский", callback_data="set_lang_ru")],
            [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")]
        ]
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text("🌐 **Select Language**\n\nChoose your preferred language:", reply_markup=markup)
        return WAITING_FOR_LANGUAGE
        
    except Exception as e:
        log_event("Client Set Language Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_view_logs(update: Update, context):
    """View client logs and statistics."""
    try:
        query = update.callback_query
        query.answer()
        user_id = update.effective_user.id
        
        with db_lock:
            cursor.execute("""
                SELECT invitation_code, total_messages_sent, groups_reached, forwards_count 
                FROM clients WHERE user_id = ?
            """, (user_id,))
            result = cursor.fetchone()
        
        if not result:
            query.edit_message_text("Account not found.")
            return ConversationHandler.END
            
        invitation_code, total_sent, groups_reached, forwards_count = result
        
        # Get recent logs
        with db_lock:
            cursor.execute("""
                SELECT timestamp, event, details 
                FROM logs WHERE client_id = ? 
                ORDER BY timestamp DESC LIMIT 10
            """, (invitation_code,))
            logs = cursor.fetchall()
        
        message = f"📊 **Account Statistics**\n\n"
        message += f"📨 Total Messages Sent: {total_sent or 0}\n"
        message += f"👥 Groups Reached: {groups_reached or 0}\n"
        message += f"🔄 Forwards Count: {forwards_count or 0}\n\n"
        
        if logs:
            message += "📋 **Recent Activity:**\n"
            for timestamp, event, details in logs[:5]:
                date_str = datetime.fromtimestamp(timestamp).strftime('%m/%d %H:%M')
                message += f"• {date_str} - {event}\n"
        
        keyboard = [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_client_menu")]]
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Client View Logs Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def process_group_links_for_folder(update: Update, context):
    """Process group links for folder."""
    try:
        user_id = update.effective_user.id
        folder_id = context.user_data.get('target_folder_id')
        
        if not folder_id:
            update.message.reply_text("Session expired. Please start over.")
            return ConversationHandler.END
        
        links_text = update.message.text.strip()
        links = [link.strip() for link in links_text.split('\n') if link.strip()]
        
        if not links:
            update.message.reply_text("No valid links found. Please try again:")
            return WAITING_FOR_GROUP_LINKS_FOLDER
        
        added_count = 0
        errors = []
        
        for link in links:
            try:
                # Parse group link/username
                if link.startswith('@'):
                    group_username = link[1:]
                    group_link = f"https://t.me/{group_username}"
                    group_id = hash(group_username) % (10**15)  # Generate a pseudo ID
                elif 't.me' in link:
                    group_link = link
                    if '/joinchat/' in link:
                        group_id = hash(link.split('/joinchat/')[1]) % (10**15)
                    else:
                        group_username = link.split('/')[-1]
                        group_id = hash(group_username) % (10**15)
                else:
                    errors.append(f"Invalid format: {link}")
                    continue
                
                # Extract group name
                group_name = link.split('/')[-1] if 't.me' in link else link[1:]
                
                # Add to database
                with db_lock:
                    cursor.execute("""
                        INSERT OR IGNORE INTO target_groups 
                        (group_id, group_name, group_link, added_by, folder_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (group_id, group_name, group_link, str(user_id), folder_id))
                    if cursor.rowcount > 0:
                        added_count += 1
                    db.commit()
                
            except Exception as e:
                errors.append(f"Error with {link}: {str(e)}")
        
        # Get folder name
        with db_lock:
            cursor.execute("SELECT name FROM folders WHERE id = ?", (folder_id,))
            folder_name = cursor.fetchone()[0]
        
        message = f"✅ **Groups Added to {folder_name}**\n\n"
        message += f"✅ Successfully added: {added_count} groups\n"
        
        if errors:
            message += f"❌ Errors: {len(errors)}\n"
            for error in errors[:3]:  # Show first 3 errors
                message += f"• {error}\n"
        
        log_event("Groups Added to Folder", f"User: {user_id}, Folder: {folder_name}, Count: {added_count}")
        
        keyboard = [
            [InlineKeyboardButton("➕ Add More Groups", callback_data=f"add_groups_{folder_id}")],
            [InlineKeyboardButton("⬅️ Back to Folders", callback_data="client_manage_folders")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Process Group Links Error", f"User: {user_id}, Error: {e}")
        update.message.reply_text("An error occurred. Please try again.")
        return ConversationHandler.END

def process_invite_duration(update: Update, context):
    """Process the duration input and generate invitation code."""
    try:
        user_id = update.effective_user.id
        if not is_admin(user_id):
            update.message.reply_text("Unauthorized")
            return ConversationHandler.END
        
        duration_text = update.message.text.strip()
        
        # Validate the input
        try:
            days = int(duration_text)
            if days <= 0 or days > 365:
                update.message.reply_text(
                    "❌ Invalid duration. Please enter a number between 1 and 365 days."
                )
                return WAITING_FOR_INVITE_DURATION
        except ValueError:
            update.message.reply_text(
                "❌ Invalid input. Please enter a valid number of days (e.g., 30, 60, 90)."
            )
            return WAITING_FOR_INVITE_DURATION
        
        # Generate a unique invitation code
        invite_code = str(uuid.uuid4())[:8]
        
        # Set subscription end based on user input
        subscription_end = int((datetime.now() + timedelta(days=days)).timestamp())
        
        with db_lock:
            cursor.execute("""
                INSERT INTO clients (invitation_code, subscription_end, created_by, status)
                VALUES (?, ?, ?, 'inactive')
            """, (invite_code, subscription_end, user_id))
            db.commit()
        
        log_admin_action(user_id, "generate_invite", invite_code, f"Generated new invitation code for {days} days")
        
        end_date = datetime.fromtimestamp(subscription_end).strftime('%Y-%m-%d')
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="admin_panel")]]
        markup = InlineKeyboardMarkup(keyboard)
        
        update.message.reply_text(
            f"✅ New invitation code generated!\n\n"
            f"🎫 Code: `{invite_code}`\n"
            f"⏰ Duration: {days} days\n"
            f"📅 Valid until: {end_date}\n\n"
            f"Send this code to the client to activate their account.",
            reply_markup=markup,
            parse_mode='Markdown'
        )
        return ConversationHandler.END
        
    except Exception as e:
        logging.error(f"Process invite duration error: {e}")
        update.message.reply_text("An error occurred while generating invitation code.")
        return ConversationHandler.END

if __name__ == "__main__":
    # Set up main conversation handler
    main_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CallbackQueryHandler(handle_callback_query)
        ],
        states={
            WAITING_FOR_CODE: [
                MessageHandler(Filters.text & ~Filters.command, process_invitation_code)
            ],
            WAITING_FOR_INVITE_DURATION: [
                MessageHandler(Filters.text & ~Filters.command, process_invite_duration)
            ],
            WAITING_FOR_TASK_NAME_CLIENT: [
                MessageHandler(Filters.text & ~Filters.command, process_task_name)
            ],
            WAITING_FOR_FOLDER_NAME: [
                MessageHandler(Filters.text & ~Filters.command, process_folder_name)
            ],
            WAITING_FOR_GROUP_LINKS_FOLDER: [
                MessageHandler(Filters.text & ~Filters.command, process_group_links_for_folder)
            ],
            WAITING_FOR_LANGUAGE: [
                CallbackQueryHandler(handle_callback_query)
            ],
            TASK_SETUP: [
                CallbackQueryHandler(handle_callback_query)
            ],
            WAITING_FOR_FOLDER_ACTION: [
                CallbackQueryHandler(handle_callback_query)
            ],
            WAITING_FOR_USERBOT_SELECTION: [
                CallbackQueryHandler(handle_callback_query)
            ],
            ADMIN_CLIENT_SELECTION: [
                CallbackQueryHandler(handle_callback_query)
            ],
            ADMIN_BULK_OPERATIONS: [
                CallbackQueryHandler(handle_callback_query)
            ],
            ADMIN_TEMPLATE_MANAGEMENT: [
                CallbackQueryHandler(handle_callback_query)
            ],
        },
        fallbacks=[
            CommandHandler('start', start),
            CommandHandler('cancel', cancel_invite_generation),
            CallbackQueryHandler(handle_callback_query)
        ]
    )
    
    # Add main conversation handler
    dp.add_handler(main_conv_handler)
    
    # Start the bot
    updater.start_polling()
    logging.info("Enhanced Auto Forwarding Bot started!")
    updater.idle()
