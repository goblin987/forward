#!/usr/bin/env python3
"""
Enhanced Auto Forwarding Bot - Demo Setup Script
This script helps you quickly set up and test the enhanced auto forwarding system.
"""

import os
import sqlite3
import json
from datetime import datetime, timedelta

def create_demo_environment():
    """Create a demo environment with sample data."""
    print("🚀 Setting up Enhanced Auto Forwarding Bot Demo Environment...")
    
    # Create data directory
    data_dir = "./demo_data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"✅ Created data directory: {data_dir}")
    
    # Create sessions directory
    sessions_dir = os.path.join(data_dir, "sessions")
    if not os.path.exists(sessions_dir):
        os.makedirs(sessions_dir)
        print(f"✅ Created sessions directory: {sessions_dir}")
    
    # Create demo database
    db_path = os.path.join(data_dir, "telegram_bot.db")
    print(f"📊 Setting up database: {db_path}")
    
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    
    # Create tables
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
    
    print("✅ Database tables created successfully!")
    
    # Insert demo data
    print("📝 Inserting demo data...")
    
    # Demo clients
    subscription_end = int((datetime.now() + timedelta(days=30)).timestamp())
    
    demo_clients = [
        ("demo123", 111111111, subscription_end, "+1234567890,+0987654321", "Marketing", 5, 10, 50, "en", 999999999),
        ("test456", 222222222, subscription_end, "+1111111111", "Sales", 3, 8, 25, "en", 999999999),
        ("client789", 333333333, subscription_end, "+2222222222,+3333333333", "Support", 7, 15, 75, "en", 999999999)
    ]
    
    for client in demo_clients:
        cursor.execute("""
            INSERT OR IGNORE INTO clients 
            (invitation_code, user_id, subscription_end, dedicated_userbots, folder_name, 
             forwards_count, groups_reached, total_messages_sent, language, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, client)
    
    # Demo userbots
    demo_userbots = [
        ("+1234567890", "userbot1.session", "active", "demo123", 12345, "your_api_hash", "demo_bot1"),
        ("+0987654321", "userbot2.session", "active", "demo123", 12345, "your_api_hash", "demo_bot2"),
        ("+1111111111", "userbot3.session", "active", "test456", 12345, "your_api_hash", "demo_bot3"),
        ("+2222222222", "userbot4.session", "active", "client789", 12345, "your_api_hash", "demo_bot4"),
        ("+3333333333", "userbot5.session", "inactive", "client789", 12345, "your_api_hash", "demo_bot5")
    ]
    
    for userbot in demo_userbots:
        cursor.execute("""
            INSERT OR IGNORE INTO userbots 
            (phone_number, session_file, status, assigned_client, api_id, api_hash, username)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, userbot)
    
    # Demo folders
    demo_folders = [
        ("Marketing Channels", "demo123"),
        ("Sales Groups", "test456"),
        ("Support Communities", "client789"),
        ("General Promotion", "demo123")
    ]
    
    for folder in demo_folders:
        cursor.execute("INSERT OR IGNORE INTO folders (name, created_by) VALUES (?, ?)", folder)
    
    # Demo target groups
    demo_groups = [
        (-1001234567890, "Marketing Channel 1", "https://t.me/marketing1", "demo123", 1),
        (-1001234567891, "Marketing Channel 2", "https://t.me/marketing2", "demo123", 1),
        (-1001234567892, "Sales Group 1", "https://t.me/sales1", "test456", 2),
        (-1001234567893, "Support Community", "https://t.me/support1", "client789", 3)
    ]
    
    for group in demo_groups:
        cursor.execute("""
            INSERT OR IGNORE INTO target_groups 
            (group_id, group_name, group_link, added_by, folder_id)
            VALUES (?, ?, ?, ?, ?)
        """, group)
    
    # Demo tasks
    start_time = int(datetime.now().timestamp())
    
    demo_tasks = [
        ("Daily Marketing Blast", "demo123", "+1234567890", "https://t.me/demo/123", 
         "https://t.me/demo/124", start_time, None, 1440, "active", 1, 0, None, 5, 4, 1),
        ("Hourly Sales Updates", "test456", "+1111111111", "https://t.me/demo/125", 
         None, start_time, None, 60, "active", 2, 0, None, 12, 10, 2),
        ("Support Announcements", "client789", "+2222222222", "https://t.me/demo/126", 
         "https://t.me/demo/127", start_time, None, 720, "paused", 3, 0, None, 8, 6, 2)
    ]
    
    for task in demo_tasks:
        cursor.execute("""
            INSERT OR IGNORE INTO tasks 
            (name, client_id, userbot_phone, message_link, fallback_message_link,
             start_time, end_time, repetition_interval, status, folder_id, send_to_all_groups,
             last_run, total_runs, successful_runs, failed_runs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, task)
    
    # Demo task templates
    demo_templates = [
        ("Daily Promotion", "Standard daily promotional message template", 
         json.dumps({"repetition_interval": 1440, "send_to_all_groups": 0, "enable_fallback": True}), 999999999),
        ("Hourly Updates", "Frequent update template for active campaigns",
         json.dumps({"repetition_interval": 60, "send_to_all_groups": 1, "enable_fallback": False}), 999999999)
    ]
    
    for template in demo_templates:
        cursor.execute("""
            INSERT OR IGNORE INTO task_templates 
            (name, description, config_json, created_by)
            VALUES (?, ?, ?, ?)
        """, template)
    
    # Demo logs
    demo_logs = [
        (int(datetime.now().timestamp()), "Task Executed", "Daily Marketing Blast completed successfully", "demo123", 1),
        (int(datetime.now().timestamp()) - 3600, "Task Failed", "Hourly Sales Updates failed - userbot offline", "test456", 2),
        (int(datetime.now().timestamp()) - 7200, "Admin Action", "Task paused by admin", "client789", 3)
    ]
    
    for log in demo_logs:
        cursor.execute("""
            INSERT INTO logs (timestamp, event, details, client_id, task_id)
            VALUES (?, ?, ?, ?, ?)
        """, log)
    
    db.commit()
    db.close()
    
    print("✅ Demo data inserted successfully!")
    
    # Create environment file template
    env_template = """# Enhanced Auto Forwarding Bot Configuration
# Copy this to .env and update with your actual values

# Telegram API Configuration
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_bot_token

# Admin Configuration (comma-separated user IDs)
ADMIN_IDS=999999999,888888888

# Data Directory (optional)
DATA_DIR=./demo_data

# Bot Configuration (optional)
CHECK_TASKS_INTERVAL=60
CLIENT_TIMEOUT=30
"""
    
    with open(".env.template", "w") as f:
        f.write(env_template)
    
    print("✅ Environment template created: .env.template")
    
    return data_dir

def print_demo_info():
    """Print information about the demo setup."""
    print("\n🎉 Demo Environment Setup Complete!")
    print("\n📋 Demo Data Created:")
    print("   • 3 Demo clients with invitation codes: demo123, test456, client789")
    print("   • 5 Demo userbots with different statuses")
    print("   • 4 Target group folders")
    print("   • 3 Active/paused tasks")
    print("   • 2 Task templates")
    print("   • Sample logs and admin actions")
    
    print("\n🔧 Setup Instructions:")
    print("1. Copy .env.template to .env")
    print("2. Update .env with your actual Telegram credentials")
    print("3. Add your Telegram user ID to ADMIN_IDS")
    print("4. Run: python enhanced_frw.py")
    
    print("\n👨‍💼 Admin Features to Test:")
    print("   • Send /start to bot as admin")
    print("   • Try 'Manage Client Tasks' to see all clients")
    print("   • Use 'Bulk Operations' to pause/resume tasks")
    print("   • Create templates in 'Task Templates'")
    print("   • Generate system reports")
    
    print("\n👤 Client Features to Test:")
    print("   • Use invitation codes: demo123, test456, or client789")
    print("   • Try enhanced task setup")
    print("   • View task statistics")
    print("   • Edit existing tasks")
    
    print("\n📊 Database Location: ./demo_data/telegram_bot.db")
    print("📁 Sessions Directory: ./demo_data/sessions/")
    print("📝 Logs: ./demo_data/bot.log")

def main():
    """Main demo setup function."""
    print("Enhanced Telegram Auto Forwarding Bot")
    print("=====================================")
    print("This script will create a demo environment with sample data.")
    print()
    
    # Check if demo already exists
    if os.path.exists("./demo_data/telegram_bot.db"):
        response = input("Demo database already exists. Recreate? (y/N): ")
        if response.lower() != 'y':
            print("Demo setup cancelled.")
            return
    
    try:
        data_dir = create_demo_environment()
        print_demo_info()
        
        print(f"\n✨ Demo environment ready in: {data_dir}")
        print("\nNext steps:")
        print("1. Configure your .env file")
        print("2. Run: python enhanced_frw.py")
        print("3. Start testing with /start command!")
        
    except Exception as e:
        print(f"❌ Error setting up demo: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 