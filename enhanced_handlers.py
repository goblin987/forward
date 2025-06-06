# Enhanced callback handlers and utility functions for the auto forwarding system

def handle_enhanced_callback(update: Update, context):
    """Enhanced callback handler for all admin and client operations."""
    try:
        query = update.callback_query
        query.answer()
        data = query.data
        user_id = query.from_user.id

        # Admin-specific callbacks
        if data == "admin_panel":
            return enhanced_admin_panel(update, context)
        
        elif data == "admin_manage_client_tasks":
            return admin_manage_client_tasks(update, context)
        
        elif data == "admin_view_all_tasks":
            return admin_view_all_tasks(update, context)
        
        elif data == "admin_bulk_operations":
            return admin_bulk_operations(update, context)
        
        elif data == "admin_task_templates":
            return admin_task_templates(update, context)
        
        elif data.startswith("admin_client_"):
            client_code = data.split("_")[2]
            return admin_view_client_tasks(update, context, client_code)
        
        elif data == "admin_create_new_task":
            return admin_create_task_workflow(update, context)
        
        elif data.startswith("admin_edit_task_"):
            task_id = int(data.split("_")[3])
            return admin_edit_task(update, context, task_id)
        
        # Bulk operations
        elif data == "bulk_pause_all":
            return handle_bulk_pause_all(update, context)
        
        elif data == "bulk_resume_all":
            return handle_bulk_resume_all(update, context)
        
        elif data == "bulk_delete_completed":
            return handle_bulk_delete_completed(update, context)
        
        elif data == "bulk_generate_report":
            return handle_bulk_generate_report(update, context)
        
        elif data == "bulk_restart_failed":
            return handle_bulk_restart_failed(update, context)
        
        # Template operations
        elif data == "create_template":
            return admin_create_template(update, context)
        
        elif data.startswith("use_template_"):
            template_id = int(data.split("_")[2])
            return admin_use_template(update, context, template_id)
        
        # Client callbacks (existing ones enhanced)
        elif data == "client_setup_tasks":
            return client_setup_enhanced_tasks(update, context)
        
        elif data == "back_to_client_menu":
            return client_menu(update, context)
        
        # Default fallback to original handler
        else:
            # Call original callback handler for existing functionality
            return handle_original_callback(update, context)
        
    except Exception as e:
        log_event("Enhanced Callback Error", f"User: {user_id}, Data: {data}, Error: {e}")
        query.edit_message_text("An error occurred. Please try again.")
        return ConversationHandler.END

def admin_view_client_tasks(update: Update, context, client_code):
    """View and manage tasks for a specific client."""
    try:
        query = update.callback_query
        
        with db_lock:
            cursor.execute("""
                SELECT t.id, t.name, t.userbot_phone, t.status, t.last_run, 
                       t.total_runs, t.successful_runs, t.failed_runs,
                       c.user_id
                FROM tasks t
                JOIN clients c ON t.client_id = c.invitation_code
                WHERE t.client_id = ?
                ORDER BY t.created_at DESC
            """, (client_code,))
            tasks = cursor.fetchall()
            
            cursor.execute("SELECT user_id, dedicated_userbots FROM clients WHERE invitation_code = ?", (client_code,))
            client_info = cursor.fetchone()
        
        if not client_info:
            query.edit_message_text("Client not found.")
            return ConversationHandler.END
        
        user_id, userbots_str = client_info
        userbot_phones = userbots_str.split(",") if userbots_str else []
        
        message = f"📋 Client Tasks: {client_code} (User: {user_id})\n"
        message += f"Available Userbots: {len(userbot_phones)}\n\n"
        
        keyboard = []
        
        if tasks:
            for task_id, name, phone, status, last_run, total, success, failed in tasks:
                status_emoji = {"active": "🟢", "paused": "⏸️", "completed": "✅", "failed": "❌"}.get(status, "⚪")
                last_run_str = datetime.fromtimestamp(last_run).strftime('%m/%d %H:%M') if last_run else "Never"
                
                message += f"{status_emoji} {name}\n"
                message += f"   Phone: {phone} | Runs: {success}/{total}\n"
                message += f"   Last: {last_run_str}\n\n"
                
                keyboard.append([
                    InlineKeyboardButton(f"Edit {name[:15]}...", callback_data=f"admin_edit_task_{task_id}"),
                    InlineKeyboardButton("❌", callback_data=f"admin_delete_task_{task_id}")
                ])
        else:
            message += "No tasks found for this client.\n\n"
        
        keyboard.append([InlineKeyboardButton("➕ Create Task for Client", callback_data=f"admin_create_task_{client_code}")])
        keyboard.append([InlineKeyboardButton("⬅️ Back to Client List", callback_data="admin_manage_client_tasks")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Admin View Client Tasks Error", f"Client: {client_code}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_create_task_workflow(update: Update, context):
    """Start the workflow to create a new task for any client."""
    try:
        query = update.callback_query
        
        with db_lock:
            cursor.execute("SELECT invitation_code, user_id FROM clients WHERE status = 'active' ORDER BY created_at DESC")
            clients = cursor.fetchall()
        
        if not clients:
            query.edit_message_text("No active clients found.")
            return ConversationHandler.END
        
        message = "Select a client to create a task for:\n\n"
        keyboard = []
        
        for code, user_id in clients:
            message += f"🔑 {code} (User: {user_id})\n"
            keyboard.append([InlineKeyboardButton(f"{code}", callback_data=f"admin_create_task_{code}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="admin_panel")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ADMIN_CLIENT_SELECTION
        
    except Exception as e:
        log_event("Admin Create Task Workflow Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_edit_task(update: Update, context, task_id):
    """Edit a specific task."""
    try:
        query = update.callback_query
        
        with db_lock:
            cursor.execute("""
                SELECT t.*, c.user_id 
                FROM tasks t
                JOIN clients c ON t.client_id = c.invitation_code
                WHERE t.id = ?
            """, (task_id,))
            task_data = cursor.fetchone()
        
        if not task_data:
            query.edit_message_text("Task not found.")
            return ConversationHandler.END
        
        # Extract task details
        (task_id, name, client_id, userbot_phone, message_link, fallback_message_link,
         start_time, end_time, repetition_interval, status, folder_id, send_to_all_groups,
         last_run, total_runs, successful_runs, failed_runs, created_by, created_at,
         updated_at, template_id, config_json, user_id) = task_data
        
        context.user_data['editing_task_id'] = task_id
        
        start_time_str = format_lithuanian_time(start_time)
        end_time_str = format_lithuanian_time(end_time) if end_time else "No end time"
        interval_str = format_interval(repetition_interval)
        
        message = f"📝 Editing Task: {name}\n\n"
        message += f"Client: {client_id} (User: {user_id})\n"
        message += f"Userbot: {userbot_phone}\n"
        message += f"Status: {status}\n"
        message += f"Message Link: {message_link or 'Not set'}\n"
        message += f"Fallback Link: {fallback_message_link or 'Not set'}\n"
        message += f"Start Time: {start_time_str}\n"
        message += f"End Time: {end_time_str}\n"
        message += f"Interval: {interval_str}\n"
        message += f"Target: {'All Groups' if send_to_all_groups else f'Folder ID: {folder_id}'}\n"
        message += f"Stats: {successful_runs}/{total_runs} successful runs\n"
        
        keyboard = [
            [InlineKeyboardButton("📝 Edit Name", callback_data=f"edit_task_name_{task_id}"),
             InlineKeyboardButton("🔗 Edit Links", callback_data=f"edit_task_links_{task_id}")],
            [InlineKeyboardButton("⏰ Edit Timing", callback_data=f"edit_task_timing_{task_id}"),
             InlineKeyboardButton("🎯 Edit Targets", callback_data=f"edit_task_targets_{task_id}")],
            [InlineKeyboardButton(f"{'⏸️ Pause' if status == 'active' else '▶️ Resume'}", 
                                callback_data=f"toggle_task_status_{task_id}")],
            [InlineKeyboardButton("🗑️ Delete Task", callback_data=f"admin_delete_task_{task_id}"),
             InlineKeyboardButton("💾 Save as Template", callback_data=f"save_task_template_{task_id}")],
            [InlineKeyboardButton("⬅️ Back", callback_data=f"admin_client_{client_id}")]
        ]
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ADMIN_TASK_EDITING
        
    except Exception as e:
        log_event("Admin Edit Task Error", f"Task ID: {task_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Bulk operation handlers
def handle_bulk_pause_all(update: Update, context):
    """Pause all active tasks."""
    try:
        query = update.callback_query
        admin_id = query.from_user.id
        
        with db_lock:
            cursor.execute("UPDATE tasks SET status = 'paused' WHERE status = 'active'")
            affected_rows = cursor.rowcount
            db.commit()
        
        log_admin_action(admin_id, "BULK_PAUSE", details=f"Paused {affected_rows} tasks")
        query.edit_message_text(f"✅ Paused {affected_rows} active tasks.")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Bulk Pause Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_bulk_resume_all(update: Update, context):
    """Resume all paused tasks."""
    try:
        query = update.callback_query
        admin_id = query.from_user.id
        
        with db_lock:
            cursor.execute("UPDATE tasks SET status = 'active' WHERE status = 'paused'")
            affected_rows = cursor.rowcount
            db.commit()
        
        log_admin_action(admin_id, "BULK_RESUME", details=f"Resumed {affected_rows} tasks")
        query.edit_message_text(f"✅ Resumed {affected_rows} paused tasks.")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Bulk Resume Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_bulk_delete_completed(update: Update, context):
    """Delete all completed tasks."""
    try:
        query = update.callback_query
        admin_id = query.from_user.id
        
        with db_lock:
            cursor.execute("DELETE FROM tasks WHERE status = 'completed'")
            affected_rows = cursor.rowcount
            db.commit()
        
        log_admin_action(admin_id, "BULK_DELETE_COMPLETED", details=f"Deleted {affected_rows} completed tasks")
        query.edit_message_text(f"✅ Deleted {affected_rows} completed tasks.")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Bulk Delete Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def handle_bulk_generate_report(update: Update, context):
    """Generate a comprehensive task report."""
    try:
        query = update.callback_query
        
        with db_lock:
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    SUM(total_runs) as total_runs,
                    SUM(successful_runs) as successful_runs,
                    SUM(failed_runs) as failed_runs
                FROM tasks 
                GROUP BY status
            """)
            status_stats = cursor.fetchall()
            
            cursor.execute("""
                SELECT 
                    c.invitation_code,
                    c.user_id,
                    COUNT(t.id) as task_count,
                    SUM(t.successful_runs) as total_success
                FROM clients c
                LEFT JOIN tasks t ON c.invitation_code = t.client_id
                GROUP BY c.invitation_code, c.user_id
                ORDER BY total_success DESC
                LIMIT 10
            """)
            client_stats = cursor.fetchall()
        
        report = "📊 **System Report**\n\n"
        
        # Status overview
        report += "**Task Status Overview:**\n"
        total_tasks = 0
        for status, count, total_runs, successful_runs, failed_runs in status_stats:
            total_tasks += count
            success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
            report += f"• {status.title()}: {count} tasks (Success: {success_rate:.1f}%)\n"
        
        report += f"\n**Total Tasks:** {total_tasks}\n\n"
        
        # Top clients
        report += "**Top 10 Clients by Success:**\n"
        for code, user_id, task_count, total_success in client_stats:
            total_success = total_success or 0
            report += f"• {code}: {task_count} tasks, {total_success} successes\n"
        
        query.edit_message_text(report, parse_mode='Markdown')
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Bulk Report Error", f"Error: {e}")
        query.edit_message_text("An error occurred generating the report.")
        return ConversationHandler.END

def handle_bulk_restart_failed(update: Update, context):
    """Restart all failed tasks."""
    try:
        query = update.callback_query
        admin_id = query.from_user.id
        
        with db_lock:
            cursor.execute("UPDATE tasks SET status = 'active' WHERE status = 'failed'")
            affected_rows = cursor.rowcount
            db.commit()
        
        log_admin_action(admin_id, "BULK_RESTART_FAILED", details=f"Restarted {affected_rows} failed tasks")
        query.edit_message_text(f"✅ Restarted {affected_rows} failed tasks.")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Bulk Restart Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Template management functions
def admin_create_template(update: Update, context):
    """Create a new task template."""
    try:
        query = update.callback_query
        
        query.edit_message_text("Enter a name for the new template:")
        return WAITING_FOR_TEMPLATE_NAME
        
    except Exception as e:
        log_event("Create Template Error", f"Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def admin_use_template(update: Update, context, template_id):
    """Use a template to create a new task."""
    try:
        query = update.callback_query
        
        with db_lock:
            cursor.execute("SELECT name, config_json FROM task_templates WHERE id = ?", (template_id,))
            template_data = cursor.fetchone()
        
        if not template_data:
            query.edit_message_text("Template not found.")
            return ConversationHandler.END
        
        template_name, config_json = template_data
        context.user_data['template_config'] = json.loads(config_json) if config_json else {}
        context.user_data['template_name'] = template_name
        
        # Now ask user to select client for this template
        with db_lock:
            cursor.execute("SELECT invitation_code, user_id FROM clients WHERE status = 'active'")
            clients = cursor.fetchall()
        
        message = f"Using template: {template_name}\nSelect client to create task for:\n\n"
        keyboard = []
        
        for code, user_id in clients:
            keyboard.append([InlineKeyboardButton(f"{code} (User: {user_id})", 
                                                callback_data=f"template_client_{code}")])
        
        keyboard.append([InlineKeyboardButton("⬅️ Back to Templates", callback_data="admin_task_templates")])
        
        markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(message, reply_markup=markup)
        return ADMIN_CLIENT_SELECTION
        
    except Exception as e:
        log_event("Use Template Error", f"Template ID: {template_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

def client_setup_enhanced_tasks(update: Update, context):
    """Enhanced client task setup with better UI."""
    try:
        query = update.callback_query
        user_id = query.from_user.id
        
        with db_lock:
            cursor.execute("SELECT invitation_code, dedicated_userbots FROM clients WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            
            if not result:
                query.edit_message_text("Client account not found.")
                return ConversationHandler.END
            
            client_code, userbots_str = result
            userbot_phones = userbots_str.split(",") if userbots_str else []
            
            # Get existing tasks
            cursor.execute("""
                SELECT id, name, userbot_phone, status, last_run, successful_runs, total_runs
                FROM tasks 
                WHERE client_id = ?
                ORDER BY created_at DESC
            """, (client_code,))
            tasks = cursor.fetchall()
        
        message = f"📋 Your Tasks ({len(tasks)} total):\n\n"
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
        log_event("Client Enhanced Tasks Error", f"User: {user_id}, Error: {e}")
        query.edit_message_text("An error occurred.")
        return ConversationHandler.END

# Message handlers for text inputs
def process_template_name(update: Update, context):
    """Process template name input."""
    try:
        template_name = update.message.text.strip()
        admin_id = update.effective_user.id
        
        # For now, create a basic template - in full implementation, you'd gather more config
        config = {
            "repetition_interval": 60,
            "send_to_all_groups": 0,
            "default_message": "Template message"
        }
        
        with db_lock:
            cursor.execute("""
                INSERT INTO task_templates (name, description, config_json, created_by)
                VALUES (?, ?, ?, ?)
            """, (template_name, f"Template created by admin {admin_id}", 
                  json.dumps(config), admin_id))
            db.commit()
        
        log_admin_action(admin_id, "CREATE_TEMPLATE", details=f"Created template: {template_name}")
        update.message.reply_text(f"✅ Template '{template_name}' created successfully!")
        return ConversationHandler.END
        
    except Exception as e:
        log_event("Process Template Name Error", f"Error: {e}")
        update.message.reply_text("An error occurred creating the template.")
        return ConversationHandler.END

def handle_original_callback(update: Update, context):
    """Fallback to handle original callback functionality."""
    # This would contain the original callback handlers from your existing system
    # For now, return a placeholder
    query = update.callback_query
    query.edit_message_text("This functionality is being migrated to the enhanced system.")
    return ConversationHandler.END

# Enhanced conversation handler setup
def create_enhanced_conversation_handler():
    """Create the enhanced conversation handler with all states."""
    return ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            WAITING_FOR_CODE: [MessageHandler(Filters.text & ~Filters.command, process_invitation_code)],
            WAITING_FOR_TEMPLATE_NAME: [MessageHandler(Filters.text & ~Filters.command, process_template_name)],
            ADMIN_CLIENT_SELECTION: [CallbackQueryHandler(handle_enhanced_callback)],
            ADMIN_TASK_EDITING: [CallbackQueryHandler(handle_enhanced_callback)],
            ADMIN_BULK_OPERATIONS: [CallbackQueryHandler(handle_enhanced_callback)],
            ADMIN_TEMPLATE_MANAGEMENT: [CallbackQueryHandler(handle_enhanced_callback)],
            # Add more states as needed
        },
        fallbacks=[
            CommandHandler('start', start),
            CallbackQueryHandler(handle_enhanced_callback)
        ]
    )

# Placeholder functions that would need to be implemented
def process_invitation_code(update: Update, context):
    """Process invitation code - placeholder."""
    update.message.reply_text("Invitation code processing - implement from original system")
    return ConversationHandler.END

# Add any additional utility functions needed
def get_task_statistics():
    """Get comprehensive task statistics."""
    with db_lock:
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_tasks,
                SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) as paused_tasks,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                SUM(total_runs) as total_executions,
                SUM(successful_runs) as successful_executions
            FROM tasks
        """)
        return cursor.fetchone()

def get_client_task_summary(client_code):
    """Get task summary for a specific client."""
    with db_lock:
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_tasks,
                SUM(successful_runs) as total_success,
                SUM(failed_runs) as total_failures
            FROM tasks
            WHERE client_id = ?
        """, (client_code,))
        return cursor.fetchone()

def validate_task_config(config):
    """Validate task configuration."""
    required_fields = ['name', 'client_id', 'userbot_phone']
    for field in required_fields:
        if field not in config or not config[field]:
            return False, f"Missing required field: {field}"
    return True, "Valid configuration"

def create_task_from_template(template_id, client_code, userbot_phone, task_name):
    """Create a new task from a template."""
    try:
        with db_lock:
            cursor.execute("SELECT config_json FROM task_templates WHERE id = ?", (template_id,))
            result = cursor.fetchone()
            
            if not result:
                return False, "Template not found"
            
            config = json.loads(result[0])
            
            # Create new task with template config
            cursor.execute("""
                INSERT INTO tasks (name, client_id, userbot_phone, repetition_interval, 
                                 send_to_all_groups, template_id, config_json, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task_name, client_code, userbot_phone,
                config.get('repetition_interval', 60),
                config.get('send_to_all_groups', 0),
                template_id, json.dumps(config), 0  # System created
            ))
            db.commit()
            
            return True, "Task created successfully from template"
            
    except Exception as e:
        return False, f"Error creating task from template: {e}" 