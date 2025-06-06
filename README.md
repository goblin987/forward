# Enhanced Telegram Auto Forwarding Bot

## Overview

This is an enhanced version of your auto forwarding bot that allows admins to setup and manage auto forward tasks for multiple clients. The system has been completely rebuilt with improved admin functionality, better task management, and advanced features for running forwarding services for other people.

## Key Features

### For Admins
- **📊 System Overview Dashboard** - Real-time statistics on clients, userbots, and tasks
- **🎯 Manage Client Tasks** - Create, edit, and monitor tasks for any client
- **📋 View All Tasks** - Comprehensive view of all active tasks across all clients
- **⚡ Bulk Operations** - Pause, resume, delete, or restart multiple tasks at once
- **📝 Task Templates** - Create reusable task configurations
- **👥 Client Management** - Manage clients, subscriptions, and userbots
- **📈 Advanced Reporting** - Generate detailed performance reports
- **🔍 Audit Trail** - Track all admin actions for accountability

### For Clients
- **Enhanced Task Setup** - Improved UI for creating and managing tasks
- **📊 Task Statistics** - Success rates, run counts, and performance metrics
- **🎯 Flexible Targeting** - Send to specific folders or all groups
- **⏰ Advanced Scheduling** - Start times, end times, and intervals
- **🔄 Fallback Messages** - Backup messages if primary fails
- **📱 Multi-userbot Support** - Use multiple userbots for better performance

### System Improvements
- **Enhanced Database Schema** - New tables for better data organization
- **Improved Task Execution** - Better error handling and concurrent processing
- **Advanced Logging** - Detailed logs with client and task tracking
- **Template System** - Reusable configurations for common setups
- **Bulk Operations** - Mass management capabilities

## Installation & Setup

### Requirements
- Python 3.8+
- Telegram Bot Token
- Telegram API ID and Hash
- Admin Telegram User IDs

### Installation Steps

1. **Clone the Repository**
```bash
git clone <your-repo-url>
cd auto-forwarding-bot
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Set Environment Variables**
Create a `.env` file or set these environment variables:
```bash
export API_ID="your_telegram_api_id"
export API_HASH="your_telegram_api_hash"
export BOT_TOKEN="your_bot_token"
export ADMIN_IDS="123456789,987654321"  # Comma-separated admin user IDs
export DATA_DIR="./data"  # Optional: Custom data directory
```

4. **Run the Bot**
```bash
python enhanced_frw.py
```

## Database Schema

### Enhanced Tables

#### `tasks` (New Enhanced Table)
- **id** - Unique task identifier
- **name** - Human-readable task name
- **client_id** - Client invitation code
- **userbot_phone** - Phone number of userbot
- **message_link** - Primary message to forward
- **fallback_message_link** - Backup message if primary fails
- **start_time** - When to start the task
- **end_time** - When to stop the task (optional)
- **repetition_interval** - Minutes between repeats
- **status** - active, paused, completed, failed
- **folder_id** - Target folder (if not sending to all)
- **send_to_all_groups** - Boolean flag
- **total_runs** - Total execution count
- **successful_runs** - Successful execution count
- **failed_runs** - Failed execution count
- **created_by** - Admin who created the task
- **template_id** - Template used (if any)
- **config_json** - Additional configuration

#### `task_templates` (New)
- **id** - Template identifier
- **name** - Template name
- **description** - Template description
- **config_json** - Template configuration
- **created_by** - Admin who created it
- **is_public** - Whether template is public

#### `admin_actions` (New)
- **id** - Action identifier
- **admin_id** - Admin who performed action
- **action_type** - Type of action performed
- **target_id** - Target of the action
- **details** - Action details
- **timestamp** - When action was performed

## Admin Features Guide

### System Overview
- View total active clients, userbots, and tasks
- Monitor system health and performance
- Quick access to all management functions

### Client Task Management
1. **View All Clients** - See all clients with task counts
2. **Client Task Details** - View detailed task information for each client
3. **Create Tasks for Clients** - Set up new forwarding tasks for any client
4. **Edit Existing Tasks** - Modify task parameters, timing, and targets
5. **Delete Tasks** - Remove tasks that are no longer needed

### Bulk Operations
- **Pause All Active Tasks** - Emergency stop for all running tasks
- **Resume All Paused Tasks** - Restart all paused tasks
- **Delete Completed Tasks** - Clean up finished tasks
- **Restart Failed Tasks** - Retry tasks that failed
- **Generate System Report** - Comprehensive performance statistics

### Task Templates
1. **Create Templates** - Save common task configurations
2. **Use Templates** - Apply templates to create new tasks quickly
3. **Share Templates** - Make templates available to other admins

## Client Features Guide

### Enhanced Task Setup
1. **Create New Task** - Set up forwarding with improved wizard
2. **Edit Existing Tasks** - Modify your tasks with better interface
3. **View Task Statistics** - See success rates and performance
4. **Manage Multiple Tasks** - Handle multiple forwarding operations

### Task Configuration Options
- **Task Name** - Give your task a descriptive name
- **Primary Message** - Main message to forward
- **Fallback Message** - Backup if primary fails
- **Start Time** - When to begin forwarding
- **End Time** - When to stop (optional)
- **Repetition Interval** - How often to repeat
- **Target Groups** - Specific folders or all groups
- **Userbot Selection** - Choose which userbot to use

## API Changes from Original

### New Admin Endpoints
- `/admin_manage_client_tasks` - Manage tasks for clients
- `/admin_bulk_operations` - Perform bulk operations
- `/admin_task_templates` - Manage task templates
- `/admin_view_all_tasks` - View all system tasks

### Enhanced Client Endpoints  
- `/client_setup_enhanced_tasks` - Improved task setup
- `/client_task_statistics` - View task performance
- `/client_edit_task` - Enhanced task editing

### New Database Functions
- `execute_enhanced_task()` - Improved task execution
- `log_admin_action()` - Track admin activities
- `create_task_from_template()` - Template-based task creation
- `get_task_statistics()` - Comprehensive statistics

## Configuration Options

### Task Configuration JSON
```json
{
  "repetition_interval": 60,
  "send_to_all_groups": 0,
  "delay_between_forwards": 2,
  "max_retries": 3,
  "enable_fallback": true,
  "notification_settings": {
    "on_success": false,
    "on_failure": true
  }
}
```

### Template Configuration
```json
{
  "name": "Daily Marketing",
  "description": "Daily marketing message template",
  "default_interval": 1440,
  "suggested_targets": "marketing_folder",
  "recommended_time": "09:00"
}
```

## Usage Examples

### Admin: Create Task for Client
1. Start bot as admin: `/start`
2. Select "🎯 Manage Client Tasks"
3. Choose client from list
4. Click "➕ Create Task for Client"
5. Configure task parameters
6. Save and activate

### Admin: Bulk Operations
1. Access admin panel: `/start`
2. Select "⚡ Bulk Operations"
3. Choose operation (pause all, resume all, etc.)
4. Confirm action

### Client: Setup Enhanced Task
1. Start bot: `/start`
2. Select "Setup Tasks"
3. Click "➕ Create New Task"
4. Follow the enhanced setup wizard
5. Configure all parameters
6. Activate task

## Migration from Original System

The enhanced system automatically migrates data from the original `userbot_settings` table to the new `tasks` table. Your existing tasks will be preserved with the name format "Migrated Task - {phone_number}".

### What's Migrated
- ✅ All existing task configurations
- ✅ Client assignments
- ✅ Userbot associations
- ✅ Task timing and intervals
- ✅ Target group settings

### What's New
- ✅ Task names and descriptions
- ✅ Enhanced statistics tracking
- ✅ Template support
- ✅ Admin management capabilities
- ✅ Bulk operations
- ✅ Audit trail

## Monitoring & Maintenance

### Log Files
- **bot.log** - General bot operations
- **admin_actions.log** - Admin action audit trail
- **task_execution.log** - Task execution details

### Performance Monitoring
- Monitor task success rates
- Check userbot availability
- Review system resource usage
- Monitor database performance

### Regular Maintenance
- Clean up completed tasks
- Archive old logs
- Update userbot sessions
- Review admin actions

## Security Features

### Admin Controls
- Admin-only access to management functions
- Action logging and audit trail
- Client isolation (admins can't see client messages)
- Secure userbot session management

### Client Protection
- Clients can only manage their own tasks
- Secure invitation code system
- Protected userbot credentials
- Isolated task execution

## Troubleshooting

### Common Issues

**Tasks Not Running**
- Check userbot status
- Verify message links are valid
- Ensure target groups are accessible
- Check task timing configuration

**Admin Panel Not Accessible**
- Verify your user ID is in ADMIN_IDS
- Check environment variable configuration
- Restart the bot if needed

**Database Errors**
- Check database file permissions
- Verify disk space availability
- Review database logs

### Support
For support and bug reports, please create an issue in the repository or contact the development team.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Version History

### v2.0.0 (Enhanced Version)
- ✅ Complete admin task management system
- ✅ Bulk operations for mass management
- ✅ Task templates and reusable configurations
- ✅ Enhanced client interface
- ✅ Comprehensive statistics and reporting
- ✅ Audit trail and action logging
- ✅ Improved error handling and performance
- ✅ Migration from original system

### v1.0.0 (Original Version)
- Basic auto forwarding functionality
- Client invitation system
- Simple task setup
- Basic admin controls 