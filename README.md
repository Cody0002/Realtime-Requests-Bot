# Real-Time Data Analytics Telegram Bot Documentation

## Overview

The Real-Time Data Analytics Bot is a comprehensive Telegram bot designed for business intelligence and analytics, providing access to acquisition performance, deposit performance, and channel distribution data across multiple countries in Southeast Asia (Thailand, Philippines, Bangladesh, Pakistan, and Indonesia).

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Core Features](#core-features)
3. [User Authentication & Authorization](#user-authentication--authorization)
4. [Commands Reference](#commands-reference)
5. [Data Sources & Processing](#data-sources--processing)
6. [Configuration](#configuration)
7. [Deployment](#deployment)
8. [Error Handling](#error-handling)
9. [Logging & Monitoring](#logging--monitoring)

## System Architecture

### Components

```

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Telegram Bot  │ <────│  BigQuery API   │────│  Data Sources   │
│   (main.py)     │    │  (bq_client.py) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ^
         │
┌─────────────────┐    ┌─────────────────┐
│ Table Renderer  │    │  Configuration  │
│(table_renderer) │    │   (config.py)   │
└─────────────────┘    └─────────────────┘
```

### File Structure

```
├── bot/
│   ├── main.py              # Main bot application
│   ├── config.py            # Configuration management
│   ├── bq_client.py         # BigQuery client wrapper
│   ├── table_renderer.py    # Data visualization & table formatting
│   └── helpers.py           # Utility functions
├── sql/
│   ├── apf_function.sql     # Acquisition Performance query
│   ├── dpf_function.sql     # Deposit Performance query
│   └── dist_function.sql    # Distribution query
├── logs/                    # Runtime logs and user data
│   ├── registered_users.json
│   ├── invite_tokens.json
│   ├── group_policies.json
│   └── events-YYYYMMDD.jsonl
└── .env                     # Environment variables
```

## Core Features

### 1. Acquisition Performance Tracking (APF)
- **Purpose**: Monitor user acquisition metrics across brands and countries
- **Data Points**: NAR (New Account Registrations), FTD (First Time Deposits), STD (Second Time Deposits), TTD (Total Time Deposits)
- **Time Range**: Rolling 3-day window
- **Granularity**: Daily aggregations by country, group, and brand

### 2. Deposit Performance Tracking (DPF) 
- **Purpose**: Analyze deposit behavior and performance metrics
- **Data Points**: Average Deposit Amount, Total Deposit Volume, Weightage
- **Time Range**: Rolling 3-day window
- **Granularity**: Daily aggregations by country, group, and brand
- **Currency**: Native currency per country

### 3. Channel Distribution Analysis (DIST)
- **Purpose**: Breakdown of deposit transactions by payment methods
- **Data Points**: Transaction count, volume, average amount, percentage distribution
- **Time Range**: Specific date queries
- **Granularity**: By payment method and country

## User Authentication & Authorization

### Registration Flow

#### Pattern 1: Individual User Registration

1. Admin gives out invite link to the bot
2. User starts bot (/start or /help)
3. Commands available everywhere the user interacts with bot

#### Pattern 2: Group-Based Access Control

1. Admin creates Telegram group
2. Admin sets group policy: /permission -cmds=apf,dpf,dist
3. Users are added to the group (no individual registration needed)
4. Users immediately inherit group permissions
5. Commands only work within that specific group context

Key Advantage: Pattern 2 allows instant access management - admins can control who sees what data by simply managing group membership, without needing to create individual invite tokens.

### Permission System

#### Individual User Permissions
- **Full Access**: No restrictions (default for manual registration)
- **Limited Access**: Specific commands only (set via invite tokens)
- **No Access**: Blocked users (empty command list)

#### Group-Level Policies
- **Override**: Group policies override individual permissions
- **Admin Control**: Only admins can set group policies
- **Command Filtering**: Restrict available commands per group
- **Inheritance**: Users in groups automatically inherit group permissions (no individual registration required)
- **Immediate Access**: Users can use commands immediately upon joining groups with set policies

## Commands Reference

### User Commands

#### `/help` or `/start`
- **Purpose**: Display available commands based on user permissions
- **Context-Aware**: Shows only commands available to user in current chat
- **Dynamic**: Adapts to group policies and individual permissions

#### `/apf <country|a>`
- **Purpose**: Acquisition Performance Report
- **Parameters**:
  - `a` - All countries
  - `TH|PH|BD|PK|ID` - Specific country
- **Output**: Grouped tables by brand with NAR/FTD/STD/TTD metrics

#### `/dpf <country|a>`
- **Purpose**: Deposit Performance Report
- **Parameters**:
  - Country selector (`a` for all, or specific country code)
- **Examples**:
  - `/dpf BD`
- **Output**: Average/Total deposit amounts with performance percentages

#### `/dist <country|a> <YYYYMMDD>`
- **Purpose**: Channel Distribution for specific date
- **Parameters**:
  - Country selector (a for all, or specific country code)
  - Date in YYYYMMDD format
- **Examples**:
  - `/dist a 20260401`
- **Output**: Payment method breakdown with volumes and percentages

### Administrative Commands [Admin only]

#### `/admin_create_link [options] [note]`
- **Purpose**: Generate invite links with custom permissions
- **Options**:
  - TTL: `30d`, `24h`, `60m` (time to live)
  - Max uses: Integer or -1 for unlimited
  - `-cmds=apf,dpf` - Restrict commands
- **Example**: `/admin_create_link 7d 5 -cmds=apf,dpf New analyst access`

#### `/permission -cmds=<commands> [-chat=<id>]`
- **Purpose**: Set group-level command permissions
- **Parameters**:
  - `-cmds=apf,dpf,dist|all|none` - Command list
  - `-chat=<id>` - Target chat (optional, defaults to current)
- **Access**: Admin only

## Data Sources & Processing

### BigQuery Integration

The bot connects to Google BigQuery for data retrieval:

```python
class BigQueryClient:
    def __init__(self, config):
        self.client = bigquery.Client(
            project=config.BQ_PROJECT,
            location=config.BQ_LOCATION
        )
```

### Query Types

1. **APF Queries**: Parameterized by target country
2. **DPF Queries**: Rolling 3-day performance metrics
3. **DIST Queries**: Date-specific distribution analysis

### Data Processing Pipeline

```
Raw BigQuery Results → Data Normalization → Brand Grouping → 
Table Formatting → Markdown Rendering → Telegram Delivery
```

### Brand Normalization

Brands are normalized using these rules:
- Keep: 96G, BLG, WDB
- Default: KZO (for all others)
- Handle country-specific prefixes

## Configuration

### Environment Variables

```bash
# Required
TELEGRAM_BOT_TOKEN=your_bot_token
BQ_PROJECT=your_bigquery_project
REGISTER_LINK_SECRET=secure_random_string

# Optional
ADMIN_USER_IDS=123456789,987654321
```

### Supported Countries
- **TH**: Thailand (THB)
- **PH**: Philippines (PHP) 
- **BD**: Bangladesh (BDT)
- **PK**: Pakistan (PKR)
- **ID**: Indonesia (IDR)

## Data Visualization

### Table Formatting Features

1. **Unicode Box Drawing**: Professional table appearance
2. **Number Formatting**: Thousands separators, proper alignment
3. **Responsive Width**: Auto-adjusts to Telegram's constraints
4. **Multi-line Support**: Handles long content with wrapping
5. **Markdown V2**: Styled headers and emphasis

### Rendering Pipeline

```python
Data → TableFormatter → Unicode Tables → Markdown Escaping → 
Chunk Splitting → Telegram API
```

## Error Handling

### Graceful Degradation
- Invalid date formats return user-friendly error messages
- Missing data shows "No results" instead of crashing
- BigQuery timeouts are caught and reported
- Malformed tokens show specific validation errors

### Exception Logging
All errors are logged with full context:
```python
logger.exception("Error in /apf")
await update.message.reply_text(f"Error: `{e}`")
```

## Logging & Monitoring

### Event Logging
Daily JSONL files track all user interactions:
```json
{
  "ts": "2025-09-18T14:30:00+07:00",
  "user_id": 123456789,
  "chat_id": -987654321,
  "event": "command",
  "command": "/apf TH"
}
```

### User Management
Persistent storage for:
- **registered_users.json**: User profiles and permissions
- **invite_tokens.json**: Active invitation tokens
- **group_policies.json**: Chat-level permission overrides

### Performance Metrics
- Query execution times
- Message delivery success rates
- User engagement patterns
- Error frequencies

## Security Features

1. **Token-Based Authentication**: HMAC-signed invite tokens
2. **Permission Inheritance**: Group policies override individual settings
3. **Admin Controls**: Restricted administrative functions
4. **Input Validation**: All user inputs are sanitized
5. **Rate Limiting**: Implicit through Telegram's API limits

## Deployment

### Prerequisites
- Python 3.11+
- Google Cloud credentials with BigQuery access
- Telegram Bot Token
- Required Python packages (see requirements)

### Environment Setup
```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="your_token"
export BQ_PROJECT="your_project"
```

### Production Considerations
- Use proper logging configuration
- Set up monitoring and alerting
- Configure appropriate BigQuery quotas
- Implement backup strategies for user data
- Use environment-specific configurations

This bot provides a robust foundation for real-time business analytics via Telegram, with comprehensive user management, flexible permissions, and professional data visualization capabilities.
