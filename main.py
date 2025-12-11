import os
import logging
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from dotenv import load_dotenv

import json
from pathlib import Path

from bot.config import Config
from bot.bq_client import BigQueryClient
from bot.table_renderer import send_apf_tables, send_channel_distribution, send_dpf_tables, send_pmh_total, send_pmh_week

from bot.table_renderer import (send_provider_summaries, send_method_summaries
)

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import hmac, hashlib, base64, secrets, time  # add these
# Load environment variables
load_dotenv()

VALID_COMMANDS= {"apf", "dpf", "dist", "pmh_total", "pmh_provider", "pmh_method", "pmh_week"}
LIST_VALID_COMMANDS = ["apf", "dpf", "dist", "pmh_total", "pmh_provider", "pmh_method", "pmh_week"]
# --- Aliases ---
PMH_ALIAS = {"pmh": ["pmh_total", "pmh_provider", "pmh_method", "pmh_week"]}
def _expand_aliases(cmds: list[str]) -> list[str]:
    out = []
    for c in cmds:
        c = c.lower().strip()
        if c in PMH_ALIAS:
            out.extend(PMH_ALIAS[c])
        else:
            out.append(c)
    return out

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

def _parse_target_date(date_str: str):
    """Parse YYYYMMDD -> 'YYYY-MM-DD' string; raise on invalid."""
    dt = datetime.strptime(date_str, "%Y%m%d")  # will raise ValueError if bad
    return dt.strftime("%Y-%m-%d")

def get_date_range_header():
    """Get the current time and date range for the header."""
    now_bkk = datetime.now(ZoneInfo("Asia/Bangkok"))
    current_time = now_bkk.strftime("%H:%M")
    current_date = now_bkk.strftime("%Y-%m-%d")
    
    dates = [
        current_date,
        (now_bkk - timedelta(days=1)).strftime("%Y-%m-%d"),
        (now_bkk - timedelta(days=2)).strftime("%Y-%m-%d")
    ]
    return current_time, dates

# ---- unicode "font" converter ----
STYLES = {
    "mono":          {"A":0x1D670, "a":0x1D68A, "0":0x1D7F6},  # Mathematical Monospace
    "sans":          {"A":0x1D5A0, "a":0x1D5BA, "0":0x1D7E2},  # Sans-serif
    "sans_bold":     {"A":0x1D5D4, "a":0x1D5EE, "0":0x1D7EC},  # Sans-serif Bold
    "serif_bold":    {"A":0x1D400, "a":0x1D41A, "0":0x1D7CE},  # Bold
    "serif_italic":  {"A":0x1D434, "a":0x1D44E, "0":None},     # Italic (no special digits)
    "serif_bi":      {"A":0x1D468, "a":0x1D482, "0":None},     # Bold Italic
    "fullwidth":     {"A":0xFF21,  "a":0xFF41,  "0":0xFF10, "space":0x3000},  # Ｆｕｌｌｗｉｄｔｈ
}

def stylize(text: str, style: str = "mono") -> str:
    spec = STYLES.get(style)
    if not spec:
        raise ValueError(f"Unknown style '{style}'. Try: {', '.join(STYLES)}")

    out = []
    for ch in text:
        o = ch
        if "A" <= ch <= "Z" and spec.get("A") is not None:
            o = chr(spec["A"] + (ord(ch) - ord("A")))
        elif "a" <= ch <= "z" and spec.get("a") is not None:
            o = chr(spec["a"] + (ord(ch) - ord("a")))
        elif "0" <= ch <= "9" and spec.get("0") is not None:
            o = chr(spec["0"] + (ord(ch) - ord("0")))
        elif ch == " " and style == "fullwidth" and "space" in spec:
            o = chr(spec["space"])  # optional fullwidth space
        out.append(o)
    return "".join(out)

class RealTimeBot:
    def __init__(self):
        self.config = Config()
        self.bq_client = BigQueryClient(self.config)

        base_dir = Path(__file__).resolve().parent

        self.logs_dir = base_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)

        self.registered_file = self.logs_dir / "registered_users.json"
        self.registered_users = self._load_registered_users()
        
        # Initialize invite tokens
        self.tokens_file = self.logs_dir / "invite_tokens.json"
        self.invite_tokens = self._load_invite_tokens()
        self.register_secret = os.getenv("REGISTER_LINK_SECRET", "change_me_now")

        # Admins (comma-separated user IDs in env)
        raw_admins = os.getenv("ADMIN_USER_IDS", "")
        self.admin_user_ids = {int(x) for x in raw_admins.replace(" ", "").split(",") if x.strip().isdigit()}

        self.group_policies_file = self.logs_dir / "group_policies.json"
        self.group_policies = self._load_group_policies()  # { chat_id(str): {"allowed_commands":[...], "set_by": int, "ts": iso } }


        logger.info("registered_file path: %s", self.registered_file.resolve())
        logger.info("invite_tokens path:   %s", self.tokens_file.resolve())

    def _visible_commands_for_chat(self, update: Update) -> list[str]:
        """
        Return the list of commands visible in the current context.
        - If chat has a group policy -> use that (applies to everyone in the chat).
        - Else -> fall back to per-user allowed commands (None => all).
        """
        chat = update.effective_chat
        user = update.effective_user
        all_cmds = LIST_VALID_COMMANDS

        # Group policy?
        if chat and chat.type in ("group", "supergroup"):
            policy = self.group_policies.get(str(chat.id))
            if policy is not None:
                allowed = policy.get("allowed_commands") or []
                # normalize and filter
                allowed_norm = [c for c in all_cmds if c in set(x.lower() for x in allowed)]
                return allowed_norm

        # No group policy -> per-user
        allowed_user = self._user_allowed_commands(int(user.id) if user else -1)  # None => full
        if allowed_user is None:
            return all_cmds
        return [c for c in all_cmds if c in allowed_user]
    
    def _load_group_policies(self) -> dict:
        try:
            if self.group_policies_file.exists():
                with self.group_policies_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
        except Exception:
            logger.exception("Failed to load group_policies.json; starting empty")
        return {}

    def _save_group_policies(self) -> None:
        try:
            with self.group_policies_file.open("w", encoding="utf-8") as f:
                json.dump(self.group_policies, f, ensure_ascii=False, indent=2)
        except Exception:
            logger.exception("Failed to save group_policies.json")
    
    async def _ensure_allowed(self, update: Update, cmd: str) -> bool:
        """
        Returns True if user can run `cmd` in this chat.
        
        1. Private Chats: Only ADMINS are allowed.
        2. Groups: Enforce Group Policy first, then User Whitelist.
        """
        user = update.effective_user
        chat = update.effective_chat
        msg = update.effective_message

        # --- NEW: Block non-admins in Private Chats (DM) ---
        if chat.type in ("private", "PRIVATE"):
            print("PRIVATE")
            if not self._is_admin(update):
                if msg:
                    await msg.reply_text("⛔ Access denied. Please contact an administrator for permission.")
                return False
            # If Admin in DM, allow everything (or you can apply specific logic here)
            return True
        # ---------------------------------------------------

        cmd = cmd.lower()

        # 1) If the chat is a group/supergroup and has a policy, enforce it for everyone.
        if chat.type in ("group", "supergroup"):
            policy = self.group_policies.get(str(chat.id))
            if policy is not None:
                allowed = set(c.lower() for c in (policy.get("allowed_commands") or []))
                if cmd in allowed:
                    return True
                # Group policy denies
                if msg:
                    await msg.reply_text(
                        "⛔ This command is disabled in this group.\n"
                        f"Allowed here: /{', /'.join(sorted(allowed))}" if allowed else "No commands enabled here."
                    )
                return False

        # 2) No group policy → check per-user whitelist (None = full)
        allowed_user = self._user_allowed_commands(user.id)
        if allowed_user is None or cmd in allowed_user:
            return True

        if msg:
            await msg.reply_text(
                "⛔ This command is not enabled for you.\n"
                f"Allowed: /{', /'.join(sorted(allowed_user)) if allowed_user else 'all'}\n"
                f"Ask an admin for permission."
            )
        return False

    def _load_invite_tokens(self) -> dict:
        """Load invite tokens from logs/invite_tokens.json."""
        try:
            if self.tokens_file.exists():
                with self.tokens_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("tokens", {})
        except Exception:
            logger.exception("Failed to load invite_tokens.json; starting fresh")
        return {}

    def _save_invite_tokens(self) -> None:
        """Save invite tokens to logs/invite_tokens.json."""
        try:
            with self.tokens_file.open("w", encoding="utf-8") as f:
                json.dump({"tokens": self.invite_tokens}, f, ensure_ascii=False, indent=2)
        except Exception:
            logger.exception("Failed to save invite_tokens.json")

    def validate_invite_token(self, token: str) -> tuple[bool, str, dict | None]:
        """
        Validate an invite token and return (is_valid, message, token_data).
        If valid, increments usage count and saves tokens.
        
        Returns:
            (True, "success", token_data) if valid
            (False, error_message, None) if invalid
        """
        try:
            # Decode the token
            def _b64url_decode(s: str) -> bytes:
                # Add padding if needed
                padding = 4 - (len(s) % 4)
                if padding != 4:
                    s += "=" * padding
                return base64.urlsafe_b64decode(s.encode())

            def _verify_signature(secret: str, msg: bytes, sig: str) -> bool:
                expected = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()[:16]
                return hmac.compare_digest(expected, sig)

            # Decode token
            try:
                raw = _b64url_decode(token)
                decoded = raw.decode()
            except Exception:
                return False, "Invalid token format", None

            # Parse token components
            parts = decoded.split(":")
            if len(parts) != 3:
                return False, "Malformed token", None

            invite_id, exp_str, sig = parts

            # Verify signature
            msg = f"{invite_id}:{exp_str}".encode()
            if not _verify_signature(self.register_secret, msg, sig):
                return False, "Invalid token signature", None

            # Check if token exists in our records
            if invite_id not in self.invite_tokens:
                return False, "Token not found", None

            token_data = self.invite_tokens[invite_id]

            # Check if revoked
            if token_data.get("revoked", False):
                return False, "Token has been revoked", None

            # Check expiry
            exp_time = int(exp_str)
            if time.time() > exp_time:
                return False, "Token has expired", None

            # Check usage limits
            max_uses = token_data.get("max_uses", -1)
            current_uses = token_data.get("uses", 0)
            
            if max_uses != -1 and current_uses >= max_uses:
                return False, "Token usage limit reached", None

            # Token is valid - increment usage
            token_data["uses"] = current_uses + 1
            self._save_invite_tokens()

            return True, "Valid token", token_data

        except Exception as e:
            logger.exception("Error validating invite token")
            return False, f"Token validation error: {str(e)}", None

    def create_invite_link(
        self,
        *,
        bot_username: str,
        ttl_seconds: int = 30 * 24 * 60 * 60,  # 1 month default
        max_uses: int = -1,                     # -1 = unlimited until expiry
        note: str | None = None,
        allowed_commands: list[str] | None = None,   # <--- NEW
    ) -> str:
        """
        Create a signed deep-link invite usable at:
        https://t.me/<bot_username>?start=<token>

        Persists server-side state in logs/invite_tokens.json:
        {
            "tokens": {
            "<invite_id>": {
                "exp": <unix_ts>,
                "max_uses": -1|int,
                "uses": 0,
                "note": "...",
                "revoked": false,
                "created_at": "ISO-8601"
            },
            ...
            }
        }

        Return: the deep-link URL (string).
        """

        # ---- local helpers (no external deps required) ----
        def _b64url(b: bytes) -> str:
            return base64.urlsafe_b64encode(b).decode().rstrip("=")

        def _sign(secret: str, msg: bytes) -> str:
            return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()[:16]

        # ---- build signed token ----
        invite_id = secrets.token_hex(8)  # 16-hex id
        exp = int(time.time()) + int(ttl_seconds)
        msg = f"{invite_id}:{exp}".encode()
        sig = _sign(self.register_secret, msg)
        raw = f"{invite_id}:{exp}:{sig}".encode()
        token = _b64url(raw)

        # ---- persist server-side metadata ----
        self.invite_tokens[invite_id] = {
            "exp": int(exp),
            "max_uses": int(max_uses),
            "uses": 0,
            "note": note,
            "revoked": False,
            "created_at": datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
            "allowed_commands": sorted(set(allowed_commands or [])),  # <--- persist
        }
        self._save_invite_tokens()

        # ---- return deep link ----
        return f"https://t.me/{bot_username}?start={token}"

    def _is_admin(self, update: Update) -> bool:
        u = update.effective_user
        return bool(u and int(u.id) in self.admin_user_ids)

    async def admin_create_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Admin guard
        logger.info("Admin create link requested by user_id=%s", update.effective_user.id if update.effective_user else "unknown")
        if not self._is_admin(update):
            return await update.effective_chat.send_message("⚠️ You are not authorized to create invite links.")

        def _parse_ttl(s: str) -> int:
            s = s.lower().strip()
            if s.endswith("m"): return int(s[:-1]) * 60
            if s.endswith("h"): return int(s[:-1]) * 60 * 60
            if s.endswith("d"): return int(s[:-1]) * 24 * 60 * 60
            if s.endswith("mo"): return int(s[:-2]) * 30 * 24 * 60 * 60
            return int(s)  # seconds

        # Defaults
        ttl_seconds = 30 * 24 * 60 * 60
        max_uses = -1
        note: str | None = None
        allowed_commands: list[str] | None = None   # None => FULL access

        # --- Parse args robustly ---
        args = list(context.args or [])

        # 1) Pull out flags first (so they don't end up in note)
        remaining: list[str] = []
        
        for a in args:
            if a.startswith("-cmds="):
                cmds_raw = a.split("=", 1)[1]
                cmds_in = [x.strip().lower() for x in cmds_raw.split(",") if x.strip()]

                # Expand aliases (pmh -> pmh_total, pmh_provider, pmh_method)
                cmds_expanded = _expand_aliases(cmds_in)

                # Validate against known commands after expansion
                valid = VALID_COMMANDS  # only the concrete commands
                bad = [c for c in cmds_expanded if c not in valid]
                if bad:
                    return await update.effective_chat.send_message(
                        "⚠️ Unknown command(s): " + ", ".join(bad) +
                        "\nAllowed: /apf, /dpf, /dist, /pmh_total, /pmh_provider, /pmh_method, or use 'pmh' shortcut"
                    )

                allowed_commands = cmds_expanded if cmds_expanded else []  # [] means block all
            else:
                remaining.append(a)

        # 2) Parse positionals: [TTL] [max_uses] [note...]
        try:
            if remaining:
                ttl_seconds = _parse_ttl(remaining[0])
            if len(remaining) >= 2 and remaining[1].lstrip("-").isdigit():
                max_uses = int(remaining[1])
                if max_uses == 0:
                    max_uses = -1
            if len(remaining) >= 3:
                note = " ".join(remaining[2:])
            elif len(remaining) == 2 and not remaining[1].lstrip("-").isdigit():
                # If 2nd arg isn't an int, treat it as part of note
                note = remaining[1]
        except Exception:
            # fall back: treat all remaining as note
            note = " ".join(remaining) if remaining else None

        bot_username = context.bot.username

        # Create link
        link = self.create_invite_link(
            bot_username=bot_username,
            ttl_seconds=ttl_seconds,
            max_uses=max_uses,
            note=note,
            allowed_commands=allowed_commands,
        )

        cmds_txt = (
            "all"
            if allowed_commands is None
            else (", ".join(sorted(set(allowed_commands))) if allowed_commands else "(no commands)")
        )

        # Log what we parsed (helps debug)
        logger.info("invite_link args parsed: ttl=%s, max_uses=%s, cmds=%s, note=%r",
                    ttl_seconds, max_uses, allowed_commands, note)

        return await update.effective_chat.send_message(
            "🔗 Invite link created:\n"
            f"{link}\n\n"
            f"⏳ Expires in ~{ttl_seconds//3600}h\n"
            f"👥 Uses: {'unlimited' if max_uses == -1 else max_uses}\n"
            f"🛂 Allowed commands: {cmds_txt}\n"
            f"📝 {note or '(no note)'}"
        )

    def _load_registered_users(self) -> dict[int, dict]:
        """
        Load registered users from logs/registered_users.json.
        Supports old schema: {"user_ids": [int, ...]}
        New schema: {"users": [{"user_id": int, "username": str|None, "first_name": str|None,
                                "last_name": str|None, "ts": iso8601}, ...]}
        Returns in-memory mapping: { user_id: {username, first_name, last_name, ts} }
        """
        try:
            if self.registered_file.exists():
                with self.registered_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                # New schema
                if isinstance(data, dict) and "users" in data and isinstance(data["users"], list):
                    users = {}
                    for u in data["users"]:
                        uid = u.get("user_id")
                        if isinstance(uid, int):
                            users[uid] = {
                                "username":   u.get("username"),
                                "first_name": u.get("first_name"),
                                "last_name":  u.get("last_name"),
                                "ts":         u.get("ts"),
                                "allowed_commands": u.get("allowed_commands"),  # <-- KEEP
                            }
                    return users

                # Old schema (backward compatible): {"user_ids": [...]}
                if isinstance(data, dict) and "user_ids" in data and isinstance(data["user_ids"], list):
                    return {int(uid): {"username": None, "first_name": None, "last_name": None, "ts": None}
                            for uid in data["user_ids"]}

        except Exception:
            logger.exception("Failed to load registered_users.json")
        return {}

    def _save_registered_users(self) -> None:
        """
        Save in new schema: {"users": [ ... ]}, stable, human-readable ordering.
        """
        try:
            out = {
                "users": [
                    {
                        "user_id":   int(uid),
                        "username":  info.get("username"),
                        "first_name": info.get("first_name"),
                        "last_name":  info.get("last_name"),
                        "ts":         info.get("ts"),
                        "allowed_commands": info.get("allowed_commands"),  # <-- SAVE

                    }
                    for uid, info in sorted(self.registered_users.items(), key=lambda kv: kv[0])
                ]
            }
            with self.registered_file.open("w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
        except Exception:
            logger.exception("Failed to save registered_users.json")
    
    def _user_allowed_commands(self, user_id: int) -> set[str] | None:
        """
        Returns a set of allowed commands (lowercase) or None for full access.
        """
        info = self.registered_users.get(int(user_id))
        if not info:
            return None
        allowed = info.get("allowed_commands")
        if not allowed:
            return None  # None => full access
        return {c.lower() for c in allowed}

    # async def _ensure_allowed(self, update: Update, cmd: str) -> bool:
    #     """
    #     Guard: returns True if user can run `cmd`; otherwise replies a warning and returns False.
    #     `cmd` should be lowercase without leading slash, e.g., 'dpf', 'dist', 'apf'.
    #     """
    #     user = update.effective_user
    #     if not user:
    #         await update.effective_chat.send_message("⚠️ Cannot identify user.")
    #         return False

    #     allowed = self._user_allowed_commands(user.id)
    #     if allowed is None or cmd.lower() in allowed:
    #         return True

    #     await update.effective_chat.send_message(
    #         f"⛔ This command is not enabled for you.\n"
    #         f"Allowed: /{', '.join(sorted(allowed)) if allowed else 'all'}\n"
    #         f"Ask an admin for the permission."
    #     )
    #     return False

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):

        if not await self._ensure_allowed(update, "start"):
            return
        
        user = update.effective_user
        if not user:
            return await update.effective_chat.send_message("⚠️ Could not identify user.")

        uid = int(user.id)
        logger.info("/start args=%r user_id=%s", context.args, uid)

        # If there is a token, validate it FIRST (even if user exists)
        token = context.args[0] if (context.args and len(context.args) > 0) else None
        if token:
            is_valid, message, token_data = self.validate_invite_token(token)
            if not is_valid:
                return await update.effective_chat.send_message(f"⚠️ Registration failed: {message}")

            allowed = token_data.get("allowed_commands", None)

            # upsert user and update allowed_commands
            rec = self.registered_users.get(uid, {})
            existing_allowed = rec.get("allowed_commands", None)
            
            if existing_allowed == []:
                keep_allowed = existing_allowed   # preserve []
            else:
                keep_allowed = allowed # may be None or a list

            rec.update({
                "username":   getattr(user, "username", None),
                "first_name": getattr(user, "first_name", None),
                "last_name":  getattr(user, "last_name", None),
                "ts":         rec.get("ts") or datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
                "registered_via": rec.get("registered_via") or "invite_token",
                "invite_note": token_data.get("note"),
                "allowed_commands": keep_allowed,   # <-- preserve exactly (None/list/[])
            })
            self.registered_users[uid] = rec
            self._save_registered_users()
            logger.info("User %s saved with allowed=%r", uid, allowed)

            return await update.effective_chat.send_message(
                "✅ Invite accepted.\n"
                f"🛂 Allowed commands: "
                f"{'all' if allowed is None else (', '.join(allowed) if allowed else 'All')}\n"
                "Type /help to see available commands."
            )

        # No token supplied
        if uid in self.registered_users:
            return await self.help_command(update, context)

        # New manual registration flow (no token)
        self.registered_users[uid] = {
            "username":   getattr(user, "username", None),
            "first_name": getattr(user, "first_name", None),
            "last_name":  getattr(user, "last_name", None),
            "ts":         datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
            "registered_via": "manual",
            # no allowed_commands field -> full access
        }
        self._save_registered_users()
        return await update.effective_chat.send_message("🎉 Registered. Type /help to begin.")

    async def register_now(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Manual registration command (kept for backward compatibility)"""
        user = update.effective_user
        if not user:
            return await update.effective_chat.send_message("⚠️ Could not identify user.")

        uid = int(user.id)
        if uid in self.registered_users:
            # Optional: refresh profile fields in case user changed username/name
            self.registered_users[uid].update({
                "username":   getattr(user, "username", None),
                "first_name": getattr(user, "first_name", None),
                "last_name":  getattr(user, "last_name", None),
                "ts":         self.registered_users[uid].get("ts") or datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
            })
            self._save_registered_users()
            return await update.effective_chat.send_message("✅ You are already registered!")

        # New registration
        self.registered_users[uid] = {
            "username":   getattr(user, "username", None),
            "first_name": getattr(user, "first_name", None),
            "last_name":  getattr(user, "last_name", None),
            "ts":         datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
            "registered_via": "manual",
        }
        self._save_registered_users()

        await update.effective_chat.send_message(
            f"🎉 Welcome, {user.first_name or 'User'}! You are now registered.\n"
            "Please get started by typing in `/help`."
        )

    def _log_event(self, payload: dict):
        try:
            now_bkk = datetime.now(ZoneInfo("Asia/Bangkok"))
            day = now_bkk.strftime("%Y%m%d")
            log_path = self.logs_dir / f"events-{day}.jsonl"

            # ensure timestamp in GMT+7
            payload = dict(payload)  # avoid mutating caller's dict
            payload.setdefault("ts", now_bkk.isoformat())

            # make sure folder exists (defensive)
            self.logs_dir.mkdir(parents=True, exist_ok=True)

            with log_path.open("a", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
                f.write("\n")

            logger.info("Logged event to %s", log_path)
        except Exception:
            logger.exception("Failed to write log event")

    async def echo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.warning("🚀 echo() triggered!")   # WARNING always shows up
        if update.effective_message:
            logger.warning("Text received: %s", update.effective_message.text)
        else:
            logger.warning("No text in message")
        msg = update.effective_message
        user = update.effective_user
        chat = update.effective_chat

        # Only process if it's text (this handler is already text-only, but be safe)
        text = msg.text if msg else None
        if text is None:
            return

        payload = {
            "user_id": user.id if user else None,
            "chat_id": chat.id if chat else None,
            "chat_type": chat.type if chat else None,
            "text": text,
        }
        logger.info("MSG %s", payload)
        self._log_event(payload)
        
    # async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):

        if not await self._ensure_allowed(update, "apf"):
            return
        user = update.effective_user

        # Command catalog (titles + usage)
        catalog = {
            "dist": {
                "title": "Deposit Channel Distribution (Specific date)",
                "lines": [
                    "`/dist a <YYYYMMDD>`: all countries",
                    "`/dist <COUNTRY> <YYYYMMDD>`: e.g., `/dist TH 20250901`",
                ],
            },
            "apf": {
                "title": "Acquisition Performance (Rolling 3 days)",
                "lines": [
                    "`/apf a`: all countries",
                    "`/apf <COUNTRY>`: e.g., `/apf TH`",
                ],
            },
            "dpf": {
                "title": "Deposit Performance (Rolling 3 days)",
                "lines": [
                    "`/dpf a`: all countries",
                    "`/dpf <COUNTRY>`: e.g., `/dpf PH`",
                ],
            },
            # ✅ Add these two
            "pmh_total": {
                "title": "Payment Health — Total (Specific date)",
                "lines": [
                    "`/pmh_total A <YYYYMMDD>`",
                    "`/pmh_total <COUNTRY> <YYYYMMDD>`",
                ],
            },
            "pmh_provider": {
                "title": "Payment Health — Providers (Specific date)",
                "lines": [
                    "`/pmh_provider A <YYYYMMDD>`",
                    "`/pmh_provider <COUNTRY> <YYYYMMDD>`",
                ],
            },
             "pmh_method": {
                "title": "Payment Health — Methods (Specific date)",
                "lines": [
                    "`/pmh_method A <YYYYMMDD>`",
                    "`/pmh_method <COUNTRY> <YYYYMMDD>`",
                ],
            },
        }

        # 🔑 Determine visibility by chat context
        visible_cmds = self._visible_commands_for_chat(update)

        if not visible_cmds:
            return await update.effective_chat.send_message(
                "🔒 No commands are enabled in this chat. Please contact an admin."
            )

        # Build help text
        parts = [stylize("🤖 *REALTIME REPORT BOT*\n", style="sans_bold")]


        # 1) Normal sections (skip pmh_* to avoid duplicates)
        for cmd in visible_cmds:
            # ✅ FIX 1: Correctly skip only pmh_ commands
            if cmd.startswith("pmh_"):
                continue
            
            # Make sure the command exists in the catalog before trying to access it
            if cmd in catalog:
                section = catalog[cmd]
                parts.append(f"*{section['title']}*")
                for line in section["lines"]:
                    parts.append(f"• {line}")
                parts.append("") # blank line

        # 2) Combined Payment Health block (only if at least one is visible)
        show_pmh_total = "pmh_total" in visible_cmds
        show_pmh_provider = "pmh_provider" in visible_cmds
        show_pmh_method = "pmh_method" in visible_cmds

        # ✅ FIX 2: Include pmh_method in the check
        if show_pmh_total or show_pmh_provider or show_pmh_method:
            parts.append("*Payment Health Report (Specific date)*")
            if show_pmh_total:
                parts.append(" • *Total*")
                parts.append("   `/pmh_total a <YYYYMMDD>`")
                parts.append("   `/pmh_total <COUNTRY> <YYYYMMDD>`")

            if show_pmh_provider:
                parts.append(" • *Providers*")
                parts.append("   `/pmh_provider a <YYYYMMDD>`")
                parts.append("   `/pmh_provider <COUNTRY> <YYYYMMDD>`")

            if show_pmh_method:
                parts.append(" • *Method*")
                parts.append("   `/pmh_method a <YYYYMMDD>`")
                parts.append("   `/pmh_method <COUNTRY> <YYYYMMDD>`")
            parts.append("") # blank line

        # Common footer
        parts.append("*📍 Supported Countries:* TH, PH, BD, PK, BR, MX")
        parts.append("*🕒 Timezone:* GMT+7\n")
        parts.append("_Please reduce your font size if the table appears misaligned_")

        text = "\n".join(parts)
        await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


    async def apf_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        def normalize_brand(b):
            KEEP_BRANDS = {"96G", "BLG", "WDB"}
            s = "" if b is None else str(b).strip()
            s = s.rstrip("12").upper()      # optional: drop trailing 1/2, normalize case
            return s if s in KEEP_BRANDS else "KZO"
        
        if not await self._ensure_allowed(update, "apf"):
            return

        try:
            if not context.args:
                return await update.effective_chat.send_message(
                    "Please type the correct function: `/apf a` or `/apf <COUNTRY>` (TH, PH, BD, PK)",
                    parse_mode=ParseMode.MARKDOWN,
                )

            sel = context.args[0].upper().strip()
            if sel == "A":
                selected_country = None
                scope_label = "all countries"
            else:
                if sel not in self.config.APF_ALLOWED:
                    return await update.effective_chat.send_message(
                        f"❌ Unsupported country `{sel}`. Allowed: {', '.join(sorted(self.config.APF_ALLOWED))}"
                    )
                selected_country = sel
                scope_label = sel

            rows = await self.bq_client.execute_apf_query(selected_country)
            if not rows:
                return await update.effective_chat.send_message(f"No data for {scope_label}.")

            country_groups = {}
            for row in rows:
                row["group"] = str(row["group"]).replace("PH96G1", "96G1")\
                        .replace("PHBLG", "BLG")\
                        .replace("1", "").replace("2", "")\
                        .replace("KZG", "KZO").replace("PHK", "KZO").replace("IDK", "KZO").replace("PKK", "KZO")
                
                row["group"] = normalize_brand(row["group"])
                country = row.get("country", "Unknown")
                if country not in country_groups:
                    country_groups[country] = []
                country_groups[country].append(row)

            current_time, date_range = get_date_range_header()
            header_text = (
                f"📊 *Acquisition Summary* \n"
                f"⏰ Data up to {current_time} (GMT+7) for each day \n"
                f"📅 Date range: {date_range[2]} → {date_range[0]}"
            )
            # await update.effective_chat.send_message(header_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
            await send_apf_tables(update, country_groups, max_width=52, max_length=2400)

        except Exception as e:
            logger.exception("Error in /apf")
            await update.effective_chat.send_message(
                f"Error: `{e}`\nLocation: {self.config.BQ_LOCATION}", 
                parse_mode=ParseMode.MARKDOWN
            )
    # --- Shared Core Function ---
    async def _pmh_command_core(self, update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str):
        """
        Core logic for PMH commands (total/provider/method).
        mode: 'total', 'provider', or 'method'
        """
        if not await self._ensure_allowed(update, f"pmh_{mode}"):
            return

        try:
            if len(context.args) < 2:
                return await update.effective_chat.send_message(
                    f"Usage:\n`/pmh_{mode} <COUNTRY/A> <YYYYMMDD>`",
                    parse_mode=ParseMode.MARKDOWN
                )

            selector = context.args[0].upper().strip()
            date_str = context.args[1].strip()

            try:
                target_date = _parse_target_date(date_str)
            except ValueError:
                return await update.effective_chat.send_message("❌ Invalid date format. Use `YYYYMMDD`.")

            if selector == "A":
                countries_to_process = self.config.APF_ALLOWED
            else:
                if selector not in self.config.APF_ALLOWED:
                    return await update.effective_chat.send_message(f"❌ Unsupported country: `{selector}`.")
                countries_to_process = [selector]

            for country_code in countries_to_process:
                # --- SIMPLIFIED: Always call the same query function ---
                rows = await self.bq_client.execute_pmh_query(target_date, country_code)
                
                if not rows:
                    await update.effective_chat.send_message(f"ℹ️ No data found for {country_code} on {target_date}.")
                    continue

                df = pd.DataFrame(rows)
                if mode == "total":
                    await send_pmh_total(update, df, target_date)
                elif mode == "provider":
                    await send_provider_summaries(update, df, target_date)
                elif mode == "method":
                    await send_method_summaries(update, df, target_date)

        except Exception as e:
            logger.exception(f"Error in /pmh_{mode}")
            await update.effective_chat.send_message(
                f"An error occurred in /pmh_{mode}: `{e}`", parse_mode=ParseMode.MARKDOWN
            )


    # --- Individual Commands ---
    async def pmh_total_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles /pmh_total command."""
        await self._pmh_command_core(update, context, mode="total")

    # Add a command method inside your BotApp class:
    async def pmh_week_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        /pmh_week <COUNTRY/A> <YYYYMMDD>
        Aggregates Mon -> as-of day (GMT+7). Also shows +/- % vs last week same span.
        """
        if not await self._ensure_allowed(update, "pmh_total"):  # reuse same permission bucket
            return
        try:
            if len(context.args) < 2:
                return await update.effective_chat.send_message(
                    "Usage:\n`/pmh_week <COUNTRY/A> <YYYYMMDD>`",
                    parse_mode=ParseMode.MARKDOWN
                )

            selector = context.args[0].upper().strip()
            date_str = context.args[1].strip()
            try:
                as_of_date = _parse_target_date(date_str)  # 'YYYY-MM-DD'
            except ValueError:
                return await update.effective_chat.send_message("❌ Invalid date format. Use `YYYYMMDD`.")

            if selector == "A":
                countries_to_process = self.config.APF_ALLOWED
            else:
                if selector not in self.config.APF_ALLOWED:
                    return await update.effective_chat.send_message(f"❌ Unsupported country: `{selector}`.")
                countries_to_process = [selector]

            for country_code in countries_to_process:
                rows = await self.bq_client.execute_pmh_week_query(as_of_date, country_code)
                if not rows:
                    await update.effective_chat.send_message(
                        f"ℹ️ No weekly data for {country_code} up to {as_of_date}."
                    )
                    continue

                df = pd.DataFrame(rows)
                await send_pmh_week(update, df, as_of_date)

        except Exception as e:
            logger.exception("Error in /pmh_week")
            await update.effective_chat.send_message(
                f"An error occurred in /pmh_week: `{e}`",
                parse_mode=ParseMode.MARKDOWN
            )

    async def pmh_provider_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles /pmh_provider command."""
        await self._pmh_command_core(update, context, mode="provider")

    async def pmh_method_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles /pmh_provider command."""
        await self._pmh_command_core(update, context, mode="method")

    async def dist_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        /dist: exact-date distribution in NATIVE currency.
        Usage:
          /dist a 20250901
          /dist TH 20250901
        """
        # user = update.effective_user
        # if not user or user.id not in self.registered_users:
        #     return await update.effective_chat.send_message("⚠️ Please register first by contacting the admin.")
        
        if not await self._ensure_allowed(update, "dist"):
            return
        # self._log_event({
        # **self._base_payload(update),
        # "event": "command",
        # "command": update.effective_message.text,   # logs "/help" or "/start"
        # })
        try:
            if len(context.args) < 2:
                return await update.effective_chat.send_message(
                    "Usage: `/dist a <YYYYMMDD>` or `/dist <COUNTRY> <YYYYMMDD>`",
                    parse_mode=ParseMode.MARKDOWN
                )

            selector = context.args[0].upper().strip()
            date_str = context.args[1].strip()

            # Parse target date
            try:
                target_date = _parse_target_date(date_str)  # 'YYYY-MM-DD'
            except ValueError:
                return await update.effective_chat.send_message(
                    "❌ Invalid date format. Use `YYYYMMDD` (e.g., `20250901`).",
                    parse_mode=ParseMode.MARKDOWN
                )

            # Country filter
            if selector == "A":
                selected_country_value = None
                target_label = "all countries"
            else:
                if selector not in self.config.APF_ALLOWED:
                    return await update.effective_chat.send_message(
                        f"❌ Unsupported country `{selector}`. "
                        f"Allowed: {', '.join(sorted(self.config.APF_ALLOWED))}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                selected_country_value = selector
                target_label = selector

            # Query BQ: exact date + native currency
            rows = await self.bq_client.execute_dist_query(target_date, selected_country_value)

            if not rows:
                return await update.effective_chat.send_message(
                    f"No results for {target_label} on {target_date}.",
                    parse_mode=ParseMode.MARKDOWN
                )

            # Group rows by country
            country_groups: dict[str, list[dict]] = {}
            for r in rows:
                c = (r.get("country") or "") or "Unknown"
                country_groups.setdefault(c, []).append(r)

            # Header
            header_text = (
                f"📊 *Deposit Channel Distribution* \n"
                f"⏰ Date: {target_date}\n"

            )
            await update.effective_chat.send_message(header_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

            # Render (table_renderer handles native currency keys)
            await send_channel_distribution(update, country_groups, max_width=72)

        except Exception as e:
            logging.exception("Error in /dist")
            await update.effective_chat.send_message(
                f"Error: `{e}`\nLocation: {self.config.BQ_LOCATION}",
                parse_mode=ParseMode.MARKDOWN
            )

    async def dpf_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        def normalize_brand(b):
            KEEP_BRANDS = {"96G", "BLG", "WDB"}
            s = "" if b is None else str(b).strip()
            s = s.rstrip("12").upper()      # optional: drop trailing 1/2, normalize case
            return s if s in KEEP_BRANDS else "KZO"
        # self._log_event({
        # **self._base_payload(update),
        # "event": "command",
        # "command": update.effective_message.text,   # logs "/help" or "/start"
        # })

        # user = update.effective_user
        # if not user or user.id not in self.registered_users:
        #     return await update.effective_chat.send_message("⚠️ Please register first by contacting the admin.")
        
        if not await self._ensure_allowed(update, "dpf"):
            return
        
        try:
            if not context.args:
                return await update.effective_chat.send_message(
                    "Usage: `/dpf a` or `/dpf <COUNTRY>` (TH, PH, BD, PK, BR, MX)",
                    parse_mode=ParseMode.MARKDOWN,
                )

            sel = context.args[0].upper().strip()
            if sel == "A":
                selected_country = None
                scope_label = "all countries"
            else:
                if sel not in self.config.APF_ALLOWED:
                    return await update.effective_chat.send_message(
                        f"❌ Unsupported country `{sel}`. Allowed: {', '.join(sorted(self.config.APF_ALLOWED))}",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                selected_country = sel
                scope_label = sel

            rows = await self.bq_client.execute_dpf_query(selected_country)
            if not rows:
                return await update.effective_chat.send_message(f"No deposit data for {scope_label}.")

            country_groups: dict[str, list[dict]] = {}
            for r in rows:
                r["group"] = str(r["group"]).replace("PH96G1", "96G1")\
                        .replace("PHBLG", "BLG")\
                        .replace("1", "").replace("2", "")\
                        .replace("KZG", "KZO").replace("PHK", "KZO").replace("IDK", "KZO").replace("PKK", "KZO")
                
                r["group"] = normalize_brand(r["group"])
                c = (r.get("country") or "") or "Unknown"
                country_groups.setdefault(c, []).append(r)

            current_time, date_range = get_date_range_header()
            header_text = (
                f"💸 *Deposit Performance* \n"
                f"⏰ Data up to {current_time} (GMT+7) for each day \n"
                f"📅 Date range: {date_range[2]} → {date_range[0]}\n"
                "`%` ~ Percent vs. latest day’s total"
            )
            # await update.effective_chat.send_message(header_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

            await send_dpf_tables(update, country_groups, max_width=52)

        except Exception as e:
            logger.exception("Error in /dpf")
            await update.effective_chat.send_message(
                f"Error: `{e}`\nLocation: {self.config.BQ_LOCATION}",
                parse_mode=ParseMode.MARKDOWN
            )

    async def permission_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Admin guard
        if not self._is_admin(update):
            msg = update.effective_message
            return await (msg.reply_text("⚠️ You are not authorized to set permissions.") if msg
                        else context.bot.send_message(update.effective_chat.id, "⚠️ You are not authorized to set permissions."))

        args = list(context.args or [])
        chat_id_override = None
        cmds_param = None

        for a in args:
            if a.startswith("-chat="):
                chat_id_override = a.split("=", 1)[1].strip()
            elif a.startswith("-cmds=") or a.startswith("-cds="):
                cmds_param = a.split("=", 1)[1].strip().lower()

        chat = update.effective_chat
        msg  = update.effective_message
        # forum/topic support
        thread_id = getattr(msg, "message_thread_id", None)

        # Resolve target chat
        if chat_id_override:
            target_chat_id = int(chat_id_override)
            target_thread_id = None  # when overriding chat id, we don't have topic info
        else:
            if not chat:
                return await context.bot.send_message(
                    update.effective_user.id,
                    "⚠️ Cannot identify chat. Use -chat=<chat_id> from DM."
                )
            target_chat_id = chat.id
            # If the group has topics, reply in the same thread
            target_thread_id = thread_id if getattr(chat, "is_forum", False) else None

        # Validate commands
        valid = VALID_COMMANDS
        if not cmds_param:
            text = ("Usage: /permission -cmds=<apf,dpf,dist,pmh_total,pmh_provider|all|none>\n"
                    "Optional (from DM): -chat=<chat_id>")
            return await (msg.reply_text(text) if msg else
                        context.bot.send_message(target_chat_id, text, message_thread_id=target_thread_id))

        if cmds_param == "all":
            allowed = sorted(valid)
        elif cmds_param == "none":
            allowed = []
        else:
            parsed_in = [c.strip().lower() for c in cmds_param.split(",") if c.strip()]

            # Expand aliases (pmh -> pmh_total, pmh_provider, pmh_method)
            parsed = _expand_aliases(parsed_in)

            bad = [c for c in parsed if c not in valid]
            if bad:
                text = (
                    "⚠️ Unknown command(s): " + ", ".join(bad) + ". "
                    "Allowed: " + ", ".join(sorted(valid)) + ", or 'pmh' (shortcut), or 'all'/'none'."
                )
                return await (msg.reply_text(text) if msg else
                            context.bot.send_message(target_chat_id, text, message_thread_id=target_thread_id))

            allowed = sorted(set(parsed))

        # Save policy
        self.group_policies[str(target_chat_id)] = {
            "allowed_commands": allowed,
            "set_by": int(update.effective_user.id) if update.effective_user else None,
            "ts": datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
        }
        self._save_group_policies()

        # Build confirmation text (HTML-safe)
        return await update.effective_chat.send_message(
                "✅ Group policy updated.\n"
                f"🛂 Allowed commands: "
                f"{'all' if allowed is None else (', '.join(allowed) if allowed else 'All')}\n"
                "Type /help to see available commands."
            )


    
    def run(self):
        application = ApplicationBuilder().token(self.config.TELEGRAM_TOKEN).build()
        # application.add_handler(MessageHandler("who", self.who_command))  # <-- add this

        application.add_handler(CommandHandler("register_now", self.register_now))
        application.add_handler(CommandHandler("start", self.start_command))

        application.add_handler(CommandHandler("help", self.help_command))
        application.add_handler(CommandHandler("apf", self.apf_command))
        application.add_handler(CommandHandler("dist", self.dist_command))
        application.add_handler(CommandHandler("dpf", self.dpf_command))

        # After building the Application in your startup:
        application.add_handler(CommandHandler("pmh_week", self.pmh_week_command))
        application.add_handler(CommandHandler("pmh_total", self.pmh_total_command))
        application.add_handler(CommandHandler("pmh_provider", self.pmh_provider_command))
        application.add_handler(CommandHandler("pmh_method", self.pmh_method_command))

        application.add_handler(CommandHandler("admin_create_link", self.admin_create_link))
        application.add_handler(CommandHandler("permission", self.permission_command))


        # Catch-all for logging all invalid messages
        application.add_handler(MessageHandler(filters.ALL, self.echo), group=1)

        application.run_polling(poll_interval=2.0, timeout=50)

if __name__ == "__main__":
    bot = RealTimeBot()
    bot.run()