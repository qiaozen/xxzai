#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import asyncio
import logging
import sqlite3
import datetime
from collections import defaultdict
from typing import Optional

import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ==================== 配置（沿用你原来的）====================
LLM_API_KEY = "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"
LLM_BASE_URL = "https://api.groq.com/openai/v1"

LLM_MODELS = [
    "openai/gpt-oss-120b",
    "qwen/qwen3.8-27b",
    "openai/gpt-oss-20b",
    "allam-2-7b",
]

DIFFICULTY_MODELS = {
    "simple":  ["openai/gpt-oss-20b", "allam-2-7b"],
    "medium":  ["qwen/qwen3.8-27b", "openai/gpt-oss-20b"],
    "complex": ["openai/gpt-oss-120b", "qwen/qwen3.8-27b"],
}

CLASSIFIER_MODEL = "allam-2-7b"
ENABLE_DIFFICULTY_CACHE = True
COOLDOWN_SECONDS = 300

SYSTEM_PROMPT = """你是一个友好、简洁、聪明的AI助手。
回答要口语化，不要长篇大论，除非用户明确要求详细。
可以用emoji，但不要滥用。不确定的事情要诚实说不知道。"""

RATE_LIMIT = 10
RATE_WINDOW = 60
MAX_HISTORY = 20
DB_PATH = "bot_data.db"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

INIT_CREDITS = 20
CHECKIN_CREDITS = 5
COST_SIMPLE = 1
COST_MEDIUM = 3
COST_COMPLEX = 5

# 管理员密钥（前端用这个"登录"成管理员，替代原来的 Telegram ID 校验）
ADMIN_KEY = "change-me-to-a-strong-secret"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ==================== 数据库（原样保留）====================
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                msg_count   INTEGER DEFAULT 0,
                credits     INTEGER DEFAULT 0,
                last_checkin TEXT,
                banned      INTEGER DEFAULT 0,
                ban_reason  TEXT
            );
            CREATE TABLE IF NOT EXISTS history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                role        TEXT NOT NULL,
                content     TEXT NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_history_user ON history(user_id, id DESC);
        """)
        self._migrate()
        self.conn.commit()

    def _migrate(self):
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(users)")}
        for col, ddl in [
            ("credits", "ALTER TABLE users ADD COLUMN credits INTEGER DEFAULT 0"),
            ("last_checkin", "ALTER TABLE users ADD COLUMN last_checkin TEXT"),
            ("banned", "ALTER TABLE users ADD COLUMN banned INTEGER DEFAULT 0"),
            ("ban_reason", "ALTER TABLE users ADD COLUMN ban_reason TEXT"),
            ("msg_count", "ALTER TABLE users ADD COLUMN msg_count INTEGER DEFAULT 0"),
        ]:
            if col not in cols:
                self.conn.execute(ddl)

    def get_user(self, user_id):
        return self.conn.execute(
            "SELECT user_id, username, first_name, msg_count, credits, last_checkin, banned, ban_reason "
            "FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def upsert_user(self, user_id, username, first_name):
        if self.get_user(user_id) is None:
            self.conn.execute(
                "INSERT INTO users (user_id, username, first_name, msg_count, credits) VALUES (?, ?, ?, 1, ?)",
                (user_id, username or "", first_name or "", INIT_CREDITS))
        else:
            self.conn.execute(
                "UPDATE users SET username = ?, first_name = ?, msg_count = msg_count + 1 WHERE user_id = ?",
                (username or "", first_name or "", user_id))
        self.conn.commit()

    def add_message(self, user_id, role, content):
        self.conn.execute("INSERT INTO history (user_id, role, content) VALUES (?, ?, ?)",
                          (user_id, role, content))
        self.conn.commit()

    def get_history(self, user_id, limit=MAX_HISTORY):
        rows = self.conn.execute(
            "SELECT role, content FROM history WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit)).fetchall()
        return [{"role": r, "content": c} for r, c in reversed(rows)]

    def clear_history(self, user_id):
        self.conn.execute("DELETE FROM history WHERE user_id = ?", (user_id,))
        self.conn.commit()

    def get_stats(self, user_id):
        row = self.conn.execute(
            "SELECT msg_count, credits, last_checkin FROM users WHERE user_id = ?",
            (user_id,)).fetchone()
        total = self.conn.execute("SELECT COUNT(*) FROM history WHERE user_id = ?",
                                  (user_id,)).fetchone()[0]
        return {
            "msg_count": row[0] if row else 0,
            "credits": row[1] if row else 0,
            "last_checkin": row[2] if row else None,
            "history": total,
        }

    def get_global_stats(self):
        return {
            "total_users": self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "banned_users": self.conn.execute("SELECT COUNT(*) FROM users WHERE banned = 1").fetchone()[0],
            "total_credits": self.conn.execute("SELECT COALESCE(SUM(credits),0) FROM users").fetchone()[0],
            "total_msgs": self.conn.execute("SELECT COALESCE(SUM(msg_count),0) FROM users").fetchone()[0],
        }

    def get_credits(self, user_id):
        row = self.conn.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row else 0

    def add_credits(self, user_id, amount):
        self.conn.execute("UPDATE users SET credits = credits + ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()

    def spend_credits(self, user_id, amount):
        if self.get_credits(user_id) < amount:
            return False
        self.conn.execute("UPDATE users SET credits = credits - ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()
        return True

    def try_checkin(self, user_id):
        today = datetime.date.today().isoformat()
        row = self.conn.execute("SELECT last_checkin FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row and row[0] == today:
            return False, today
        self.conn.execute("UPDATE users SET last_checkin = ?, credits = credits + ? WHERE user_id = ?",
                          (today, CHECKIN_CREDITS, user_id))
        self.conn.commit()
        return True, today

    def set_banned(self, user_id, banned, reason=None):
        self.conn.execute("UPDATE users SET banned = ?, ban_reason = ? WHERE user_id = ?",
                          (1 if banned else 0, reason if banned else None, user_id))
        self.conn.commit()

    def is_banned(self, user_id):
        row = self.conn.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return bool(row[0]) if row else False

    def get_ban_reason(self, user_id):
        row = self.conn.execute("SELECT ban_reason FROM users WHERE user_id = ?", (user_id,)).fetchone()
        return row[0] if row else None

    def list_banned(self):
        return self.conn.execute(
            "SELECT user_id, username, first_name, ban_reason FROM users WHERE banned = 1").fetchall()

    def list_users(self, offset=0, limit=20):
        return self.conn.execute(
            "SELECT user_id, username, first_name, banned, credits FROM users ORDER BY user_id LIMIT ? OFFSET ?",
            (limit, offset)).fetchall()

    def count_users(self):
        return self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]


db = Database(DB_PATH)


# ==================== RateLimiter ====================
class RateLimiter:
    def __init__(self, max_calls, window):
        self.max_calls = max_calls
        self.window = window
        self.buckets = defaultdict(list)

    def check(self, user_id):
        now = time.time()
        bucket = self.buckets[user_id]
        bucket[:] = [t for t in bucket if now - t < self.window]
        if len(bucket) >= self.max_calls:
            return False, int(self.window - (now - bucket[0])) + 1
        bucket.append(now)
        return True, 0


limiter = RateLimiter(RATE_LIMIT, RATE_WINDOW)


# ==================== LLM 客户端（原样保留）====================
class LLMClient:
    def __init__(self, base_url, api_key, models):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.models = models
        self.current_idx = 0
        self.session = None
        self.cooldown = {}

    @property
    def current_model(self):
        return self.models[self.current_idx]

    def _find_available(self, candidates, tried):
        now = time.time()
        for model in candidates:
            if model in tried:
                continue
            if self.cooldown.get(model, 0) < now:
                return model
        return None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=90, connect=15)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def _try_model(self, model, messages, temperature, max_tokens):
        session = await self._get_session()
        payload = {"model": model, "messages": messages, "temperature": temperature,
                   "stream": False, "max_tokens": max_tokens}
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json",
                   "User-Agent": BROWSER_UA, "Accept": "application/json"}
        try:
            async with session.post(f"{self.base_url}/chat/completions", json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return True, data["choices"][0]["message"]["content"].strip(), False
                if resp.status == 401:
                    return False, "❌ Groq API Key 无效。", False
                if resp.status == 403:
                    return False, "🚫 Cloudflare 拦截。", False
                if resp.status == 404:
                    return False, None, True
                if resp.status == 429:
                    self.cooldown[model] = time.time() + COOLDOWN_SECONDS
                    return False, None, True
                text = await resp.text()
                logger.error(f"LLM {model} API {resp.status}: {text[:200]}")
                return False, None, True
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logger.warning(f"模型 {model} 网络错误: {e}")
            return False, None, True
        except Exception:
            logger.exception(f"模型 {model} 调用异常")
            return False, None, True

    async def chat(self, messages, temperature=0.7, max_tokens=4096, candidates=None, tag=""):
        pool = candidates or self.models
        tried = set()
        failed = []
        for _ in range(len(pool)):
            model = self._find_available(pool, tried)
            if model is None:
                break
            tried.add(model)
            ok, reply, _ = await self._try_model(model, messages, temperature, max_tokens)
            if ok:
                if model in self.models:
                    self.current_idx = self.models.index(model)
                logger.info(f"✅ 使用模型: {model} {tag}")
                return reply
            if reply:
                return reply
            failed.append(model)
        return f"⚠️ 所有候选模型都不可用（{', '.join(failed)}），请稍后再试。"

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


llm = LLMClient(LLM_BASE_URL, LLM_API_KEY, LLM_MODELS)


# ==================== 难度判断（原样保留）====================
SIMPLE_HINTS = ["你好", "hi", "hello", "在吗", "谢谢", "thanks", "几点", "今天", "天气", "翻译"]
COMPLEX_HINTS = ["架构", "设计", "重构", "优化", "算法", "证明", "推导", "详细", "深入",
                 "分析", "对比", "方案", "实现一个", "写一个完整", "debug", "报错",
                 "异常", "性能", "architecture", "design", "algorithm", "optimize", "refactor"]

_difficulty_cache: dict = {}


def quick_difficulty(text: str) -> Optional[str]:
    if not text:
        return "simple"
    lower = text.lower()
    length = len(text)
    if length <= 20:
        return "complex" if any(h in lower for h in COMPLEX_HINTS) else "simple"
    if any(h in lower for h in COMPLEX_HINTS):
        return "complex"
    if length <= 15 and any(h in lower for h in SIMPLE_HINTS):
        return "simple"
    if length > 300:
        return "complex"
    if length > 80:
        return "medium"
    return None


async def classify_difficulty(text: str) -> str:
    quick = quick_difficulty(text)
    if quick:
        return quick
    cache_key = text[:60]
    if ENABLE_DIFFICULTY_CACHE and cache_key in _difficulty_cache:
        return _difficulty_cache[cache_key]
    prompt = (
        "判断下面用户问题的难度，只返回一个单词：\n"
        "simple（简单闲聊、常识、短问题）\n"
        "medium（需要一定解释、中等长度问题）\n"
        "complex（需要深入分析、代码、架构、多步推理）\n\n"
        "只返回 simple / medium / complex 中的一个，不要其他文字。\n\n"
        f"问题：{text}"
    )
    try:
        session = await llm._get_session()
        payload = {"model": CLASSIFIER_MODEL, "messages": [{"role": "user", "content": prompt}],
                   "temperature": 0, "max_tokens": 8, "stream": False}
        headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json",
                   "User-Agent": BROWSER_UA}
        async with session.post(f"{LLM_BASE_URL}/chat/completions", json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw = data["choices"][0]["message"]["content"].strip().lower()
                for level in ("simple", "medium", "complex"):
                    if level in raw:
                        if ENABLE_DIFFICULTY_CACHE:
                            _difficulty_cache[cache_key] = level
                        return level
    except Exception as e:
        logger.warning(f"难度判断失败: {e}")
    return "medium"


DIFFICULTY_COST = {"simple": COST_SIMPLE, "medium": COST_MEDIUM, "complex": COST_COMPLEX}


# ==================== 文件意图检测（原样保留）====================
FILE_WORDS = ["文件", "脚本", "file", "script"]
ACTION_WORDS = ["生成", "写", "创建", "做", "给我", "来一个", "来个", "帮我", "写个", "整个"]
SUSPECT_WORDS = ["代码", "程序", "函数", "类", "模块", "工具", "网页", "页面", "爬虫",
                 "接口", "api", "bot", "机器人", "游戏", "小程序", "code", "program",
                 "function", "class", "module", "tool", "page", "web", "crawler", "game", "app"]

LANG_EXT = {
    "python": "py", "py": "py", "shell": "sh", "bash": "sh", "sh": "sh",
    "javascript": "js", "js": "js", "node": "js", "typescript": "ts", "ts": "ts",
    "java": "java", "kotlin": "kt", "c": "c", "cpp": "cpp", "c++": "cpp", "c#": "cs",
    "go": "go", "golang": "go", "rust": "rs", "php": "php", "ruby": "rb", "rb": "rb",
    "html": "html", "css": "css", "scss": "scss", "sql": "sql", "json": "json",
    "yaml": "yaml", "yml": "yml", "markdown": "md", "md": "md", "txt": "txt",
    "文本": "txt", "text": "txt", "csv": "csv", "xml": "xml", "toml": "toml",
    "ini": "ini", "conf": "conf", "env": "env", "dockerfile": "dockerfile", "makefile": "makefile",
}


def detect_file_intent(text: str):
    if not text:
        return False, None, None, False
    lower = text.lower()
    has_file = any(w in lower for w in FILE_WORDS) or any(w in text for w in FILE_WORDS)
    has_action = any(w in text for w in ACTION_WORDS)
    has_suspect = any(w in lower for w in SUSPECT_WORDS)
    if has_file:
        for lang, ext in LANG_EXT.items():
            if lang in lower:
                return True, ext, lang, False
        return True, "txt", "txt", False
    if has_action and has_suspect:
        return False, None, None, True
    return False, None, None, False


async def smart_detect(text: str):
    prompt = (
        "判断下面用户消息是否要求生成一个可下载的文件（如代码文件、文本文件）。\n"
        "如果是，只返回 JSON：{\"file\": true, \"ext\": \"py\", \"lang\": \"python\"}\n"
        "如果不是，只返回：{\"file\": false}\n"
        "只返回 JSON，不要任何其他文字。\n\n"
        f"用户消息：{text}"
    )
    try:
        r = await llm.chat([{"role": "user", "content": prompt}], temperature=0.1, max_tokens=100,
                           candidates=[CLASSIFIER_MODEL, "openai/gpt-oss-20b"], tag="[检测]")
        r = r.strip().strip("`").replace("json\n", "").strip()
        m = re.search(r"\{.*\}", r, re.DOTALL)
        if not m:
            return False, None, None
        data = json.loads(m.group(0))
        if data.get("file"):
            return True, data.get("ext", "txt"), data.get("lang", "text")
    except Exception as e:
        logger.warning(f"smart_detect 失败: {e}")
    return False, None, None


def extract_code_block(text: str) -> str:
    matches = re.findall(r"```[a-zA-Z0-9_+\-#\.]*\n(.*?)```", text, re.DOTALL)
    return matches[0].strip() if matches else text.strip()


# ==================== 核心问答逻辑 ====================
async def ask_llm(user_id: int, user_text: str) -> dict:
    """返回 {ok, reply, cost, remaining, model}"""
    difficulty = await classify_difficulty(user_text)
    cost = DIFFICULTY_COST.get(difficulty, COST_MEDIUM)

    if not db.spend_credits(user_id, cost):
        return {
            "ok": False,
            "reply": f"💸 积分不足（本次需 {cost} 分，当前 {db.get_credits(user_id)} 分）\n发送 /checkin 每日签到领 {CHECKIN_CREDITS} 积分。",
            "cost": 0, "remaining": db.get_credits(user_id), "model": None,
        }

    candidates = DIFFICULTY_MODELS.get(difficulty, LLM_MODELS)
    history = db.get_history(user_id, MAX_HISTORY)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history + [{"role": "user", "content": user_text}]

    reply = await llm.chat(messages, candidates=candidates, tag="[回答]")
    if reply.startswith("⚠️ 所有候选模型") and candidates != LLM_MODELS:
        reply = await llm.chat(messages, candidates=LLM_MODELS, tag="[回答-升级]")
    if reply.startswith("⚠️ 所有候选模型"):
        db.add_credits(user_id, cost)
        return {"ok": False, "reply": reply + f"\n（已退回 {cost} 积分）",
                "cost": 0, "remaining": db.get_credits(user_id), "model": None}

    db.add_message(user_id, "user", user_text)
    db.add_message(user_id, "assistant", reply)
    return {
        "ok": True, "reply": reply, "cost": cost,
        "remaining": db.get_credits(user_id), "model": llm.current_model,
        "difficulty": difficulty,
    }


async def generate_file(user_id: int, user_text: str, ext: str, lang: str) -> dict:
    """返回 {ok, filename, content, cost, remaining}"""
    if not db.spend_credits(user_id, COST_COMPLEX):
        return {"ok": False, "reply": f"💸 积分不足（生成文件需 {COST_COMPLEX} 分，当前 {db.get_credits(user_id)} 分）"}

    system_hint = (
        f"用户要求生成一个 .{ext} 文件（语言：{lang}）。\n"
        f"请只输出一个代码块（用 ``` 包裹），不要任何解释文字。\n"
        f"代码块内容会被原样保存为文件，所以要保证完整、可直接运行、无省略。"
    )
    history = db.get_history(user_id, MAX_HISTORY)
    messages = [{"role": "system", "content": SYSTEM_PROMPT + "\n\n" + system_hint}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_text})

    reply = await llm.chat(messages, max_tokens=4096, candidates=DIFFICULTY_MODELS["complex"], tag="[文件]")
    db.add_message(user_id, "user", user_text)
    db.add_message(user_id, "assistant", reply)

    code = extract_code_block(reply)
    if not code:
        db.add_credits(user_id, COST_COMPLEX)
        return {"ok": False, "reply": f"⚠️ AI 没按格式输出，已退回积分：\n\n{reply}"}

    ts = int(time.time())
    filename = f"ai_{ts}.{ext}"
    return {
        "ok": True, "filename": filename, "content": code, "lang": lang,
        "cost": COST_COMPLEX, "remaining": db.get_credits(user_id), "model": llm.current_model,
    }


# ==================== FastAPI ====================
app = FastAPI(title="小小臻AI Web")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
async def startup():
    await llm._get_session()
    logger.info("🚀 Web 服务已启动")


@app.on_event("shutdown")
async def shutdown():
    await llm.close()


def get_user_id(x_user_id: Optional[str] = Header(None)) -> int:
    """用 HTTP Header 中的 X-User-Id 作为用户标识（替代 Telegram user_id）"""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="缺少 X-User-Id")
    try:
        return int(x_user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="X-User-Id 必须是数字")


def is_admin(x_admin_key: Optional[str] = Header(None)) -> bool:
    return x_admin_key == ADMIN_KEY


def require_admin(x_admin_key: Optional[str] = Header(None)):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="无管理员权限")
    return True


# ---------- 数据模型 ----------
class ChatReq(BaseModel):
    text: str
    username: Optional[str] = None
    first_name: Optional[str] = None


class AdminCreditReq(BaseModel):
    user_id: int
    amount: int


class AdminBanReq(BaseModel):
    user_id: int
    reason: Optional[str] = "未说明"


# ---------- 页面 ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


# ---------- 用户接口 ----------
@app.post("/api/register")
async def api_register(req: ChatReq, user_id: int = Depends(get_user_id)):
    """首次访问注册，返回积分等信息"""
    is_new = db.get_user(user_id) is None
    db.upsert_user(user_id, req.username, req.first_name)
    return {
        "is_new": is_new,
        "credits": db.get_credits(user_id),
        "init_credits": INIT_CREDITS,
    }


@app.get("/api/me")
async def api_me(user_id: int = Depends(get_user_id)):
    stats = db.get_stats(user_id)
    return stats


@app.post("/api/clear")
async def api_clear(user_id: int = Depends(get_user_id)):
    db.clear_history(user_id)
    return {"ok": True}


@app.post("/api/checkin")
async def api_checkin(user_id: int = Depends(get_user_id)):
    ok, today = db.try_checkin(user_id)
    return {"ok": ok, "date": today, "credits": db.get_credits(user_id),
            "checkin_credits": CHECKIN_CREDITS}


@app.post("/api/chat")
async def api_chat(req: ChatReq, user_id: int = Depends(get_user_id)):
    user_text = (req.text or "").strip()
    if not user_text:
        raise HTTPException(status_code=400, detail="消息不能为空")

    if db.is_banned(user_id):
        reason = db.get_ban_reason(user_id) or "未说明"
        return {"ok": False, "reply": f"🚫 你已被封禁。原因：{reason}"}

    allowed, wait = limiter.check(user_id)
    if not allowed:
        return {"ok": False, "reply": f"⏳ 太快啦，请 {wait} 秒后再试。"}

    db.upsert_user(user_id, req.username, req.first_name)

    is_file, ext, lang, is_suspect = detect_file_intent(user_text)
    if is_suspect:
        is_file, ext, lang = await smart_detect(user_text)

    if is_file:
        result = await generate_file(user_id, user_text, ext, lang)
        if result.get("ok"):
            return {
                "ok": True, "type": "file",
                "filename": result["filename"], "content": result["content"], "lang": result["lang"],
                "cost": result["cost"], "remaining": result["remaining"], "model": result["model"],
            }
        return {"ok": False, "reply": result.get("reply", "生成失败")}

    result = await ask_llm(user_id, user_text)
    return {
        "ok": result["ok"], "type": "text", "reply": result["reply"],
        "cost": result.get("cost", 0), "remaining": result.get("remaining", 0),
        "model": result.get("model"), "difficulty": result.get("difficulty"),
    }


# ---------- 管理接口 ----------
@app.get("/api/admin/panel")
async def admin_panel(_: bool = Depends(require_admin)):
    g = db.get_global_stats()
    now = time.time()
    cooldown_info = []
    for m in LLM_MODELS:
        cd = llm.cooldown.get(m, 0)
        cooldown_info.append({"model": m, "available": cd < now,
                              "cooldown": int(cd - now) if cd > now else 0})
    return {
        "global": g,
        "current_model": llm.current_model,
        "cooldown": cooldown_info,
        "difficulty_models": DIFFICULTY_MODELS,
        "difficulty_cost": DIFFICULTY_COST,
    }


@app.get("/api/admin/users")
async def admin_users(page: int = 0, _: bool = Depends(require_admin)):
    total = db.count_users()
    total_pages = max(1, (total + 20 - 1) // 20)
    page = max(0, min(page, total_pages - 1))
    rows = db.list_users(offset=page * 20, limit=20)
    return {
        "total": total, "page": page, "total_pages": total_pages,
        "users": [{"user_id": r[0], "username": r[1], "first_name": r[2],
                   "banned": bool(r[3]), "credits": r[4]} for r in rows],
    }


@app.post("/api/admin/credits")
async def admin_credits(req: AdminCreditReq, _: bool = Depends(require_admin)):
    if db.get_user(req.user_id) is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    db.add_credits(req.user_id, req.amount)
    return {"ok": True, "credits": db.get_credits(req.user_id)}


@app.post("/api/admin/ban")
async def admin_ban(req: AdminBanReq, _: bool = Depends(require_admin)):
    if db.get_user(req.user_id) is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    db.set_banned(req.user_id, True, req.reason or "未说明")
    return {"ok": True}


@app.post("/api/admin/unban")
async def admin_unban(req: AdminBanReq, _: bool = Depends(require_admin)):
    if db.get_user(req.user_id) is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    db.set_banned(req.user_id, False)
    return {"ok": True}


@app.get("/api/admin/banlist")
async def admin_banlist(_: bool = Depends(require_admin)):
    rows = db.list_banned()
    return {"users": [{"user_id": r[0], "username": r[1], "first_name": r[2],
                       "reason": r[3]} for r in rows]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)