"""Парсер YouTube: данные каналов и видео из channels.txt сохраняет в SQLite
и пишет журнал работы.

Запуск: python parser.py
"""
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv

CHANNELS_FILE = "channels.txt"
ENV_FILE = "api.env"
DB_FILE = "youtube.db"
LOG_FILE = "parser.log"

API_URL = "https://www.googleapis.com/youtube/v3"
MAX_VIDEOS_PER_CHANNEL = 50   # столько последних видео проверяем на новые
TIMEOUT = 15                  # секунд на один запрос
RETRIES = 3                   # попыток при временном сбое

QUOTA_REASONS = {"quotaExceeded", "dailyLimitExceeded"}
RATE_LIMIT_REASONS = {"rateLimitExceeded", "userRateLimitExceeded"}

log = logging.getLogger("parser")


class QuotaExceeded(Exception):
    pass


# --- настройка ---

def setup_logging():
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    log.setLevel(logging.INFO)
    handlers = [
        logging.StreamHandler(),
        RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3,
                            encoding="utf-8"),
    ]
    for handler in handlers:
        handler.setFormatter(fmt)
        log.addHandler(handler)


def load_key():
    load_dotenv(ENV_FILE)
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        raise RuntimeError(f"в {ENV_FILE} нет YOUTUBE_API_KEY")
    return key


def read_channels():
    with open(CHANNELS_FILE, encoding="utf-8") as f:
        lines = [line.strip() for line in f]
    return [line for line in lines if line and not line.startswith("#")]


# --- запросы к API ---

def error_reason(response):
    try:
        return response.json()["error"]["errors"][0]["reason"]
    except (ValueError, KeyError, IndexError, TypeError):
        return ""


def api_get(endpoint, key, **params):
    params["key"] = key
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(f"{API_URL}/{endpoint}", params=params, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            # в тексте ошибки requests бывает адрес запроса вместе с ключом
            error = f"сетевая ошибка: {str(e).replace(key, '***')}"
        else:
            if r.status_code == 200:
                return r.json()
            reason = error_reason(r)
            if reason in QUOTA_REASONS:
                raise QuotaExceeded(reason)
            temporary = (r.status_code >= 500 or r.status_code == 429
                         or reason in RATE_LIMIT_REASONS)
            if not temporary:
                raise RuntimeError(f"HTTP {r.status_code} {reason}: {r.text[:200]}")
            error = f"HTTP {r.status_code} {reason}".strip()
        if attempt == RETRIES:
            raise RuntimeError(error)
        log.warning("%s, повтор %d из %d", error, attempt, RETRIES - 1)
        time.sleep(2 ** attempt)


def get_channel(key, name):
    params = {"part": "snippet,statistics,contentDetails"}
    if name.startswith("@"):
        params["forHandle"] = name
    else:
        params["id"] = name
    items = api_get("channels", key, **params).get("items", [])
    if not items:
        raise RuntimeError(f"канал не найден: {name}")
    item = items[0]
    snippet, stats = item["snippet"], item.get("statistics", {})
    return {
        "channel_id": item["id"],
        "title": snippet.get("title"),
        "url": f"https://www.youtube.com/channel/{item['id']}",
        "description": snippet.get("description"),
        "created_at": snippet.get("publishedAt"),
        "country": snippet.get("country"),
        "subscribers": to_int(stats.get("subscriberCount")),
        "views": to_int(stats.get("viewCount")),
        "video_count": to_int(stats.get("videoCount")),
        "uploads_playlist": item["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def get_video_ids(key, playlist_id):
    # список загрузок стоит 1 единицу квоты, а search.list целых 100
    ids, page = [], None
    while len(ids) < MAX_VIDEOS_PER_CHANNEL:
        data = api_get("playlistItems", key, part="contentDetails",
                       playlistId=playlist_id, maxResults=50, pageToken=page)
        ids += [item["contentDetails"]["videoId"] for item in data.get("items", [])]
        page = data.get("nextPageToken")
        if not page:
            break
    return ids[:MAX_VIDEOS_PER_CHANNEL]


def get_videos(key, ids):
    # за раз API отдаёт до 50 видео; удалённых и приватных в ответе нет
    videos = {}
    for i in range(0, len(ids), 50):
        data = api_get("videos", key, part="snippet,contentDetails,statistics",
                       id=",".join(ids[i:i + 50]))
        for item in data.get("items", []):
            videos[item["id"]] = item
    return videos


def get_categories(key):
    try:
        data = api_get("videoCategories", key, part="snippet", regionCode="US")
    except QuotaExceeded:
        raise
    except RuntimeError as e:
        log.warning("не получили названия категорий: %s", e)
        return {}
    return {c["id"]: c["snippet"]["title"] for c in data.get("items", [])}


# --- вспомогательное ---

def to_int(value):
    # YouTube отдаёт числа строками, а скрытых значений просто нет
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def duration_to_seconds(iso):
    # PT1H2M3S -> 3723
    m = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return None
    days, hours, minutes, seconds = (int(x or 0) for x in m.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --- база данных ---

def init_db():
    db = sqlite3.connect(DB_FILE)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id   TEXT PRIMARY KEY,
            title        TEXT,
            url          TEXT,
            description  TEXT,
            created_at   TEXT,
            country      TEXT,
            subscribers  INTEGER,
            views        INTEGER,
            video_count  INTEGER,
            collected_at TEXT
        );
        CREATE TABLE IF NOT EXISTS channel_stats_history (
            channel_id  TEXT,
            date        TEXT,
            subscribers INTEGER,
            views       INTEGER,
            video_count INTEGER,
            PRIMARY KEY (channel_id, date)
        );
        CREATE TABLE IF NOT EXISTS videos (
            video_id      TEXT PRIMARY KEY,
            channel_id    TEXT,
            channel_title TEXT,
            title         TEXT,
            url           TEXT,
            description   TEXT,
            published_at  TEXT,
            duration_sec  INTEGER,
            category      TEXT,
            views         INTEGER,
            likes         INTEGER,
            comments      INTEGER,
            tags          TEXT,     -- JSON-список
            thumbnail     TEXT,
            available     INTEGER,  -- 0, если видео удалили или закрыли
            collected_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS video_stats_history (
            video_id TEXT,
            date     TEXT,
            views    INTEGER,
            likes    INTEGER,
            comments INTEGER,
            PRIMARY KEY (video_id, date)
        );
        CREATE TABLE IF NOT EXISTS runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at       TEXT,
            finished_at      TEXT,
            duration_sec     REAL,
            channels_total   INTEGER,
            channels_ok      INTEGER,
            videos_found     INTEGER,
            videos_processed INTEGER,
            videos_new       INTEGER,
            errors           INTEGER,
            status           TEXT,  -- success, partial или failed
            critical_errors  TEXT
        );
    """)
    return db


def save_channel(db, ch):
    collected = now()
    db.execute("""
        INSERT INTO channels VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(channel_id) DO UPDATE SET
            title=excluded.title, url=excluded.url,
            description=excluded.description, created_at=excluded.created_at,
            country=excluded.country, subscribers=excluded.subscribers,
            views=excluded.views, video_count=excluded.video_count,
            collected_at=excluded.collected_at
    """, (ch["channel_id"], ch["title"], ch["url"], ch["description"],
          ch["created_at"], ch["country"], ch["subscribers"], ch["views"],
          ch["video_count"], collected))
    # в тот же день снимок перезаписывается, на следующий добавляется новый
    db.execute("INSERT OR REPLACE INTO channel_stats_history VALUES (?,?,?,?,?)",
               (ch["channel_id"], collected[:10], ch["subscribers"], ch["views"],
                ch["video_count"]))
    db.commit()


def save_video(db, ch, video, categories):
    snippet, stats = video["snippet"], video.get("statistics", {})
    collected = now()
    views, likes, comments = (to_int(stats.get(k)) for k in
                              ("viewCount", "likeCount", "commentCount"))
    thumbnails = snippet.get("thumbnails", {})
    thumbnail = (thumbnails.get("high") or thumbnails.get("default") or {}).get("url")
    tags = snippet.get("tags")
    duration = video.get("contentDetails", {}).get("duration")
    db.execute("""
        INSERT INTO videos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_title=excluded.channel_title, title=excluded.title,
            description=excluded.description, duration_sec=excluded.duration_sec,
            category=excluded.category, views=excluded.views, likes=excluded.likes,
            comments=excluded.comments, tags=excluded.tags,
            thumbnail=excluded.thumbnail, available=1,
            collected_at=excluded.collected_at
    """, (video["id"], ch["channel_id"], ch["title"], snippet.get("title"),
          f"https://www.youtube.com/watch?v={video['id']}",
          snippet.get("description"), snippet.get("publishedAt"),
          duration_to_seconds(duration), categories.get(snippet.get("categoryId")),
          views, likes, comments,
          json.dumps(tags, ensure_ascii=False) if tags else None,
          thumbnail, collected))
    db.execute("INSERT OR REPLACE INTO video_stats_history VALUES (?,?,?,?,?)",
               (video["id"], collected[:10], views, likes, comments))


def save_run(db, started_at, duration, total, status, critical):
    db.execute("""
        INSERT INTO runs (started_at, finished_at, duration_sec, channels_total,
                          channels_ok, videos_found, videos_processed, videos_new,
                          errors, status, critical_errors)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (started_at, now(), duration, total["channels"], total["ok"],
          total["found"], total["processed"], total["new"], total["errors"],
          status, "; ".join(critical) or None))
    db.commit()


# --- обработка канала и запуск ---

def process_channel(key, db, name, categories):
    channel = get_channel(key, name)
    save_channel(db, channel)

    latest = get_video_ids(key, channel["uploads_playlist"])
    known = {row[0] for row in db.execute(
        "SELECT video_id FROM videos WHERE channel_id=?", (channel["channel_id"],))}
    # старые видео из базы обновляем вместе с последними
    ids = list(dict.fromkeys(latest + sorted(known)))
    videos = get_videos(key, ids)

    result = {"title": channel["title"], "found": len(latest), "processed": 0,
              "new": 0, "unavailable": 0, "errors": 0}
    for video_id in ids:
        if video_id not in videos:
            result["unavailable"] += 1
            db.execute("UPDATE videos SET available=0 WHERE video_id=?", (video_id,))
            continue
        try:
            save_video(db, channel, videos[video_id], categories)
        except Exception as e:
            result["errors"] += 1
            log.error("  видео %s: %s", video_id, e)
            continue
        result["processed"] += 1
        result["new"] += video_id not in known
    db.commit()
    return result


def main():
    setup_logging()
    started = time.time()
    started_at = now()
    log.info("запуск начат")

    total = {"channels": 0, "ok": 0, "found": 0, "processed": 0, "new": 0, "errors": 0}
    critical = []
    db = None
    try:
        db = init_db()
        key = load_key()
        channels = read_channels()
        total["channels"] = len(channels)
        log.info("каналов в списке: %d", len(channels))
        categories = get_categories(key)

        for n, name in enumerate(channels, 1):
            log.info("[%d/%d] канал %s", n, len(channels), name)
            try:
                result = process_channel(key, db, name, categories)
            except QuotaExceeded:
                raise
            except Exception as e:
                # ошибка одного канала не должна останавливать остальные
                total["errors"] += 1
                log.error("  канал %s: %s", name, e)
                continue
            total["ok"] += 1
            for field in ("found", "processed", "new", "errors"):
                total[field] += result[field]
            log.info("  %s: найдено %d, обработано %d, новых %d, недоступных %d, "
                     "ошибок %d", result["title"], result["found"],
                     result["processed"], result["new"], result["unavailable"],
                     result["errors"])
    except QuotaExceeded:
        critical.append("закончилась квота YouTube API, запуск остановлен")
    except KeyboardInterrupt:
        critical.append("запуск прерван вручную")
    except Exception as e:
        critical.append(f"{type(e).__name__}: {e}")

    for message in critical:
        log.critical(message)

    if total["ok"] == 0:
        status = "failed"
    elif total["errors"] or critical:
        status = "partial"
    else:
        status = "success"
    duration = round(time.time() - started, 1)
    log.info("запуск завершён: статус %s, каналов %d из %d, найдено %d, "
             "обработано %d, новых %d, ошибок %d, %s сек", status, total["ok"],
             total["channels"], total["found"], total["processed"], total["new"],
             total["errors"], duration)
    if db:
        save_run(db, started_at, duration, total, status, critical)
    return 0 if status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
