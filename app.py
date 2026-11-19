# -*- coding: utf-8 -*-
"""Arvio Stream - Stremio Addon (single file, stateless, pipelined proxy)."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
import urllib.parse
from pathlib import Path

from flask import (Flask, jsonify, request, render_template_string,
                   Response, abort)
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

# ---------- Telethon ----------
BASE_DIR = Path(__file__).parent
LIB_PATH = BASE_DIR / "resources" / "lib"
THIRDPARTY_PATH = LIB_PATH / "thirdparty"
for _p in (str(THIRDPARTY_PATH), str(LIB_PATH)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from telethon import TelegramClient, functions, types, utils
    from telethon.sessions import StringSession
    from telethon.tl.types import (
        DocumentAttributeFilename, DocumentAttributeVideo,
        InputMessagesFilterVideo, InputMessagesFilterDocument,
        MessageMediaDocument, Document,
        InputDocumentFileLocation,
    )
    from telethon.errors import SessionPasswordNeededError
    _TELETHON_OK = True
except Exception as _exc:
    print(f"[arvio] Telethon indisponível: {_exc}")
    TelegramClient = StringSession = functions = types = utils = None
    DocumentAttributeFilename = DocumentAttributeVideo = None
    InputMessagesFilterVideo = InputMessagesFilterDocument = None
    MessageMediaDocument = Document = None
    InputDocumentFileLocation = None
    SessionPasswordNeededError = Exception
    _TELETHON_OK = False

try:
    import requests as _requests
    _REQUESTS_OK = True
except Exception:
    _requests = None
    _REQUESTS_OK = False

_CRYPTO_BACKEND = "pyaes"
if _TELETHON_OK:
    try:
        from telethon.crypto import aes as _taes
        if getattr(_taes, "cryptg", None):
            _CRYPTO_BACKEND = "cryptg"
        elif getattr(_taes, "libssl", None):
            _CRYPTO_BACKEND = "libssl"
    except Exception:
        pass
    if _CRYPTO_BACKEND == "pyaes":
        try:
            import cryptg  # noqa: F401
            _CRYPTO_BACKEND = "cryptg"
        except Exception:
            pass

# ---------- Credenciais ----------
TELEGRAM_API_ID = int(os.environ.get("TELEGRAM_API_ID", 0))
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SEARCH_TIMEOUT_S = 20

MT_CHUNK = 256 * 1024
PIPELINE_DEPTH = 3
INITIAL_BUFFER_BYTES = 1 * 1024 * 1024
QR_BACKGROUND_TIMEOUT = 240  # 4 minutos

# ---------- Token AES-GCM stateless ----------
_SECRET = os.environ.get("ARVIO_SECRET") or hashlib.sha256(
    str(Path(__file__).resolve()).encode()
).hexdigest()
_KEY = hashlib.sha256(_SECRET.encode()).digest()

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _HAS_AES = True
except Exception:
    _HAS_AES = False


def _encrypt(data: dict) -> str:
    raw = json.dumps(data, separators=(",", ":")).encode()
    if _HAS_AES:
        nonce = os.urandom(12)
        ct = AESGCM(_KEY).encrypt(nonce, raw, None)
        blob = nonce + ct
    else:
        blob = raw
    return base64.urlsafe_b64encode(blob).decode().rstrip("=")


def _decrypt(token: str | None) -> dict | None:
    if not token:
        return None
    try:
        pad = "=" * (-len(token) % 4)
        blob = base64.urlsafe_b64decode(token + pad)
        if _HAS_AES:
            nonce, ct = blob[:12], blob[12:]
            raw = AESGCM(_KEY).decrypt(nonce, ct, None)
        else:
            raw = blob
        return json.loads(raw.decode())
    except Exception:
        return None


def _session_from(token: str | None) -> str | None:
    d = _decrypt(token) if token else None
    return d.get("s") if d else None


def _token_for(session: str, extra: dict | None = None) -> str:
    payload = {"s": session}
    if extra:
        payload.update(extra)
    return _encrypt(payload)


# ---------- Flask ----------
_app = Flask(__name__)
_app.wsgi_app = ProxyFix(
    _app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=0,
)
CORS(_app)


@_app.after_request
def _no_cache(response):
    """Proíbe cache em todas as respostas dinâmicas.

    O Beamup/Nginx cacheia GET por ~1h30 sem respeitar a lógica da app,
    o que congela /configure/qr_poll no mesmo estado. Enviamos cabeçalhos
    que o Nginx respeita para forçar bypass total.
    """
    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, max-age=0, private"
    )
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"
    return response


def _base_url() -> str:
    base = request.url_root.rstrip("/")
    if "localhost" not in base and "127.0.0.1" not in base:
        base = base.replace("http://", "https://", 1)
        #if not '.baby-beamup.club' in base:
        #    base = base + '.baby-beamup.club'
    return base


# ==================================================================
# Estado do login QR (thread em background)
# ==================================================================
_qr_slot: dict = {
    "state": "idle",
    "qr_url": None,
    "token": None,
    "error": None,
    "client": None,
    "qr_login": None,
    "loop": None,
    "thread": None,
    "cancel": None,
    "started_at": 0.0,
    "pending_2fa": None,
}
_qr_slot_lock = threading.Lock()


def _slot_reset():
    with _qr_slot_lock:
        old = dict(_qr_slot)
        _qr_slot.update({
            "state": "idle",
            "qr_url": None,
            "token": None,
            "error": None,
            "client": None,
            "qr_login": None,
            "loop": None,
            "thread": None,
            "cancel": None,
            "started_at": 0.0,
            "pending_2fa": None,
        })
    if old.get("cancel"):
        try:
            old["cancel"].set()
        except Exception:
            pass


def _slot_get() -> dict:
    with _qr_slot_lock:
        return dict(_qr_slot)


def _slot_update(**kw):
    with _qr_slot_lock:
        _qr_slot.update(kw)


# ==================================================================
# Landing
# ==================================================================
@_app.route("/")
def landing():
    return render_template_string(
        TEMPLATE_LANDING,
        base_url=_base_url(),
        crypto_backend=_CRYPTO_BACKEND,
        telethon_ok=_TELETHON_OK,
    )


# ==================================================================
# Manifest / Streams
# ==================================================================
@_app.route("/manifest.json")
def manifest():
    token = request.args.get("token")
    has = bool(_session_from(token))
    return jsonify({
        "id": "arvio.stream",
        "version": "1.0.0",
        "name": "Arvio Stream",
        "description": "Stream de vídeos do Telegram via sua conta. "
                       "Configure para parear via QR code. Nada é salvo no servidor.",
        "logo": "https://raw.githubusercontent.com/zoreu/arvio_stream/refs/heads/main/logo.png",
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "resources": ["stream"],
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": not has,
        },
    })


@_app.route("/stream/<content_type>/<content_id>.json")
def streams_json(content_type: str, content_id: str):
    token = request.args.get("token")
    session = _session_from(token)
    if not session:
        return jsonify({"streams": [], "error": "not_connected"}), 401
    if content_type not in ("movie", "series"):
        return jsonify({"streams": []})

    imdb_id = content_id.split(":")[0]
    if not imdb_id.startswith("tt"):
        return jsonify({"streams": []})

    meta = _cinemeta_lookup(imdb_id, content_type)
    title = (meta or {}).get("name") or ""
    year = (meta or {}).get("year") or ""
    alt = (meta or {}).get("originalName") or ""
    aliases = list({title, alt} - {""})

    queries = []
    for t in aliases:
        if year:
            queries.append(f"{t} {year}")
        queries.append(t)
        queries.append(t.replace(" ", "."))
    queries = [q for q in dict.fromkeys(queries) if q.strip()]
    if not queries:
        queries = [imdb_id]

    try:
        videos = _search_telegram_multi(session, queries, imdb_id=imdb_id)
    except Exception as e:
        print(f"[arvio] busca falhou: {e}")
        return jsonify({"streams": [], "error": str(e)}), 500

    streams = []
    for v in videos:
        play_payload = {
            "s": session,
            "p": v["peerId"],
            "t": v.get("peerType", ""),
            "r": v.get("peerRawId", ""),
            "a": v.get("accessHash", ""),
            "m": v["messageId"],
            "n": v.get("fileName", "video.mp4"),
            "z": int(v.get("size") or 0),
            "d": v.get("documentId", 0),
            "ah": v.get("documentAccessHash", 0),
            "dc": v.get("documentDcId", 0),
            "fr": base64.b64encode(v.get("fileReference", b"")).decode(),
            "mime": v.get("mime", "video/mp4"),
        }
        play_token = _encrypt(play_payload)
        url = f"{_base_url()}/play/{play_token}"
        streams.append({
            "url": url,
            "name": "Arvio Stream",
            "title": f"{v.get('fileName', 'video')}",
            "behaviorHints": {"notWebReady": True},
        })
    return jsonify({"streams": streams})


# ==================================================================
# Cinemeta
# ==================================================================
_CINEMETA_CACHE: dict[str, dict] = {}
_CINEMETA_LOCK = threading.Lock()


def _cinemeta_lookup(imdb_id: str, content_type: str) -> dict | None:
    key = f"{content_type}:{imdb_id}"
    with _CINEMETA_LOCK:
        if key in _CINEMETA_CACHE:
            return _CINEMETA_CACHE[key]
    if not _REQUESTS_OK:
        return None
    for kind in ("movie", "series"):
        try:
            r = _requests.get(
                f"https://v3-cinemeta.strem.io/meta/{kind}/{imdb_id}.json",
                timeout=6,
            )
            if r.ok:
                meta = (r.json() or {}).get("meta") or {}
                if meta.get("name"):
                    with _CINEMETA_LOCK:
                        _CINEMETA_CACHE[key] = meta
                    return meta
        except Exception:
            continue
    return None


# ==================================================================
# Telethon helpers
# ==================================================================
def _new_client(session_string: str | None = None):
    if not _TELETHON_OK:
        raise RuntimeError("Telethon não instalado.")
    s = StringSession(session_string) if session_string else StringSession()
    return TelegramClient(
        s, TELEGRAM_API_ID, TELEGRAM_API_HASH,
        timeout=15, request_retries=3, connection_retries=3,
        auto_reconnect=True, receive_updates=False,
    )


def _run_sync(coro_factory, timeout: float | None = None):
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running and running.is_running():
        box, done = {}, threading.Event()

        def worker():
            try:
                box["v"] = _run_new_loop(coro_factory, timeout)
            except BaseException as e:
                box["e"] = e
            finally:
                done.set()

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        done.wait((timeout or 60) + 10)
        if "e" in box:
            raise box["e"]
        return box.get("v")
    return _run_new_loop(coro_factory, timeout)


def _run_new_loop(coro_factory, timeout):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        coro = coro_factory()
        if timeout:
            coro = asyncio.wait_for(coro, timeout=timeout)
        return loop.run_until_complete(coro)
    finally:
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass


# ==================================================================
# Busca
# ==================================================================
def _file_name(doc) -> str:
    for a in getattr(doc, "attributes", []) or []:
        if isinstance(a, DocumentAttributeFilename):
            return a.file_name or ""
    return ""


def _duration(doc) -> int:
    for a in getattr(doc, "attributes", []) or []:
        if isinstance(a, DocumentAttributeVideo):
            try:
                return int(a.duration or 0)
            except Exception:
                return 0
    return 0


def _peer_desc(entity) -> dict:
    if entity is None:
        return {}
    try:
        signed = str(utils.get_peer_id(entity))
    except Exception:
        signed = ""
    raw_id = getattr(entity, "id", None)
    ah = getattr(entity, "access_hash", None)
    if isinstance(entity, types.Channel):
        kind = "channel"
    elif isinstance(entity, types.User):
        kind = "user"
    elif isinstance(entity, types.Chat):
        kind = "chat"
    else:
        kind = ""
    return {
        "peerId": signed,
        "peerType": kind,
        "peerRawId": str(raw_id) if raw_id is not None else "",
        "accessHash": str(ah) if ah is not None else "",
    }


def _video_from_msg(msg, desc: dict) -> dict | None:
    media = getattr(msg, "media", None)
    if not isinstance(media, MessageMediaDocument):
        return None
    doc = media.document
    if not isinstance(doc, Document):
        return None
    mime = (doc.mime_type or "").lower()
    name = _file_name(doc)
    ext_ok = name.lower().endswith(
        (".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts")
    )
    if not (mime.startswith("video/") or mime == "application/x-matroska" or ext_ok):
        return None
    name = name or "video.mp4"
    return {
        "key": f"{desc.get('peerId')}|{msg.id}|{name}|{doc.size or 0}",
        "fileName": name,
        "size": int(doc.size or 0),
        "mime": mime or "video/mp4",
        "duration": _duration(doc),
        "caption": msg.message or "",
        "peerId": desc.get("peerId", ""),
        "peerType": desc.get("peerType", ""),
        "peerRawId": desc.get("peerRawId", ""),
        "accessHash": desc.get("accessHash", ""),
        "messageId": int(msg.id),
        "documentId": int(getattr(doc, "id", 0) or 0),
        "documentAccessHash": int(getattr(doc, "access_hash", 0) or 0),
        "documentDcId": int(getattr(doc, "dc_id", 0) or 0),
        "fileReference": bytes(getattr(doc, "file_reference", b"") or b""),
    }


def _tokens(text: str) -> list[str]:
    norm = re.sub(r"[^\w\s]", " ", str(text or "").lower(), flags=re.UNICODE)
    return [t for t in norm.split() if len(t) >= 2]


def _score(video: dict, query: str, imdb_id: str) -> int:
    blob = f"{video.get('fileName','')} {video.get('caption','')}".lower()
    q = query.lower()
    q_tokens = _tokens(q)
    blob_tokens = set(_tokens(blob))
    score = 0
    if imdb_id and imdb_id.lower() in blob:
        score += 100
    if q in blob:
        score += 40
    if q_tokens and all(t in blob_tokens for t in q_tokens):
        score += 30
    if q_tokens:
        hits = sum(1 for t in q_tokens if t in blob_tokens)
        score += int(20 * hits / max(1, len(q_tokens)))
    years = re.findall(r"(19|20)\d{2}", q)
    if years and any(y in blob for y in years):
        score += 10
    if video.get("size", 0) > 100 * 1024 * 1024:
        score += 5
    return score


async def _joined_scope(client) -> dict:
    scope = {}
    try:
        async for d in client.iter_dialogs(limit=None):
            e = getattr(d, "entity", None)
            if e is None or getattr(e, "deleted", False) or getattr(e, "left", False):
                continue
            if not isinstance(e, (types.Channel, types.Chat)):
                continue
            desc = _peer_desc(e)
            if desc.get("peerId"):
                scope[desc["peerId"]] = {"entity": e, "desc": desc}
    except Exception as exc:
        print(f"[arvio] scope: {exc}")
    return scope


async def _search_global(client, query: str, scope: dict, limit: int = 20) -> list[dict]:
    if not scope or not query.strip():
        return []

    async def one(flt):
        try:
            return await client(functions.messages.SearchGlobalRequest(
                q=query, filter=flt, min_date=0, max_date=0,
                offset_rate=0, offset_peer=types.InputPeerEmpty(),
                offset_id=0, limit=min(limit, 100),
            ))
        except Exception as exc:
            print(f"[arvio] global falhou ({query}): {exc}")
            return None

    results = await asyncio.gather(
        one(InputMessagesFilterVideo()),
        one(InputMessagesFilterDocument()),
        return_exceptions=True,
    )
    seen, out = set(), []
    for r in results:
        if not r or isinstance(r, BaseException):
            continue
        for msg in getattr(r, "messages", []) or []:
            try:
                signed = str(utils.get_peer_id(msg.peer_id))
            except Exception:
                signed = ""
            entry = scope.get(signed)
            if not entry:
                continue
            v = _video_from_msg(msg, entry["desc"])
            if v and v["key"] not in seen:
                seen.add(v["key"])
                out.append(v)
    return out


async def _search_per_dialog(client, query: str, scope: dict, limit: int = 20) -> list[dict]:
    if not scope:
        return []
    sem = asyncio.Semaphore(5)
    out: list[dict] = []
    seen = set()

    async def scan(entry):
        entity = entry.get("entity")
        if entity is None:
            return []
        local = []
        try:
            async with sem:
                async for msg in client.iter_messages(entity, search=query, limit=15):
                    v = _video_from_msg(msg, entry["desc"])
                    if v:
                        local.append(v)
        except Exception:
            pass
        return local

    tasks = [asyncio.ensure_future(scan(e)) for e in scope.values()]
    try:
        for fut in asyncio.as_completed(tasks):
            try:
                found = await fut
            except Exception:
                found = []
            for v in found:
                if v["key"] not in seen:
                    seen.add(v["key"])
                    out.append(v)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return out[:limit]


async def _search_by_filename(client, tokens: list[str], scope: dict, limit: int = 30) -> list[dict]:
    if not scope or not tokens:
        return []
    sem = asyncio.Semaphore(4)
    out: list[dict] = []
    seen = set()
    key_tokens = [t for t in tokens if len(t) >= 3][:6]

    async def scan(entry):
        entity = entry.get("entity")
        if entity is None:
            return []
        local = []
        try:
            async with sem:
                async for msg in client.iter_messages(
                    entity, limit=200, filter=InputMessagesFilterVideo()
                ):
                    v = _video_from_msg(msg, entry["desc"])
                    if not v:
                        continue
                    blob = f"{v['fileName']} {v.get('caption','')}".lower()
                    hits = sum(1 for t in key_tokens if t in blob)
                    if hits >= max(1, len(key_tokens) - 1):
                        local.append(v)
                        if len(local) >= 8:
                            break
        except Exception:
            pass
        return local

    tasks = [asyncio.ensure_future(scan(e)) for e in scope.values()]
    try:
        for fut in asyncio.as_completed(tasks):
            try:
                found = await fut
            except Exception:
                found = []
            for v in found:
                if v["key"] not in seen:
                    seen.add(v["key"])
                    out.append(v)
                    if len(out) >= limit:
                        return out
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return out


async def _search_async(session: str, queries: list[str], imdb_id: str) -> list[dict]:
    client = _new_client(session)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("Sessão expirada. Reconecte em /configure.")
        scope = await _joined_scope(client)
        print(f"[arvio] escopo: {len(scope)} canal(is)")

        all_results: dict[str, dict] = {}
        scores: dict[str, int] = {}

        def absorb(videos: list[dict], query: str):
            for v in videos:
                k = v["key"]
                if k not in all_results:
                    all_results[k] = v
                    scores[k] = 0
                scores[k] = max(scores[k], _score(v, query, imdb_id))

        for q in queries[:4]:
            found = await _search_global(client, q, scope)
            absorb(found, q)

        if len(all_results) < 5:
            for q in queries[:3]:
                found = await _search_per_dialog(client, q, scope)
                absorb(found, q)

        tokens = _tokens(queries[0] if queries else "")
        if tokens:
            found = await _search_by_filename(client, tokens, scope)
            absorb(found, queries[0] if queries else "")

        ordered = sorted(all_results.values(), key=lambda v: scores[v["key"]], reverse=True)
        filtered = [v for v in ordered if scores[v["key"]] >= 10] or ordered[:5]
        return filtered[:25]
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def _search_telegram_multi(session: str, queries: list[str], imdb_id: str = "") -> list[dict]:
    return _run_sync(
        lambda: _search_async(session, queries, imdb_id),
        timeout=TELEGRAM_SEARCH_TIMEOUT_S + 60,
    )


# ==================================================================
# Proxy /play
# ==================================================================
@_app.route("/play/<token>", methods=["GET", "HEAD"])
def play(token: str):
    data = _decrypt(token)
    if not data or not data.get("s"):
        abort(401)

    total = int(data.get("z") or 0)
    mime = data.get("mime") or "video/mp4"
    if mime == "application/x-matroska":
        mime = "video/x-matroska"
    if total <= 0:
        abort(500)

    start, end = 0, total - 1
    partial = False
    rng = request.headers.get("Range", "")
    m = re.match(r"bytes=(\d*)-(\d*)", rng or "")
    if m:
        partial = True
        if m.group(1):
            start = int(m.group(1))
        if m.group(2):
            end = int(m.group(2))
        else:
            end = total - 1
        end = min(end, total - 1)
        if start > end:
            abort(416)

    length = end - start + 1
    headers = {
        "Content-Type": mime,
        "Content-Length": str(length),
        "Accept-Ranges": "bytes",
    }
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"

    if request.method == "HEAD":
        return Response(status=206 if partial else 200, headers=headers)

    return Response(
        _stream_pipelined(data, start, length),
        status=206 if partial else 200,
        headers=headers,
    )


def _stream_pipelined(data: dict, start: int, length: int):
    location = InputDocumentFileLocation(
        id=int(data["d"]),
        access_hash=int(data["ah"]),
        file_reference=base64.b64decode(data["fr"]) if data.get("fr") else b"",
        thumb_size="",
    )
    dc_id = int(data.get("dc") or 0) or None
    file_size = int(data.get("z") or 0)
    session = data["s"]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = None
    try:
        client = _new_client(session)
        loop.run_until_complete(client.connect())
        if not loop.run_until_complete(client.is_user_authorized()):
            print("[arvio] play: sessão não autorizada")
            return

        print(f"[arvio] play: AES={_CRYPTO_BACKEND}, pipeline={PIPELINE_DEPTH}x"
              f"{MT_CHUNK//1024}KB, start={start}, len={length}")

        async def _pipeline():
            slots: dict[int, bytes] = {}
            cond = asyncio.Condition()
            state = {"next_offset": start, "consume_offset": start,
                     "error": None, "done": False}
            end_offset = start + length

            async def producer():
                while True:
                    async with cond:
                        while (state["next_offset"] in slots
                               and not state["done"]
                               and state["error"] is None):
                            await cond.wait()
                        if (state["error"] is not None or state["done"]
                                or state["next_offset"] >= end_offset):
                            return
                        offset = state["next_offset"]
                        want = min(MT_CHUNK, end_offset - offset)
                        state["next_offset"] = offset + want
                    try:
                        buf = await _fetch_chunk(
                            client, location, offset, want, dc_id, file_size)
                    except Exception as exc:
                        async with cond:
                            state["error"] = exc
                            cond.notify_all()
                        return
                    async with cond:
                        slots[offset] = buf
                        cond.notify_all()

            producers = [asyncio.ensure_future(producer())
                         for _ in range(PIPELINE_DEPTH)]
            try:
                buffered = 0
                pending_out: list[bytes] = []
                while state["consume_offset"] < end_offset:
                    async with cond:
                        target = state["consume_offset"]
                        while (target not in slots
                               and state["error"] is None
                               and not (state["done"] and target >= end_offset)):
                            if state["done"]:
                                break
                            await cond.wait()
                        if state["error"] is not None:
                            raise state["error"]
                        if target not in slots:
                            break
                        buf = slots.pop(target)
                        state["consume_offset"] = target + len(buf)
                    buffered += len(buf)
                    pending_out.append(buf)
                    if buffered >= INITIAL_BUFFER_BYTES or state["consume_offset"] >= end_offset:
                        for chunk in pending_out:
                            yield chunk
                        pending_out.clear()
                        buffered = 0
                for chunk in pending_out:
                    yield chunk
            finally:
                async with cond:
                    state["done"] = True
                    cond.notify_all()
                for t in producers:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*producers, return_exceptions=True)

        agen = _pipeline()
        try:
            while True:
                try:
                    chunk = loop.run_until_complete(agen.__anext__())
                except StopAsyncIteration:
                    break
                if chunk:
                    yield chunk
        finally:
            try:
                loop.run_until_complete(agen.aclose())
            except Exception:
                pass
    except GeneratorExit:
        raise
    except Exception as exc:
        print(f"[arvio] stream erro: {type(exc).__name__}: {exc}")
    finally:
        if client is not None:
            try:
                loop.run_until_complete(client.disconnect())
            except Exception:
                pass
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass


async def _fetch_chunk(client, location, offset: int, wanted: int,
                       dc_id, file_size) -> bytes:
    parts: list[bytes] = []
    got = 0
    limit = max(1, math.ceil(wanted / MT_CHUNK))
    async for chunk in client.iter_download(
        location, offset=offset, request_size=MT_CHUNK,
        limit=limit, dc_id=dc_id, file_size=file_size,
    ):
        if not chunk:
            break
        raw = bytes(chunk)
        parts.append(raw)
        got += len(raw)
        if got >= wanted:
            break
    return b"".join(parts)[:wanted]


# ==================================================================
# QR Login em background — START / POLL / CANCEL
# ==================================================================
def _qr_background_worker(client, qr_login, loop, cancel_event):
    async def _wait():
        while not cancel_event.is_set():
            if time.time() - _qr_slot.get("started_at", 0) > QR_BACKGROUND_TIMEOUT:
                raise TimeoutError("timeout")
            try:
                await asyncio.wait_for(qr_login.wait(timeout=10), timeout=15)
                return
            except asyncio.TimeoutError:
                continue
        raise RuntimeError("cancelled")

    try:
        loop.run_until_complete(_wait())
        me = loop.run_until_complete(client.get_me())
        if not me:
            raise RuntimeError("Falha ao obter conta.")
        session_string = client.session.save()
        tok = _token_for(session_string)
        _slot_update(state="success", token=tok)
    except SessionPasswordNeededError:
        try:
            _slot_update(pending_2fa=client.session.save(), state="2fa")
        except Exception as exc:
            _slot_update(state="error", error=str(exc))
    except Exception as exc:
        msg = str(exc).lower()
        if "timeout" in msg:
            _slot_update(state="error", error="Tempo esgotado. Tente novamente.")
        elif "cancel" in msg:
            _slot_update(state="cancelled")
        else:
            _slot_update(state="error", error=str(exc))
    finally:
        try:
            loop.run_until_complete(client.disconnect())
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass


@_app.route("/configure/qr_start", methods=["POST"])
def qr_start():
    if not _TELETHON_OK:
        return jsonify({"success": False, "error": "Telethon indisponível."}), 500

    _slot_reset()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = _new_client()
        loop.run_until_complete(client.connect())
        qr_login = loop.run_until_complete(client.qr_login())
        qr_url = qr_login.url
    except Exception as exc:
        try:
            loop.close()
        except Exception:
            pass
        return jsonify({"success": False, "error": str(exc)}), 500

    cancel_event = threading.Event()
    _slot_update(
        state="waiting",
        qr_url=qr_url,
        client=client,
        qr_login=qr_login,
        loop=loop,
        cancel=cancel_event,
        started_at=time.time(),
    )

    t = threading.Thread(
        target=_qr_background_worker,
        args=(client, qr_login, loop, cancel_event),
        name="ArvioQRWorker",
        daemon=True,
    )
    _slot_update(thread=t)
    t.start()

    return jsonify({
        "success": True,
        "qr_url": qr_url,
        "status": "waiting",
    })


@_app.route("/configure/qr_poll")
def qr_poll():
    slot = _slot_get()
    state = slot.get("state", "idle")
    out = {"status": state}

    if state == "waiting":
        out["qr_url"] = slot.get("qr_url")
    elif state == "2fa":
        out["needs_2fa"] = True
    elif state == "success":
        tok = slot.get("token")
        out["token"] = tok
        out["new_manifest_url"] = (
            f"{_base_url()}/manifest.json?token={urllib.parse.quote(tok)}"
            if tok else None
        )
        out["message"] = "Conectado!"
    elif state == "error":
        out["error"] = slot.get("error") or "Erro desconhecido."
    elif state == "cancelled":
        out["message"] = "Login cancelado."
    return jsonify(out)


@_app.route("/configure/qr_cancel", methods=["POST"])
def qr_cancel():
    slot = _slot_get()
    ev = slot.get("cancel")
    if ev:
        try:
            ev.set()
        except Exception:
            pass
    _slot_update(state="cancelled")
    return jsonify({"success": True})


@_app.route("/configure/2fa_start", methods=["POST"])
def twofa_start():
    data = request.get_json() or {}
    pw = (data.get("password") or "").strip()
    if not pw:
        return jsonify({"success": False, "error": "Senha vazia."}), 400

    slot = _slot_get()
    pending = slot.get("pending_2fa")
    if not pending:
        return jsonify({"success": False, "error": "Nenhum login aguardando senha."}), 400

    _slot_update(state="waiting_2fa")

    def _worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = None
        try:
            client = _new_client(pending)
            loop.run_until_complete(client.connect())
            me = loop.run_until_complete(client.sign_in(password=pw))
            if not me:
                me = loop.run_until_complete(client.get_me())
            if not me:
                raise RuntimeError("2FA falhou.")
            session_string = client.session.save()
            tok = _token_for(session_string)
            _slot_update(state="success", token=tok)
        except Exception as exc:
            _slot_update(state="error", error=str(exc))
        finally:
            if client is not None:
                try:
                    loop.run_until_complete(client.disconnect())
                except Exception:
                    pass
            try:
                loop.close()
            except Exception:
                pass

    threading.Thread(target=_worker, name="Arvio2FAWorker", daemon=True).start()
    return jsonify({"success": True, "status": "waiting_2fa"})


# ==================================================================
# Configure page
# ==================================================================
@_app.route("/configure")
def configure():
    token = request.args.get("token")
    session = _session_from(token)
    if session:
        return render_template_string(
            TEMPLATE_CONNECTED,
            manifest_url=f"{_base_url()}/manifest.json?token={urllib.parse.quote(token)}",
            base_url=_base_url(),
        )
    return render_template_string(TEMPLATE_DISCONNECTED, base_url=_base_url())


@_app.route("/configure/disconnect", methods=["POST"])
def disconnect():
    return jsonify({"success": True,
                    "new_manifest_url": f"{_base_url()}/manifest.json"})


@_app.route("/health")
def health():
    return jsonify({
        "status": "ok", "addon": "Arvio Stream",
        "telethon": _TELETHON_OK, "aes": _HAS_AES,
        "crypto_backend": _CRYPTO_BACKEND,
        "pipeline": f"{PIPELINE_DEPTH}x{MT_CHUNK//1024}KB",
    })


# ==================================================================
# Templates
# ==================================================================
TEMPLATE_LANDING = """<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Arvio Stream — Addon para Stremio</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:radial-gradient(ellipse at top,#1a1a2e 0%,#0f0f1a 100%);
  color:#eee;min-height:100vh;display:flex;flex-direction:column;
  align-items:center;justify-content:center;padding:20px;line-height:1.6}
.container{max-width:640px;width:100%;text-align:center;animation:fadeIn .5s ease-out}
@keyframes fadeIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.logo{width:96px;height:96px;margin:0 auto 24px;
  background:linear-gradient(135deg,#e94560 0%,#c72e4a 100%);
  border-radius:24px;display:flex;align-items:center;justify-content:center;
  box-shadow:0 12px 32px rgba(233,69,96,.4);position:relative;overflow:hidden}
.logo::before{content:'';position:absolute;top:-50%;left:-50%;width:200%;height:200%;
  background:radial-gradient(circle,rgba(255,255,255,.2) 0%,transparent 60%);
  animation:shine 4s linear infinite}
@keyframes shine{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
.logo svg{width:52px;height:52px;fill:white;position:relative;z-index:1}
h1{font-size:42px;font-weight:800;margin-bottom:12px;
  background:linear-gradient(135deg,#fff 0%,#aaa 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;letter-spacing:-1px}
.subtitle{color:#888;font-size:17px;margin-bottom:36px;max-width:480px;
  margin-left:auto;margin-right:auto}
.features{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
  gap:12px;margin-bottom:36px}
.feature{background:rgba(22,33,62,.6);border:1px solid rgba(233,69,96,.15);
  border-radius:12px;padding:18px 12px;font-size:13px;color:#aaa;transition:all .2s}
.feature:hover{border-color:rgba(233,69,96,.4);transform:translateY(-2px);
  background:rgba(22,33,62,.9)}
.feature-icon{font-size:24px;margin-bottom:6px;display:block}
.feature strong{color:#eee;display:block;margin-bottom:2px;font-size:14px}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:10px;
  padding:16px 48px;background:linear-gradient(135deg,#e94560 0%,#c72e4a 100%);
  color:#fff;border:none;border-radius:12px;font-size:17px;font-weight:700;
  cursor:pointer;text-decoration:none;transition:all .2s;
  box-shadow:0 8px 24px rgba(233,69,96,.35);letter-spacing:.5px}
.btn:hover{transform:translateY(-2px);box-shadow:0 12px 32px rgba(233,69,96,.5)}
.btn svg{width:20px;height:20px;fill:currentColor}
.footer{margin-top:48px;font-size:12px;color:#555}
.footer a{color:#888;text-decoration:none;border-bottom:1px dotted #444}
.footer a:hover{color:#e94560;border-color:#e94560}
.status-badges{display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-bottom:32px}
.badge{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;
  background:rgba(22,33,62,.8);border:1px solid rgba(255,255,255,.06);
  border-radius:20px;font-size:11px;color:#888;font-weight:500}
.badge-dot{width:6px;height:6px;border-radius:50%;background:#4dff4d;
  box-shadow:0 0 8px rgba(77,255,77,.6)}
.badge-dot.warn{background:#ffb84d;box-shadow:0 0 8px rgba(255,184,77,.6)}
</style></head><body>
<div class="container">
  <div class="logo"><svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></div>
  <h1>Arvio Stream</h1>
  <p class="subtitle">Assista aos vídeos dos seus canais e grupos do Telegram
    diretamente no Stremio. Pareamento por QR code. Nada é armazenado no servidor.</p>
  <div class="status-badges">
    <span class="badge"><span class="badge-dot {% if not telethon_ok %}warn{% endif %}"></span>
      Telethon {% if telethon_ok %}ativo{% else %}ausente{% endif %}</span>
    <span class="badge"><span class="badge-dot {% if crypto_backend == 'pyaes' %}warn{% endif %}"></span>
      AES {{ crypto_backend }}</span>
    <span class="badge"><span class="badge-dot"></span>Proxy em pipeline</span>
  </div>
  <div class="features">
    <div class="feature"><span class="feature-icon">🔐</span>
      <strong>Privado</strong>Token criptografado</div>
    <div class="feature"><span class="feature-icon">⚡</span>
      <strong>Rápido</strong>Download paralelo</div>
    <div class="feature"><span class="feature-icon">🎬</span>
      <strong>Completo</strong>Busca por IMDB</div>
  </div>
  <a href="{{ base_url }}/configure" class="btn">
    <svg viewBox="0 0 24 24"><path d="M12 15.5A3.5 3.5 0 0 1 8.5 12 3.5 3.5 0 0 1 12 8.5a3.5 3.5 0 0 1 3.5 3.5 3.5 3.5 0 0 1-3.5 3.5m7.43-2.53c.04-.32.07-.64.07-.97 0-.33-.03-.66-.07-1l2.11-1.63c.19-.15.24-.42.12-.64l-2-3.46c-.12-.22-.39-.31-.61-.22l-2.49 1c-.52-.39-1.06-.73-1.69-.98l-.37-2.65A.506.506 0 0 0 14 2h-4c-.25 0-.46.18-.5.42l-.37 2.65c-.63.25-1.17.59-1.69.98l-2.49-1c-.22-.09-.49 0-.61.22l-2 3.46c-.13.22-.07.49.12.64L4.57 11c-.04.34-.07.67-.07 1 0 .33.03.65.07.97l-2.11 1.66c-.19.15-.25.42-.12.64l2 3.46c.12.22.39.3.61.22l2.49-1.01c.52.4 1.06.74 1.69.99l.37 2.65c.04.24.25.42.5.42h4c.25 0 .46-.18.5-.42l.37-2.65c.63-.26 1.17-.59 1.69-.99l2.49 1.01c.22.08.49 0 .61-.22l2-3.46c.12-.22.07-.49-.12-.64l-2.11-1.66z"/></svg>
    Configurar Addon
  </a>
  <div class="footer">Conexão via MTProto • <a href="{{ base_url }}/health">Status do servidor</a></div>
</div></body></html>"""

TEMPLATE_DISCONNECTED = """<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Arvio Stream — Configuração</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:radial-gradient(ellipse at top,#1a1a2e 0%,#0f0f1a 100%);
  color:#eee;min-height:100vh;display:flex;align-items:center;justify-content:center;
  padding:20px;line-height:1.6}
.container{background:rgba(22,33,62,.7);backdrop-filter:blur(10px);
  border:1px solid rgba(233,69,96,.15);border-radius:16px;padding:40px;
  max-width:520px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.4)}
.back{display:inline-flex;align-items:center;gap:6px;color:#888;
  text-decoration:none;font-size:13px;margin-bottom:20px}
.back:hover{color:#e94560}
h1{color:#e94560;text-align:center;margin:0 0 10px;font-size:28px}
.subtitle{text-align:center;color:#888;margin-bottom:30px;font-size:14px}
.btn{display:block;width:100%;padding:16px;background:linear-gradient(135deg,#e94560,#c72e4a);
  color:#fff;border:none;border-radius:10px;font-size:16px;font-weight:600;
  cursor:pointer;transition:all .2s;box-shadow:0 4px 16px rgba(233,69,96,.3)}
.btn:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(233,69,96,.45)}
.btn:disabled{background:#555;cursor:not-allowed;box-shadow:none;transform:none}
.qr-container{text-align:center;margin:20px 0;display:none}
.qr-container.visible{display:block}
.qr-code{max-width:280px;margin:0 auto;background:#fff;padding:12px;border-radius:12px}
.status{text-align:center;padding:15px;border-radius:10px;margin:15px 0;display:none;font-size:14px}
.status.visible{display:block}
.status.waiting{background:rgba(15,52,96,.7);color:#4da6ff;border:1px solid rgba(77,166,255,.2)}
.status.success{background:rgba(15,52,96,.7);color:#4dff4d;border:1px solid rgba(77,255,77,.2)}
.status.error{background:rgba(61,15,26,.7);color:#ff4d4d;border:1px solid rgba(255,77,77,.2)}
.input-group{display:none;margin-top:15px}
.input-group.visible{display:block}
input[type=password]{width:100%;padding:14px;border:1px solid rgba(255,255,255,.1);
  border-radius:10px;background:rgba(15,15,26,.6);color:#eee;font-size:15px;margin-bottom:10px}
input[type=password]:focus{outline:none;border-color:#e94560}
.btn-secondary{background:linear-gradient(135deg,#0f3460,#1a4a80)}
</style></head><body>
<div class="container">
  <a href="{{ base_url }}/" class="back">← Voltar</a>
  <h1>Configuração</h1>
  <p class="subtitle">Conecte sua conta do Telegram via QR code.<br>Nada é armazenado no servidor.</p>
  <button class="btn" id="qrBtn" onclick="startQr()">Conectar via QR Code</button>
  <div class="qr-container" id="qrContainer">
    <p style="color:#aaa;font-size:13px;margin-bottom:10px">Escaneie com o app do Telegram</p>
    <img class="qr-code" id="qrImage" src="" alt="QR Code">
  </div>
  <div class="status" id="status"></div>
  <div class="input-group" id="pwGroup">
    <input type="password" id="pw" placeholder="Senha de verificação em duas etapas">
    <button class="btn btn-secondary" id="pwBtn" onclick="submit2fa()">Confirmar</button>
  </div>
</div>
<script>
let poll=null;
let pollStartTime=0;
const POLL_MAX_MS = 5*60*1000;
function bust(url){return url + (url.indexOf('?')>=0?'&':'?') + 'v=' + Date.now()}
function showStatus(m,t){const el=document.getElementById('status');el.textContent=m;el.className='status visible '+t}
function stopPoll(){if(poll){clearInterval(poll);poll=null}}
function startPoll(){
  if(poll)return;
  pollStartTime=Date.now();
  poll=setInterval(()=>{
    if(Date.now()-pollStartTime>POLL_MAX_MS){stopPoll();showStatus('Tempo esgotado. Recarregue a página.','error');return}
    fetch(bust('{{ base_url }}/configure/qr_poll'),{cache:'no-store'})
      .then(r=>r.json()).then(d=>{
      if(d.status==='waiting'){
        if(d.qr_url){
          const src='https://api.qrserver.com/v1/create-qr-code/?size=280x280&data='+encodeURIComponent(d.qr_url);
          const img=document.getElementById('qrImage');
          if(img.src!==src){img.src=src;document.getElementById('qrContainer').className='qr-container visible'}
        }
        showStatus('Escaneie o QR code com o Telegram','waiting');
      } else if(d.status==='success'&&d.token){
        stopPoll();
        showStatus('Conectado! Redirecionando...','success');
        setTimeout(()=>{window.location.href='{{ base_url }}/configure?token='+encodeURIComponent(d.token)+'&v='+Date.now()},1200);
      } else if(d.status==='2fa'||d.needs_2fa){
        stopPoll();
        showStatus('Verificação em duas etapas necessária','waiting');
        document.getElementById('pwGroup').className='input-group visible';
        document.getElementById('qrBtn').style.display='none';
        document.getElementById('qrContainer').className='qr-container';
      } else if(d.status==='waiting_2fa'){
        showStatus('Verificando senha...','waiting');
      } else if(d.status==='error'){
        stopPoll();
        showStatus(d.error||'Erro no login.','error');
        document.getElementById('qrBtn').disabled=false;
        document.getElementById('qrBtn').textContent='Tentar novamente';
      } else if(d.status==='cancelled'){
        stopPoll();
        showStatus('Login cancelado.','error');
        document.getElementById('qrBtn').disabled=false;
        document.getElementById('qrBtn').textContent='Tentar novamente';
      }
    }).catch(()=>{});
  },1500);
}
function startQr(){
  document.getElementById('qrBtn').disabled=true;
  document.getElementById('qrBtn').textContent='Iniciando...';
  fetch(bust('{{ base_url }}/configure/qr_start'),{method:'POST',cache:'no-store'})
    .then(r=>r.json()).then(d=>{
      if(d.success){
        showStatus('Escaneie o QR code com o Telegram','waiting');
        if(d.qr_url){
          document.getElementById('qrImage').src='https://api.qrserver.com/v1/create-qr-code/?size=280x280&data='+encodeURIComponent(d.qr_url);
          document.getElementById('qrContainer').className='qr-container visible';
        }
        startPoll();
      } else {
        showStatus(d.error||'Erro ao iniciar QR.','error');
        document.getElementById('qrBtn').disabled=false;
        document.getElementById('qrBtn').textContent='Tentar novamente';
      }
    }).catch(e=>{
      showStatus('Erro: '+e.message,'error');
      document.getElementById('qrBtn').disabled=false;
      document.getElementById('qrBtn').textContent='Tentar novamente';
    });
}
function submit2fa(){
  const pw=document.getElementById('pw').value;
  if(!pw)return;
  document.getElementById('pwBtn').disabled=true;
  fetch(bust('{{ base_url }}/configure/2fa_start'),{
    method:'POST',
    cache:'no-store',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({password:pw})
  }).then(r=>r.json()).then(d=>{
    if(d.success){
      showStatus('Verificando senha...','waiting');
      startPoll();
    } else {
      showStatus(d.error||'Senha incorreta.','error');
      document.getElementById('pwBtn').disabled=false;
    }
  }).catch(()=>{
    showStatus('Erro de rede.','error');
    document.getElementById('pwBtn').disabled=false;
  });
}
</script></body></html>"""

TEMPLATE_CONNECTED = """<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Arvio Stream — Conectado</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:radial-gradient(ellipse at top,#1a1a2e 0%,#0f0f1a 100%);
  color:#eee;min-height:100vh;display:flex;align-items:center;justify-content:center;
  padding:20px;line-height:1.6}
.container{background:rgba(22,33,62,.7);backdrop-filter:blur(10px);
  border:1px solid rgba(77,255,77,.2);border-radius:16px;padding:40px;
  max-width:600px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.4)}
.back{display:inline-flex;align-items:center;gap:6px;color:#888;
  text-decoration:none;font-size:13px;margin-bottom:20px}
.back:hover{color:#e94560}
h1{color:#e94560;text-align:center;margin:0 0 20px;font-size:28px}
.status.connected{background:rgba(15,52,96,.7);color:#4dff4d;padding:16px;
  border-radius:10px;text-align:center;margin-bottom:24px;
  border:1px solid rgba(77,255,77,.2);font-weight:600}
.manifest-label{font-size:12px;color:#888;text-transform:uppercase;
  letter-spacing:1px;margin-bottom:8px}
.manifest-url{background:rgba(15,52,96,.6);padding:16px;border-radius:10px;
  word-break:break-all;font-family:'SF Mono',Monaco,monospace;font-size:11px;
  color:#4da6ff;margin-bottom:20px;border:1px solid rgba(77,166,255,.2);
  line-height:1.5;max-height:120px;overflow-y:auto}
.btn-group{display:flex;gap:10px;justify-content:center;flex-wrap:wrap}
.btn{padding:12px 24px;background:linear-gradient(135deg,#e94560,#c72e4a);color:#fff;
  border:none;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer;transition:all .2s;
  text-decoration:none;display:inline-block}
.btn:hover{transform:translateY(-1px)}
.btn-secondary{background:linear-gradient(135deg,#0f3460,#1a4a80)}
.step{background:rgba(15,15,26,.4);padding:14px;border-radius:10px;
  margin-bottom:16px;font-size:13px;color:#aaa;border-left:3px solid #e94560}
.step strong{color:#eee;display:block;margin-bottom:4px}
</style></head><body>
<div class="container">
  <a href="{{ base_url }}/" class="back">← Voltar</a>
  <h1>Conectado</h1>
  <div class="status connected">✓ Conta do Telegram conectada</div>
  <div class="step"><strong>1. Copie a URL do manifest abaixo</strong>
    Sua sessão está criptografada no token — nada é salvo no servidor.</div>
  <div class="manifest-label">Manifest URL</div>
  <div class="manifest-url" id="murl">{{ manifest_url }}</div>
  <div class="step"><strong>2. Cole no Stremio</strong>
    Abra o Stremio → Addons → Adicionar addon → Cole a URL acima.
    Ou clique em "Instalar no Stremio" abaixo.</div>
  <div class="btn-group">
    <button class="btn btn-secondary" onclick="copyUrl()">📋 Copiar URL</button>
    <button class="btn" onclick="installStremio()">▶ Instalar no Stremio</button>
    <button class="btn btn-secondary" onclick="disconnect()">Desconectar</button>
  </div>
</div>
<script>
function copyUrl(){
  navigator.clipboard.writeText(document.getElementById('murl').textContent)
    .then(()=>alert('URL copiada! Cole no Stremio.'))
    .catch(()=>{
      const r=document.createRange();r.selectNode(document.getElementById('murl'));
      window.getSelection().removeAllRanges();window.getSelection().addRange(r);
      alert('Selecione manualmente e copie.');
    });
}
function installStremio(){
  const manifestUrl = document.getElementById('murl').textContent.trim();
  const deep = 'stremio://' + manifestUrl.replace(/^https?:\\/\\//, '');
  window.location.href = deep;
}
function disconnect(){
  if(!confirm('Remover token (desconectar)?'))return;
  window.location.href='{{ base_url }}/configure?v='+Date.now();
}
</script></body></html>"""


# ==================================================================
# Entry point
# ==================================================================
def run_addon(host: str = "0.0.0.0", port: int = 8138, debug: bool = False):
    print(f"[arvio] backend AES: {_CRYPTO_BACKEND}")
    if _CRYPTO_BACKEND == "pyaes":
        print("[arvio] AVISO: cryptg não instalado — velocidade limitada. "
              "Rode: pip install cryptg")
    print(f"[arvio] pipeline: {PIPELINE_DEPTH} x {MT_CHUNK//1024}KB")
    print(f"[arvio] landing: http://{host}:{port}/")
    _app.run(host=host, port=port, debug=debug, threaded=True)


if __name__ == "__main__":
    run_addon(host="0.0.0.0", port=int(os.environ.get("PORT", 8138)), debug=False)
