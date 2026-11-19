# -*- coding: utf-8 -*-
"""Arvio Stream - Stremio Addon (single file, stateless, pipelined proxy)."""

from __future__ import annotations

import asyncio
import base64
import difflib
import hashlib
import json
import math
import mimetypes
import os
import re
import sys
import unicodedata
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
TELEGRAM_SEARCH_TIMEOUT_S = int(os.environ.get("ARVIO_SEARCH_TIMEOUT", 45))
ARVIO_GLOBAL_SEARCH_LIMIT = int(os.environ.get("ARVIO_GLOBAL_SEARCH_LIMIT", 50))
ARVIO_DIALOG_SEARCH_LIMIT = int(os.environ.get("ARVIO_DIALOG_SEARCH_LIMIT", 40))
ARVIO_HISTORY_SCAN_LIMIT = int(os.environ.get("ARVIO_HISTORY_SCAN_LIMIT", 350))
ARVIO_MAX_RESULTS = int(os.environ.get("ARVIO_MAX_RESULTS", 30))

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


def _token_data(token: str | None) -> dict | None:
    return _decrypt(token) if token else None


def _session_from(token: str | None) -> str | None:
    data = _token_data(token)
    return data.get("s") if data else None


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


def _matches_requested_episode(video: dict, season_no: int, episode_no: int) -> bool:
    """Mantém somente arquivos identificáveis como o episódio solicitado."""
    text = f"{video.get('fileName', '')} {video.get('caption', '')}"
    norm = _normalize_match_text(text)

    # Formas comuns: S01E02, S01 EP02, 1x02, T1E2, Season 1 Episode 2,
    # Temporada 1 Episódio 2 e Episode 2 Season 1. A normalização anterior
    # converte pontos, sublinhados e hífens em espaços.
    patterns = (
        r"(?<![a-z0-9])s\s*0*(\d{1,2})\s*e(?:p(?:isode|isodio)?)?\s*0*(\d{1,3})(?!\d)",
        r"(?<![a-z0-9])t\s*0*(\d{1,2})\s*e(?:p)?\s*0*(\d{1,3})(?!\d)",
        r"(?<![a-z0-9])(\d{1,2})\s*x\s*0*(\d{1,3})(?!\d)",
        r"\b(?:season|temporada|temp)\s*0*(\d{1,2})\b.{0,35}?\b(?:episode|episodio|epis|ep|e)\s*0*(\d{1,3})\b",
        r"\b(?:episode|episodio|epis|ep)\s*0*(\d{1,3})\b.{0,35}?\b(?:season|temporada|temp)\s*0*(\d{1,2})\b",
    )
    found_explicit_marker = False
    for index, pattern in enumerate(patterns):
        for match in re.finditer(pattern, norm):
            found_explicit_marker = True
            first, second = map(int, match.groups())
            found_season, found_episode = (
                (second, first) if index == 4 else (first, second)
            )
            if found_season == season_no and found_episode == episode_no:
                return True
    if found_explicit_marker:
        return False

    # Mantém a compatibilidade com uploads antigos que não incluem temporada
    # e só identificam o episódio (E02, EP02, Episode 02 ou número isolado).
    episode_only = r"(?<![a-z0-9])(?:episode|episodio|epis|ep|e)\s*0*(\d{1,3})(?!\d)"
    if re.search(episode_only, norm):
        return any(int(m.group(1)) == episode_no for m in re.finditer(episode_only, norm))
    return bool(re.search(rf"(?<![a-z0-9])0*{episode_no}(?![0-9])", norm))


def _matches_requested_title(video: dict, aliases: list[str]) -> bool:
    """Aceita título completo, palavras-chave ou sigla no nome ou legenda."""
    if not aliases:
        return True
    text = f"{video.get('fileName', '')} {video.get('caption', '')}"
    norm = _normalize_match_text(text)
    blob_words = norm.split()
    blob_tokens = set(blob_words)
    compact_blob = "".join(blob_words)
    stopwords = {
        "the", "a", "an", "and", "of", "to", "la", "el", "los", "las",
        "as", "os", "de", "da", "do", "dos", "das", "e",
    }

    for alias in aliases:
        alias_norm = _normalize_match_text(alias)
        alias_words = alias_norm.split()
        if not alias_words:
            continue
        if f" {alias_norm} " in f" {norm} ":
            return True

        # Aceita títulos colados, como TheAmericans, além de iniciais com ou
        # sem pontuação: Two and a Half Men, TAAHM e T.A.A.H.M.
        compact_alias = "".join(alias_words)
        if len(compact_alias) >= 5 and compact_alias in compact_blob:
            return True

        meaningful = [word for word in alias_words if word not in stopwords]
        if meaningful:
            hits = sum(word in blob_tokens for word in meaningful)
            needed = 1 if len(meaningful) == 1 else max(2, (len(meaningful) + 1) // 2)
            if hits >= needed:
                return True

        initials = [word[0] for word in alias_words if word]
        meaningful_initials = [word[0] for word in meaningful if word]
        for letters in (initials, meaningful_initials):
            acronym = "".join(letters)
            if len(acronym) < 3:
                continue
            if acronym in blob_tokens:
                return True
            for start in range(max(0, len(blob_words) - len(letters) + 1)):
                if blob_words[start:start + len(letters)] == letters:
                    return True
    return False



@_app.route("/stream/<content_type>/<content_id>.json")
def streams_json(content_type: str, content_id: str):
    token = request.args.get("token")
    token_data = _token_data(token)
    session = token_data.get("s") if token_data else None
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
    meta_aliases = (meta or {}).get("aliases") or []
    if isinstance(meta_aliases, str):
        meta_aliases = [meta_aliases]
    aliases = list(dict.fromkeys(
        str(value).strip() for value in (title, alt, *meta_aliases)
        if value and str(value).strip()
    ))

    # A sigla ajuda em nomes longos (Two and a Half Men -> TAAHM). Mantemos
    # também a forma pontuada usada em alguns nomes de arquivo.
    acronyms = []
    for alias in aliases:
        words = re.findall(r"[a-z0-9]+", _normalize_match_text(alias))
        if len(words) >= 3:
            acronym = "".join(word[0] for word in words)
            if len(acronym) >= 3:
                acronyms.extend((acronym, ".".join(acronym) + "."))

    episode_parts = content_id.split(":")
    season_no = episode_no = None
    if content_type == "series" and len(episode_parts) >= 3:
        try:
            season_no, episode_no = int(episode_parts[1]), int(episode_parts[2])
            if not (0 <= season_no <= 99 and 0 <= episode_no <= 999):
                season_no = episode_no = None
        except (TypeError, ValueError):
            season_no = episode_no = None

    episode_tags = []
    if season_no is not None:
        episode_tags = list(dict.fromkeys((
            f"S{season_no:02d}E{episode_no:02d}",
            f"{season_no}x{episode_no:02d}",
        )))

    # Mantém consultas em cada idioma do Cinemeta, com grafia original,
    # separadores alternativos e siglas antes das consultas mais genéricas.
    queries = []
    if episode_tags:
        queries.extend(f"{alias} {episode_tags[0]}" for alias in aliases)
    queries.extend(aliases)
    if episode_tags:
        queries.extend(f"{acronym} {episode_tags[0]}" for acronym in acronyms)
    for alias in aliases:
        queries.append(alias.replace(" ", "."))
    queries.extend(acronyms)
    for alias in aliases:
        if year:
            queries.append(f"{alias} {year}")
        queries.extend(f"{alias} {tag}" for tag in episode_tags[1:])
    for acronym in acronyms:
        queries.extend(f"{acronym} {tag}" for tag in episode_tags[1:])
    queries.extend(episode_tags)
    queries = [q for q in dict.fromkeys(queries) if q.strip()]
    if not queries:
        queries = [imdb_id]

    # Token antigo sem a chave "sources" continua pesquisando em todos os
    # diálogos. Depois que o usuário salva uma seleção, só esses IDs entram.
    selected_sources = token_data.get("sources") if "sources" in token_data else None

    try:
        videos = _search_telegram_multi(
            session, queries, imdb_id=imdb_id,
            selected_sources=selected_sources,
            search_aliases=aliases,
            season_no=season_no,
            episode_no=episode_no,
        )
        # Para séries, não misturar episódios vizinhos encontrados pela busca
        # ampla do título. Exige que o arquivo/caption identifique o episódio.
        if content_type == "series" and len(episode_parts) >= 3:
            try:
                season_no, episode_no = int(episode_parts[1]), int(episode_parts[2])
                videos = [
                    video for video in videos
                    if _matches_requested_episode(video, season_no, episode_no)
                    and _matches_requested_title(video, aliases)
                ]
            except (TypeError, ValueError):
                pass
    except Exception as e:
        print(f"[arvio] busca falhou: {e}")
        return jsonify({"streams": [], "error": str(e)}), 500

    videos = videos[:ARVIO_MAX_RESULTS]
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


VIDEO_EXTENSIONS = (
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts",
    ".m2ts", ".mts", ".mpg", ".mpeg", ".mpe", ".vob", ".wmv",
    ".flv", ".3gp", ".ogv", ".ogg", ".f4v", ".divx", ".mxf",
    ".asf", ".rm", ".rmvb", ".dv", ".mpv", ".m2v",
)

MIME_VIDEO_EXTENSIONS = {
    "video/mp4": ".mp4",
    "video/x-matroska": ".mkv",
    "application/x-matroska": ".mkv",
    "video/webm": ".webm",
    "video/x-msvideo": ".avi",
    "video/quicktime": ".mov",
    "video/mpeg": ".mpg",
    "video/mp2t": ".ts",
    "video/x-ms-wmv": ".wmv",
    "video/x-flv": ".flv",
    "video/3gpp": ".3gp",
    "video/ogg": ".ogv",
}


def _video_from_msg(msg, desc: dict) -> dict | None:
    media = getattr(msg, "media", None)
    if not isinstance(media, MessageMediaDocument):
        return None
    doc = media.document
    if not isinstance(doc, Document):
        return None

    mime = (getattr(doc, "mime_type", None) or "").lower().strip()
    name = _file_name(doc).strip()
    lower_name = name.lower()
    ext_ok = lower_name.endswith(VIDEO_EXTENSIONS)
    caption = getattr(msg, "message", "") or ""
    caption_ext = next((
        ext for ext in VIDEO_EXTENSIONS
        if re.search(re.escape(ext) + r"(?=$|[\s)\]}>,;])", caption.lower())
    ), "")

    attrs = getattr(doc, "attributes", []) or []
    has_video_attribute = any(
        isinstance(a, DocumentAttributeVideo) for a in attrs
    ) if DocumentAttributeVideo else False

    # Muitos grupos enviam vídeos como "application/octet-stream", ou sem
    # nome de arquivo. Atributo de vídeo do Telegram também é um sinal válido.
    if not (mime.startswith("video/") or mime == "application/x-matroska"
            or ext_ok or caption_ext or has_video_attribute):
        return None

    if not name:
        ext = caption_ext or MIME_VIDEO_EXTENSIONS.get(mime, ".mp4")
        name = f"video_{getattr(msg, 'id', 'telegram')}{ext}"
    if mime in ("", "application/octet-stream"):
        mime = mimetypes.guess_type(name)[0] or mime

    size = int(getattr(doc, "size", 0) or 0)
    return {
        "key": f"{desc.get('peerId')}|{msg.id}|{getattr(doc, 'id', 0)}|{size}",
        "fileName": name,
        "size": size,
        "mime": mime or mimetypes.guess_type(name)[0] or "video/mp4",
        "duration": _duration(doc),
        "caption": caption,
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


def _normalize_match_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower()
    # Pontuação, separadores e nomes no padrão release passam a ser espaços.
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _tokens(text: str) -> list[str]:
    return [t for t in _normalize_match_text(text).split() if len(t) >= 2]


def _score(video: dict, query: str, imdb_id: str) -> int:
    filename = video.get("fileName", "")
    caption = video.get("caption", "")
    blob_raw = f"{filename} {caption}"
    blob = _normalize_match_text(blob_raw)
    q = _normalize_match_text(query)
    q_tokens = _tokens(query)
    blob_tokens = set(_tokens(blob_raw))

    if not q_tokens:
        return 0
    # Termos isolados de episódio são usados para ampliar a busca no Telegram,
    # mas não podem pontuar sozinhos: S01E02 existe em inúmeras séries.
    if re.fullmatch(r"(?:s\d{1,2}e\d{1,3}|\d{1,2}x\d{1,3})", q):
        return 0

    score = 0
    if imdb_id and imdb_id.lower() in blob:
        score += 120
    if q and q in blob:
        score += 45
    # TAAHM também deve corresponder a T.A.A.H.M.
    if len(q_tokens) == 1 and len(q_tokens[0]) >= 3:
        compact_blob = re.sub(r"[^a-z0-9]", "", blob)
        if q_tokens[0] in compact_blob:
            score += 38

    hits = sum(1 for token in q_tokens if token in blob_tokens)
    coverage = hits / max(1, len(q_tokens))
    if hits == len(q_tokens):
        score += 40
    score += int(35 * coverage)

    # Correspondência aproximada para títulos com abreviações, pontuação
    # diferente ou pequenos erros de digitação. Exige semelhança alta.
    title_query = " ".join(t for t in q_tokens if not re.fullmatch(r"(19|20)\d{2}", t))
    title_blob = _normalize_match_text(filename)
    if title_query and title_blob:
        ratio = difflib.SequenceMatcher(None, title_query, title_blob).ratio()
        if ratio >= 0.72:
            score += int(25 * ratio)

    years = re.findall(r"\b(?:19|20)\d{2}\b", q)
    if years and any(year in blob for year in years):
        score += 12

    # Reforço de temporada/episódio somente quando o título ou a sigla também
    # aparece. Um S01E01 isolado não deve elevar qualquer série no ranking.
    episode_patterns = re.findall(r"\bs\d{1,2}\s*e\d{1,3}\b|\b\d{1,2}x\d{1,3}\b", q)
    if episode_patterns and any(
        re.sub(r"\s+", "", pat) in re.sub(r"\s+", "", blob)
        for pat in episode_patterns
    ):
        title_query_tokens = [
            token for token in q_tokens
            if not re.fullmatch(r"s\d{1,2}e\d{1,3}|\d{1,2}x\d{1,3}", token)
        ]
        title_hits = sum(token in blob_tokens for token in title_query_tokens)
        compact_title_query = "".join(title_query_tokens)
        title_match = bool(
            title_query_tokens
            and title_hits >= max(1, (len(title_query_tokens) + 1) // 2)
        )
        if compact_title_query and len(compact_title_query) >= 3:
            compact_blob = re.sub(r"[^a-z0-9]", "", blob)
            title_match = title_match or compact_title_query in compact_blob
        if title_match:
            score += 35

    if video.get("duration", 0) > 0:
        score += 2
    return score


async def _joined_scope(client) -> dict:
    """Lista grupos, supergrupos e canais aos quais a conta tem acesso."""
    scope = {}
    try:
        async for dialog in client.iter_dialogs(limit=None):
            entity = getattr(dialog, "entity", None)
            if (entity is None or getattr(entity, "deleted", False)
                    or getattr(entity, "left", False)):
                continue
            if not isinstance(entity, (types.Channel, types.Chat)):
                continue

            desc = _peer_desc(entity)
            peer_id = desc.get("peerId")
            if not peer_id:
                continue

            if isinstance(entity, types.Chat):
                kind = "grupo"
            elif getattr(entity, "broadcast", False):
                kind = "canal"
            else:
                kind = "supergrupo"

            title = (
                getattr(dialog, "title", None)
                or getattr(entity, "title", None)
                or f"Telegram {peer_id}"
            )
            scope[peer_id] = {
                "entity": entity,
                "desc": desc,
                "title": str(title),
                "username": getattr(entity, "username", None) or "",
                "kind": kind,
            }
    except Exception as exc:
        print(f"[arvio] scope: {exc}")
    return scope


async def _list_sources_async(session: str) -> list[dict]:
    client = _new_client(session)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("Sessão expirada. Reconecte em /configure.")
        scope = await _joined_scope(client)
        sources = []
        for peer_id, entry in scope.items():
            sources.append({
                "id": peer_id,
                "title": entry.get("title") or "Sem título",
                "type": entry.get("kind") or "grupo",
                "username": entry.get("username") or "",
            })
        sources.sort(key=lambda item: (item["title"] or "").casefold())
        return sources
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _search_global(client, query: str, scope: dict,
                         limit: int = ARVIO_GLOBAL_SEARCH_LIMIT) -> list[dict]:
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
    for result in results:
        if not result or isinstance(result, BaseException):
            continue
        for msg in getattr(result, "messages", []) or []:
            try:
                signed = str(utils.get_peer_id(msg.peer_id))
            except Exception:
                signed = ""
            entry = scope.get(signed)
            if not entry:
                continue
            video = _video_from_msg(msg, entry["desc"])
            if video and video["key"] not in seen:
                seen.add(video["key"])
                out.append(video)
    return out


async def _search_per_dialog(client, query: str, scope: dict,
                             limit: int = ARVIO_DIALOG_SEARCH_LIMIT) -> list[dict]:
    if not scope:
        return []
    sem = asyncio.Semaphore(4)
    out: list[dict] = []
    seen = set()

    async def scan(entry):
        entity = entry.get("entity")
        if entity is None:
            return []
        local = []
        try:
            async with sem:
                async for msg in client.iter_messages(
                    entity, search=query, limit=limit
                ):
                    video = _video_from_msg(msg, entry["desc"])
                    if video:
                        local.append(video)
        except Exception as exc:
            print(f"[arvio] busca local ignorada em {entry.get('title')}: {exc}")
        return local

    tasks = [asyncio.ensure_future(scan(entry)) for entry in scope.values()]
    try:
        for future in asyncio.as_completed(tasks):
            try:
                found = await future
            except Exception:
                found = []
            for video in found:
                if video["key"] not in seen:
                    seen.add(video["key"])
                    out.append(video)
                    if len(out) >= limit * 4:
                        break
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return out


async def _search_by_filename(client, tokens: list[str], scope: dict,
                              limit: int = 45) -> list[dict]:
    if not scope or not tokens:
        return []
    sem = asyncio.Semaphore(3)
    out: list[dict] = []
    seen = set()
    key_tokens = [t for t in tokens if len(t) >= 3][:7]

    async def scan(entry):
        entity = entry.get("entity")
        if entity is None:
            return []
        local = []
        try:
            async with sem:
                # DocumentFilter inclui arquivos enviados como documento, não
                # apenas vídeos enviados com a opção "Enviar como vídeo".
                async for msg in client.iter_messages(
                    entity, limit=ARVIO_HISTORY_SCAN_LIMIT,
                ):
                    video = _video_from_msg(msg, entry["desc"])
                    if not video:
                        continue
                    haystack = set(_tokens(
                        f"{video['fileName']} {video.get('caption', '')}"
                    ))
                    hits = sum(1 for token in key_tokens if token in haystack)
                    if hits >= max(1, min(2, len(key_tokens))):
                        local.append(video)
                        if len(local) >= 8:
                            break
        except Exception as exc:
            print(f"[arvio] varredura de histórico ignorada em {entry.get('title')}: {exc}")
        return local

    tasks = [asyncio.ensure_future(scan(entry)) for entry in scope.values()]
    try:
        for future in asyncio.as_completed(tasks):
            try:
                found = await future
            except Exception:
                found = []
            for video in found:
                if video["key"] not in seen:
                    seen.add(video["key"])
                    out.append(video)
                    if len(out) >= limit:
                        return out
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return out


async def _search_async(session: str, queries: list[str], imdb_id: str,
                        selected_sources: list[str] | None = None,
                        search_aliases: list[str] | None = None,
                        season_no: int | None = None,
                        episode_no: int | None = None) -> list[dict]:
    client = _new_client(session)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("Sessão expirada. Reconecte em /configure.")

        scope = await _joined_scope(client)
        if selected_sources is not None:
            selected_ids = {str(value) for value in selected_sources}
            scope = {
                peer_id: entry for peer_id, entry in scope.items()
                if peer_id in selected_ids
            }

        print(f"[arvio] escopo de busca: {len(scope)} grupo(s)/canal(is)")
        if not scope:
            return []

        all_results: dict[str, dict] = {}
        scores: dict[str, int] = {}

        def absorb(videos: list[dict], query: str):
            for video in videos:
                key = video["key"]
                if key not in all_results:
                    all_results[key] = video
                    scores[key] = 0
                scores[key] = max(scores[key], _score(video, query, imdb_id))

        def exact_episode_found() -> bool:
            if season_no is None or episode_no is None:
                return False
            return any(
                _matches_requested_title(video, search_aliases or [])
                and _matches_requested_episode(video, season_no, episode_no)
                for video in all_results.values()
            )

        # Preserva a busca global do Telegram, filtrando o retorno para as
        # fontes selecionadas quando existe uma seleção salva.
        for query in queries[:6]:
            found = await _search_global(client, query, scope)
            absorb(found, query)

        if len(all_results) < 8 or (
            season_no is not None and not exact_episode_found()
        ):
            for query in queries[:4]:
                found = await _search_per_dialog(client, query, scope)
                absorb(found, query)

        title_query = next(
            (query for query in queries if query and not re.fullmatch(
                r"S\d{1,2}E\d{1,3}|\d{1,2}x\d{1,3}", query, re.I
            )),
            queries[0] if queries else "",
        )
        tokens = _tokens(title_query)
        # A busca do Telegram pode não localizar mensagens pelo atributo de
        # nome do documento quando ele foi enviado sem legenda. Com fontes
        # escolhidas, inspeciona os nomes recentes em toda busca de episódio,
        # mesmo que já tenha encontrado outra cópia. Sem seleção explícita,
        # mantém a varredura como fallback para evitar percorrer todos os chats.
        if tokens and (
            (season_no is not None and selected_sources is not None)
            or len(all_results) < 20
        ):
            found = await _search_by_filename(client, tokens, scope)
            absorb(found, title_query)

        ordered = sorted(
            all_results.values(),
            key=lambda video: scores[video["key"]],
            reverse=True,
        )
        # Limite mais baixo evita que resultados apenas vagamente relacionados
        # apareçam antes de títulos corretos.
        filtered = [
            video for video in ordered
            if scores[video["key"]] >= 18
            or (
                season_no is not None and episode_no is not None
                and _matches_requested_title(video, search_aliases or [])
                and _matches_requested_episode(video, season_no, episode_no)
            )
        ]
        # A filtragem final de título/episódio acontece na rota Stremio. Guarda
        # uma margem aqui para resultados globais vagos não expulsarem os
        # arquivos corretos antes dessa validação.
        return filtered[:ARVIO_MAX_RESULTS * 5]
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

_SEARCH_CACHE: dict[str, tuple[float, list[dict]]] = {}
_SEARCH_CACHE_LOCK = threading.Lock()
_SEARCH_CACHE_TTL = max(0, int(os.environ.get("ARVIO_SEARCH_CACHE_TTL", 60)))
_SEARCH_CACHE_MAX = 128


def _search_telegram_multi(session: str, queries: list[str], imdb_id: str = "",
                           selected_sources: list[str] | None = None,
                           search_aliases: list[str] | None = None,
                           season_no: int | None = None,
                           episode_no: int | None = None) -> list[dict]:
    # O Stremio pode repetir a mesma consulta. Um cache curto reduz tráfego e
    # latência durante a vida da instância. A chave nunca guarda sessão aberta.
    key_data = json.dumps({
        "session": hashlib.sha256(session.encode()).hexdigest(),
        "queries": queries,
        "imdb": imdb_id,
        "sources": sorted(map(str, selected_sources)) if selected_sources is not None else None,
        "aliases": search_aliases or [],
        "season": season_no,
        "episode": episode_no,
    }, sort_keys=True, separators=(",", ":"))
    cache_key = hashlib.sha256(key_data.encode()).hexdigest()
    now = time.monotonic()
    if _SEARCH_CACHE_TTL:
        with _SEARCH_CACHE_LOCK:
            cached = _SEARCH_CACHE.get(cache_key)
            if cached and now - cached[0] < _SEARCH_CACHE_TTL:
                return list(cached[1])
            if cached:
                _SEARCH_CACHE.pop(cache_key, None)

    result = _run_sync(
        lambda: _search_async(
            session, queries, imdb_id, selected_sources,
            search_aliases, season_no, episode_no,
        ),
        timeout=TELEGRAM_SEARCH_TIMEOUT_S + 60,
    )
    if _SEARCH_CACHE_TTL:
        with _SEARCH_CACHE_LOCK:
            if len(_SEARCH_CACHE) >= _SEARCH_CACHE_MAX:
                oldest = min(_SEARCH_CACHE, key=lambda key: _SEARCH_CACHE[key][0])
                _SEARCH_CACHE.pop(oldest, None)
            _SEARCH_CACHE[cache_key] = (time.monotonic(), list(result))
    return result


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
# Gerenciador de grupos e canais
# ==================================================================
@_app.route("/configure/sources", methods=["GET"])
def configure_sources_list():
    token = request.args.get("token", "")
    token_data = _token_data(token)
    session = token_data.get("s") if token_data else None
    if not session:
        return jsonify({"success": False, "error": "not_connected"}), 401

    try:
        sources = _run_sync(
            lambda: _list_sources_async(session),
            timeout=TELEGRAM_SEARCH_TIMEOUT_S + 45,
        )
        selected = token_data.get("sources") if "sources" in token_data else None
        for source in sources:
            source["selected"] = (
                True if selected is None else source["id"] in set(map(str, selected))
            )
        return jsonify({
            "success": True,
            "sources": sources,
            "selected_count": len(sources) if selected is None else len(
                set(map(str, selected)) & {item["id"] for item in sources}
            ),
            "selection_mode": "all" if selected is None else "custom",
        })
    except Exception as exc:
        print(f"[arvio] listagem de fontes falhou: {exc}")
        return jsonify({"success": False, "error": str(exc)}), 500


@_app.route("/configure/sources/save", methods=["POST"])
def configure_sources_save():
    data = request.get_json(silent=True) or {}
    token = str(data.get("token") or "")
    token_data = _token_data(token)
    session = token_data.get("s") if token_data else None
    if not session:
        return jsonify({"success": False, "error": "not_connected"}), 401

    requested = data.get("sources")
    if not isinstance(requested, list):
        return jsonify({"success": False, "error": "Lista de fontes inválida."}), 400
    requested = list(dict.fromkeys(str(value) for value in requested if value is not None))

    try:
        available = _run_sync(
            lambda: _list_sources_async(session),
            timeout=TELEGRAM_SEARCH_TIMEOUT_S + 45,
        )
        available_ids = {item["id"] for item in available}
        selected = sorted(set(requested) & available_ids)

        # A seleção fica dentro do token criptografado; não exige banco de
        # dados nem grava a sessão Telegram no armazenamento do servidor.
        new_token = _token_for(session, {"sources": selected})
        manifest_url = (
            f"{_base_url()}/manifest.json?token={urllib.parse.quote(new_token)}"
        )
        return jsonify({
            "success": True,
            "manifest_url": manifest_url,
            "selected_count": len(selected),
            "available_count": len(available_ids),
            "ignored_count": len(set(requested) - available_ids),
        })
    except Exception as exc:
        print(f"[arvio] salvamento de fontes falhou: {exc}")
        return jsonify({"success": False, "error": str(exc)}), 500


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
            token=token,
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
.source-panel{background:rgba(15,15,26,.45);border:1px solid rgba(255,255,255,.08);
  border-radius:12px;padding:16px;margin:22px 0;text-align:left}
.source-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.source-head strong{color:#eee;font-size:15px}
.source-actions{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.source-actions button{border:1px solid rgba(255,255,255,.12);background:#18233b;color:#ddd;
  padding:8px 11px;border-radius:8px;cursor:pointer;font-size:12px}
.source-actions button:hover{border-color:#e94560}
.source-search{width:100%;padding:11px 12px;border-radius:8px;
  border:1px solid rgba(255,255,255,.12);background:#0f0f1a;color:#eee;margin:8px 0 10px}
.source-list{max-height:310px;overflow:auto;display:flex;flex-direction:column;gap:7px}
.source-item{display:flex;align-items:center;gap:10px;padding:9px 10px;
  background:rgba(255,255,255,.035);border-radius:8px;font-size:13px;cursor:pointer}
.source-item input{accent-color:#e94560;width:16px;height:16px;flex:0 0 auto}
.source-meta{display:block;color:#888;font-size:11px}
.source-count{color:#aaa;font-size:12px;margin-top:10px}
.source-message{color:#aaa;font-size:12px;margin-top:10px;min-height:18px}
.source-save{width:100%;margin-top:12px}
@media(max-width:520px){.container{padding:22px}.btn-group .btn{width:100%}}
</style></head><body>
<div class="container">
  <a href="{{ base_url }}/" class="back">← Voltar</a>
  <h1>Conectado</h1>
  <div class="status connected">✓ Conta do Telegram conectada</div>

  <section class="source-panel">
    <div class="source-head">
      <strong>Fontes do Telegram</strong>
      <span id="sourceMode" class="source-meta">Carregando grupos e canais...</span>
    </div>
    <p class="source-meta" style="margin-top:5px">
      Escolha em quais grupos, supergrupos e canais o Arvio Stream deve procurar.
      Fontes privadas aparecem quando sua conta tem acesso a elas.
    </p>
    <input id="sourceSearch" class="source-search" type="search"
      placeholder="Filtrar por nome..." oninput="renderSources()">
    <div class="source-actions">
      <button type="button" onclick="selectAllSources()">Selecionar todos</button>
      <button type="button" onclick="clearSources()">Limpar seleção</button>
      <button type="button" onclick="loadSources()">Atualizar lista</button>
    </div>
    <div id="sourceList" class="source-list"></div>
    <div id="sourceCount" class="source-count"></div>
    <div id="sourceMessage" class="source-message"></div>
    <button type="button" class="btn source-save" id="saveSources"
      onclick="saveSources()">Salvar fontes selecionadas</button>
  </section>

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
const ARVIO_TOKEN = {{ token|tojson }};
let sourceItems = [];
let selectedSourceIds = new Set();
let currentManifestUrl = document.getElementById('murl').textContent.trim();

function sourceMessage(message, isError){
  const el=document.getElementById('sourceMessage');
  el.textContent=message||'';
  el.style.color=isError?'#ff8585':'#aaa';
}
function updateSourceCount(){
  document.getElementById('sourceCount').textContent =
    selectedSourceIds.size + ' fonte(s) selecionada(s) de ' + sourceItems.length;
}
function renderSources(){
  const root=document.getElementById('sourceList');
  const filter=(document.getElementById('sourceSearch').value||'').toLocaleLowerCase();
  root.replaceChildren();
  const visible=sourceItems.filter(item =>
    (item.title+' '+item.type+' '+(item.username||'')).toLocaleLowerCase().includes(filter)
  );
  if(!visible.length){
    const empty=document.createElement('div');
    empty.className='source-meta';
    empty.textContent=sourceItems.length?'Nenhuma fonte corresponde à pesquisa.':'Nenhuma fonte encontrada.';
    root.appendChild(empty);
  }
  visible.forEach(item=>{
    const label=document.createElement('label');
    label.className='source-item';
    const checkbox=document.createElement('input');
    checkbox.type='checkbox';
    checkbox.checked=selectedSourceIds.has(item.id);
    checkbox.addEventListener('change',()=>{
      if(checkbox.checked)selectedSourceIds.add(item.id);
      else selectedSourceIds.delete(item.id);
      updateSourceCount();
    });
    const text=document.createElement('span');
    text.textContent=item.title;
    const meta=document.createElement('small');
    meta.className='source-meta';
    meta.textContent=item.type+(item.username?' • @'+item.username:'');
    text.appendChild(meta);
    label.append(checkbox,text);
    root.appendChild(label);
  });
  updateSourceCount();
}
async function loadSources(){
  const mode=document.getElementById('sourceMode');
  mode.textContent='Consultando Telegram...';
  sourceMessage('');
  document.getElementById('saveSources').disabled=true;
  try{
    const response=await fetch('{{ base_url }}/configure/sources?token='+
      encodeURIComponent(ARVIO_TOKEN)+'&v='+Date.now(),{cache:'no-store'});
    const data=await response.json();
    if(!response.ok||!data.success)throw new Error(data.error||'Não foi possível listar as fontes.');
    sourceItems=data.sources||[];
    selectedSourceIds=new Set(sourceItems.filter(x=>x.selected).map(x=>x.id));
    mode.textContent=sourceItems.length+' fontes disponíveis';
    renderSources();
    sourceMessage(data.selection_mode==='all'
      ?'Todas as fontes estão selecionadas inicialmente. Ajuste e salve sua seleção.'
      :'Seleção personalizada carregada.');
  }catch(error){
    mode.textContent='Falha ao carregar';
    sourceMessage(error.message,true);
  }finally{
    document.getElementById('saveSources').disabled=false;
  }
}
function selectAllSources(){
  selectedSourceIds=new Set(sourceItems.map(item=>item.id));
  renderSources();
}
function clearSources(){
  selectedSourceIds.clear();
  renderSources();
}
async function saveSources(){
  const button=document.getElementById('saveSources');
  button.disabled=true;
  button.textContent='Salvando...';
  sourceMessage('');
  try{
    const response=await fetch('{{ base_url }}/configure/sources/save',{
      method:'POST',cache:'no-store',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({token:ARVIO_TOKEN,sources:Array.from(selectedSourceIds)})
    });
    const data=await response.json();
    if(!response.ok||!data.success)throw new Error(data.error||'Falha ao salvar seleção.');
    currentManifestUrl=data.manifest_url;
    document.getElementById('murl').textContent=currentManifestUrl;
    sourceMessage(data.selected_count+' fonte(s) salva(s). Use a nova URL do manifest no Stremio.');
  }catch(error){
    sourceMessage(error.message,true);
  }finally{
    button.disabled=false;
    button.textContent='Salvar fontes selecionadas';
  }
}
loadSources();

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
