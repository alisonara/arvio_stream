# -*- coding: utf-8 -*-
"""Telegram integration: QR auth, search and byte-range proxy.

Important lifecycle rule: Kodi invokes a pluginsource again when a list item is
clicked. Search results therefore contain only stable Telegram identifiers.
The message is reloaded and the local proxy is started inside the playback
invocation, avoiding cross-invocation in-memory state.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import http.server
import math
import os
import re
import shutil
import tempfile
import sys
import threading
import time
import urllib.parse

from . import storage
from . import telegram_accel
from .progressive_spool import ProgressiveSpool
from .kodi_utils import Kodi
from .const import (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SEARCH_TIMEOUT_S,
                    TELEGRAM_MAX_RESULTS, TG_PROXY_PORT, TG_PROXY_CHUNK_BYTES,
                    TG_TELETHON_REQUEST_BYTES, TG_TELETHON_STREAM_BYTES,
                    TG_TELETHON_PIPELINE_CHUNKS, TG_TELETHON_CONNECTIONS_FREE,
                    TG_TELETHON_CONNECTIONS_PREMIUM, TG_PROXY_GRACE_S,
                    TG_SEARCH_FALLBACK_CONCURRENCY_DESKTOP,
                    TG_SEARCH_FALLBACK_CONCURRENCY_LOW_RESOURCE,
                    TG_ADAPTIVE_SAMPLE_BYTES_DESKTOP, TG_ADAPTIVE_SAMPLE_BYTES_LOW_RESOURCE,
                    TG_ADAPTIVE_SAFETY_RATIO, TG_ADAPTIVE_FORCE_FULL_RATIO,
                    TG_ADAPTIVE_FAST_HEADROOM_S, TG_ADAPTIVE_NORMAL_HEADROOM_S,
                    TG_ADAPTIVE_TIGHT_HEADROOM_S, TG_ADAPTIVE_EDGE_HEADROOM_S,
                    TG_ADAPTIVE_DEFICIT_EXTRA_S, TG_SPOOL_READ_BYTES,
                    TG_TEMP_RESERVE_MIN_BYTES, TG_TEMP_RESERVE_FRACTION,
                    TG_TEMP_ORPHAN_MAX_AGE_S)

_TELEGRAM_DEP_OK = False
try:
    from telethon import TelegramClient, functions, types, utils  # type: ignore
    from telethon.sessions import StringSession  # type: ignore
    from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo  # type: ignore
    _TELEGRAM_DEP_OK = True
except Exception:
    TelegramClient = StringSession = functions = types = utils = None  # type: ignore
    DocumentAttributeFilename = DocumentAttributeVideo = None  # type: ignore

_TELEGRAM_AES_BACKEND = telegram_accel.install_fast_aes() if _TELEGRAM_DEP_OK else "unavailable"


class TelegramUnavailableError(RuntimeError):
    pass


class TelegramPasswordNeeded(RuntimeError):
    pass


class TelegramOperationCancelled(RuntimeError):
    """Benign cancellation/timeout surfaced as a normal Exception.

    ``asyncio.CancelledError`` derives from ``BaseException`` on modern Python,
    so letting it escape a Kodi pluginsource worker produces a
    PythonToCppException.  Convert it at the sync boundary instead.
    """
    pass


# ---- transient asyncio runtime ---------------------------------------------
# Kodi executes pluginsource routes in short-lived Python invocations and may
# place them on worker/Dummy threads.  Telethon is deliberately NOT kept alive
# in a background event-loop thread: each network operation owns a fresh loop
# and a fresh client built from the persisted StringSession, then disconnects.
# This avoids cross-loop/cross-interpreter state, which can terminate Kodi
# outright before Python has a chance to emit a traceback.
_pending_2fa_session = None
_pending_2fa_lock = threading.Lock()
_auth_state = {"k": "idle"}
_auth_state_lock = threading.Lock()
_crypto_logged = False


def _set_state(state):
    global _auth_state
    with _auth_state_lock:
        _auth_state = dict(state)


def get_state():
    with _auth_state_lock:
        return dict(_auth_state)


def has_saved_session():
    return storage.has_telegram_session()


def is_connected():
    # Durable state between Kodi invocations is the StringSession only.
    return has_saved_session()


def me_info():
    state = get_state()
    if state.get("k") != "ready":
        return None
    return {"firstName": state.get("firstName", ""), "userId": state.get("userId", "")}


def _new_client(session_string=None):
    global _crypto_logged
    if not _TELEGRAM_DEP_OK:
        raise TelegramUnavailableError("Telethon não disponível nesta instalação do Kodi.")
    if not _crypto_logged:
        Kodi.log("TELEGRAM", "Backend AES MTProto: %s" % _TELEGRAM_AES_BACKEND, level="info")
        _crypto_logged = True
    session = StringSession(session_string) if session_string else StringSession()
    return TelegramClient(session, TELEGRAM_API_ID, TELEGRAM_API_HASH,
                          timeout=8, request_retries=2, connection_retries=1,
                          auto_reconnect=False, receive_updates=False)


def _ready_state(me):
    return {"k": "ready", "firstName": getattr(me, "first_name", "") or "",
            "userId": str(getattr(me, "id", "") or "")}


def _run_loop_here(coro_factory, timeout=None):
    """Execute one coroutine on a loop owned by the current thread only.

    Cancellation is a normal lifecycle event in Kodi (navigation can replace an
    in-flight pluginsource invocation).  Never let ``asyncio.CancelledError``
    cross the synchronous plugin boundary as ``BaseException``.
    """
    if sys.platform == "win32" and hasattr(asyncio, "SelectorEventLoop"):
        loop = asyncio.SelectorEventLoop()
    else:
        loop = asyncio.new_event_loop()
    # During interpreter teardown, asyncio's default handler may try to repr a
    # half-destroyed Task and emit a secondary ``NoneType is not callable``
    # error.  All tasks below are explicitly awaited/cancelled, so keep the
    # per-operation loop handler quiet.
    try:
        loop.set_exception_handler(lambda _loop, _context: None)
    except Exception:
        pass
    try:
        asyncio.set_event_loop(loop)
        coro = coro_factory()
        if timeout is not None:
            coro = asyncio.wait_for(coro, timeout=float(timeout))
        try:
            return loop.run_until_complete(coro)
        except asyncio.CancelledError as exc:
            raise TelegramOperationCancelled("Operação Telegram cancelada durante navegação/encerramento.") from exc
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Operação Telegram excedeu o tempo limite.") from exc
    finally:
        try:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except BaseException:
            pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except BaseException:
            pass
        # Force Telethon/asyncio generators to finalize while this loop is
        # still alive. Real Kodi logs showed a Queue.get generator being
        # collected only after loop.close(), which produced noisy
        # "Unhandled error while receiving data / Event loop is closed".
        try:
            import gc
            for _ in range(2):
                gc.collect()
                loop.run_until_complete(asyncio.sleep(0))
        except BaseException:
            pass
        try:
            loop.close()
        except BaseException:
            pass
        try:
            asyncio.set_event_loop(None)
        except BaseException:
            pass


def _run_sync(coro_factory, timeout=None):
    """Run a fresh coroutine without retaining asyncio state between calls.

    If called from a thread that already owns a running loop, use a one-shot
    helper thread and join it before returning. No asyncio thread survives the
    operation or Kodi plugin invocation. A loop-affinity RuntimeError is
    retried once from a completely new coroutine/client factory.
    """
    if not callable(coro_factory):
        raise TypeError("_run_sync requer uma fábrica de coroutine")

    def run_once():
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is None or not running.is_running():
            return _run_loop_here(coro_factory, timeout)

        box = {}
        done = threading.Event()

        def worker():
            try:
                box["value"] = _run_loop_here(coro_factory, timeout)
            except BaseException as exc:
                box["error"] = exc
            finally:
                done.set()

        thread = threading.Thread(target=worker, name="ArvioTelegramOneShot", daemon=False)
        thread.start()
        done.wait(None if timeout is None else float(timeout) + 5.0)
        thread.join(timeout=1.0)
        if thread.is_alive():
            raise TimeoutError("Operação Telegram não encerrou no tempo esperado.")
        if "error" in box:
            raise box["error"]
        return box.get("value")

    for attempt in range(2):
        try:
            return run_once()
        except RuntimeError as exc:
            text = str(exc).lower()
            loop_error = ("different loop" in text or "event loop" in text or
                          "already awaited coroutine" in text or "another loop" in text)
            if attempt == 0 and loop_error:
                Kodi.log("TELEGRAM", "Afinidade asyncio inválida; repetindo operação limpa: %s" % exc,
                         level="warning")
                continue
            raise


async def _disconnect_quiet(client):
    """Disconnect completely even if the caller itself is being cancelled.

    Telethon's public ``disconnect`` shields its internal task.  If the outer
    Kodi operation is cancelled at that exact moment, the shield can be
    cancelled while the real MTProto recv task keeps running until our
    one-shot loop is closed.  That produced the harmless-but-noisy
    ``Unhandled error while receiving data / Event loop is closed`` seen in
    the real Kodi log.  Because Telethon is vendored with the addon, prefer its
    coroutine implementation so we can await the actual cleanup task.
    """
    if client is None:
        return
    task = None
    try:
        private = getattr(client, "_disconnect_coro", None)
        if callable(private):
            task = asyncio.ensure_future(private())
        else:
            result = client.disconnect()
            if hasattr(result, "__await__"):
                task = asyncio.ensure_future(result)
        if task is not None:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Cancellation of the search/navigation must not abandon the
                # underlying network cleanup. Finish it before closing the
                # one-shot event loop, then let the caller handle cancellation.
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=3.0)
                except BaseException:
                    if not task.done():
                        task.cancel()
                    try:
                        await asyncio.gather(task, return_exceptions=True)
                    except BaseException:
                        pass
        # Give connection callbacks multiple loop turns to drain before
        # _run_loop_here performs final GC and loop.close().
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    except BaseException:
        if task is not None and not task.done():
            try:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            except BaseException:
                pass


async def _authorized_client(saved=None):
    session_string = saved if saved is not None else storage.load_telegram_session()
    if not session_string:
        raise RuntimeError("Conta do Telegram não conectada.")
    client = _new_client(session_string)
    await client.connect()
    if not await client.is_user_authorized():
        await _disconnect_quiet(client)
        storage.clear_telegram_session()
        _set_state({"k": "idle"})
        raise RuntimeError("Sessão Telegram expirada. Conecte a conta novamente.")
    return client


async def _qr_login_async(on_qr, cancel_event=None):
    global _pending_2fa_session
    from telethon.errors import SessionPasswordNeededError
    client = _new_client()
    try:
        await client.connect()
        qr_login = await client.qr_login()
        if on_qr:
            on_qr(qr_login.url)
        wait_task = asyncio.ensure_future(
            qr_login.wait(timeout=float(TELEGRAM_SEARCH_TIMEOUT_S * 12)))
        try:
            while not wait_task.done():
                if cancel_event is not None and cancel_event.is_set():
                    wait_task.cancel()
                    try:
                        await wait_task
                    except BaseException:
                        pass
                    raise RuntimeError("Login Telegram cancelado.")
                await asyncio.sleep(0.20)
            await wait_task
        except SessionPasswordNeededError:
            pending = client.session.save()
            with _pending_2fa_lock:
                _pending_2fa_session = pending
            _set_state({"k": "waitPassword"})
            raise TelegramPasswordNeeded("Senha de verificação em duas etapas necessária.")
        me = await client.get_me()
        if not me:
            raise RuntimeError("Falha ao obter conta do Telegram.")
        session_string = client.session.save()
        storage.save_telegram_session(session_string)
        with _pending_2fa_lock:
            _pending_2fa_session = None
        _set_state(_ready_state(me))
        return session_string, me.first_name or "", str(me.id or "")
    finally:
        await _disconnect_quiet(client)


def start_qr_login(on_qr, cancel_event=None):
    if not _TELEGRAM_DEP_OK:
        raise TelegramUnavailableError("Telethon não disponível nesta instalação do Kodi.")
    return _run_sync(lambda: _qr_login_async(on_qr, cancel_event), timeout=float(TELEGRAM_SEARCH_TIMEOUT_S * 12 + 15))


async def _complete_2fa_async(password, pending_session):
    client = _new_client(pending_session)
    try:
        await client.connect()
        me = await client.sign_in(password=password)
        if not me:
            me = await client.get_me()
        if not me:
            raise RuntimeError("Falha ao concluir a autenticação em duas etapas.")
        session_string = client.session.save()
        storage.save_telegram_session(session_string)
        _set_state(_ready_state(me))
        return session_string, me.first_name or "", str(me.id or "")
    finally:
        await _disconnect_quiet(client)


def complete_2fa_password(password):
    global _pending_2fa_session
    password = (password or "").strip()
    if not password:
        raise RuntimeError("Senha vazia.")
    with _pending_2fa_lock:
        pending = _pending_2fa_session
    if not pending:
        raise RuntimeError("Não há login Telegram aguardando senha.")
    try:
        result = _run_sync(lambda: _complete_2fa_async(password, pending), timeout=45)
    except Exception:
        _set_state({"k": "waitPassword"})
        raise
    with _pending_2fa_lock:
        _pending_2fa_session = None
    return result


def cancel_pending_login():
    global _pending_2fa_session
    with _pending_2fa_lock:
        _pending_2fa_session = None
    if get_state().get("k") == "waitPassword":
        _set_state({"k": "idle"})


async def _validate_saved_session_async():
    client = await _authorized_client()
    try:
        me = await client.get_me()
        if not me:
            _set_state({"k": "error"})
            return False
        _set_state(_ready_state(me))
        return True
    finally:
        await _disconnect_quiet(client)


def restore_session():
    if not _TELEGRAM_DEP_OK or not has_saved_session():
        return False
    try:
        return bool(_run_sync(lambda: _validate_saved_session_async(), timeout=30))
    except Exception as exc:
        Kodi.log("TELEGRAM", "Falha ao validar sessão: %s" % exc, level="warning")
        return False


async def _logout_saved_async(saved):
    client = _new_client(saved)
    try:
        await client.connect()
        try:
            await client.log_out()
        except Exception:
            pass
    finally:
        await _disconnect_quiet(client)


def disconnect():
    global _pending_2fa_session
    saved = storage.load_telegram_session()
    if saved:
        try:
            _run_sync(lambda: _logout_saved_async(saved), timeout=20)
        except Exception as exc:
            Kodi.log("TELEGRAM", "Logout remoto não concluído; limpando sessão local: %s" % exc, level="debug")
    storage.clear_telegram_session()
    with _pending_2fa_lock:
        _pending_2fa_session = None
    _set_state({"k": "idle"})
    stop_proxy()


def shutdown_runtime(stop_http=True):
    """Compatibility hook for addon.py.

    There is no persistent Telethon runtime anymore.  Keep only the optional
    local HTTP proxy shutdown; StringSession persistence is intentionally kept.
    """
    if stop_http:
        try:
            stop_proxy()
        except Exception:
            pass


def clear_session_local():
    disconnect()


# ---- search ---------------------------------------------------------------
def document_file_name(doc, api=None):
    for attr in getattr(doc, "attributes", []):
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name
    return ""


def _document_duration(doc):
    for attr in getattr(doc, "attributes", []):
        if isinstance(attr, DocumentAttributeVideo):
            try:
                return int(attr.duration or 0)
            except (TypeError, ValueError):
                return 0
    return 0


def _peer_descriptor(entity):
    """Return a serializable InputPeer descriptor for the next Kodi invocation."""
    if entity is None:
        return {}
    try:
        signed = str(utils.get_peer_id(entity))
    except Exception:
        signed = ""
    raw_id = getattr(entity, "id", None)
    access_hash = getattr(entity, "access_hash", None)
    if isinstance(entity, types.Channel):
        kind = "channel"
    elif isinstance(entity, types.User):
        kind = "user"
    elif isinstance(entity, types.Chat):
        kind = "chat"
    else:
        kind = ""
    return {"peerId": signed, "peerType": kind,
            "peerRawId": str(raw_id) if raw_id is not None else "",
            "accessHash": str(access_hash) if access_hash is not None else ""}


def _video_from_message(msg, peer_descriptor=None):
    media = getattr(msg, "media", None)
    if not isinstance(media, types.MessageMediaDocument):
        return None
    doc = media.document
    if not isinstance(doc, types.Document):
        return None
    mime = (doc.mime_type or "").lower()
    name = document_file_name(doc)
    video_ext = str(name or "").lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v", ".ts"))
    if not (mime.startswith("video/") or mime in ("application/x-matroska", "video/x-matroska") or video_ext):
        return None
    name = name or ("Default_Name.mp4" if mime == "video/mp4" else "Default_Name.mkv")
    peer_descriptor = dict(peer_descriptor or {})
    if not peer_descriptor.get("peerId"):
        try:
            peer_descriptor["peerId"] = str(utils.get_peer_id(msg.peer_id))
        except Exception:
            peer_descriptor["peerId"] = ""
    return {
        "key": "%s|%s|%s|%s" % (peer_descriptor.get("peerId"), msg.id, name, doc.size or 0),
        "fileName": name, "size": int(doc.size or 0), "mime": mime,
        "caption": msg.message or "", "duration": _document_duration(doc),
        "peerId": peer_descriptor.get("peerId") or "",
        "peerType": peer_descriptor.get("peerType") or "",
        "peerRawId": peer_descriptor.get("peerRawId") or "",
        "accessHash": peer_descriptor.get("accessHash") or "",
        "messageId": int(msg.id),
        # Keep the proxy payload plain/serializable across its HTTP worker thread.
        "documentId": int(getattr(doc, "id", 0) or 0),
        "documentAccessHash": int(getattr(doc, "access_hash", 0) or 0),
        "documentDcId": int(getattr(doc, "dc_id", 0) or 0),
        "fileReference": bytes(getattr(doc, "file_reference", b"") or b""),
    }

async def _joined_media_scope(client):
    """Return only channels/groups the connected account currently has as dialogs.

    Results from Telegram global search are accepted only when their peer is in
    this scope.  Private users/bots and arbitrary public channels outside the
    account's dialog list are never exposed as Arvio video sources.
    """
    scope = {}
    try:
        async for dialog in client.iter_dialogs(limit=None):
            entity = getattr(dialog, "entity", None)
            if entity is None or getattr(entity, "deleted", False) or getattr(entity, "left", False):
                continue
            if not isinstance(entity, (types.Channel, types.Chat)):
                continue
            desc = _peer_descriptor(entity)
            peer_id = desc.get("peerId") or ""
            if peer_id:
                scope[peer_id] = {"entity": entity, "descriptor": desc}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        Kodi.log("TELEGRAM", "Falha ao carregar canais/grupos inscritos: %s" % exc, level="warning")
    Kodi.log("TELEGRAM", "Escopo de busca: %d canal(is)/grupo(s) inscrito(s)." % len(scope), level="info")
    return scope


async def _search_saved_session_async(query, limit):
    client = await _authorized_client()
    try:
        scope = await _joined_media_scope(client)
        return await _search_hybrid(client, query, limit, scope=scope)
    finally:
        await _disconnect_quiet(client)


def search_video_messages(query, limit=None):
    if not _TELEGRAM_DEP_OK:
        raise TelegramUnavailableError("Telethon não disponível.")
    limit = max(1, int(limit or TELEGRAM_MAX_RESULTS))
    return _run_sync(lambda: _search_saved_session_async(query, limit),
                     timeout=float(TELEGRAM_SEARCH_TIMEOUT_S + 30))


async def _search_many_saved_session_async(queries, limit_per_query, max_total):
    """Search strongest title variants concurrently in subscribed dialogs only."""
    client = await _authorized_client()
    try:
        scope = await _joined_media_scope(client)
        if not scope:
            return []
        out, seen = [], set()
        clean_queries = []
        seen_q = set()
        for value in queries or []:
            value = str(value or "").strip()
            key = value.casefold()
            if value and key not in seen_q:
                seen_q.add(key)
                clean_queries.append(value)
        # The old path could execute up to eight global searches serially. The
        # first variants are the strongest (exact title/year/episode); run at
        # most three together on the same client, then validate/rank in router.
        primary_queries = clean_queries[:3]
        tasks = [asyncio.ensure_future(_search_global(client, query, limit_per_query, scope=scope))
                 for query in primary_queries]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
        except asyncio.CancelledError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            Kodi.log("TELEGRAM", "Busca Cinemeta cancelada; preservando resultados parciais.", level="debug")
            return out
        for found in results:
            if isinstance(found, BaseException):
                continue
            for item in found or []:
                key = item.get("key")
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(item)
                if len(out) >= max_total:
                    return out[:max_total]
        # If the filtered global search is sparse, ask Telegram *inside the
        # subscribed dialogs only* using server-side text search. This avoids
        # downloading/scanning the latest 80 messages from every dialog.
        if len(out) < min(5, max_total) and clean_queries:
            fallback_limit = min(max_total, max(limit_per_query, 25))
            try:
                out = (await _search_dialogs_fallback(
                    client, clean_queries[0], fallback_limit, out, scope=scope))[:max_total]
            except asyncio.CancelledError:
                Kodi.log("TELEGRAM", "Fallback inscrito cancelado; preservando %d resultado(s)." % len(out),
                         level="debug")
        return out[:max_total]
    finally:
        await _disconnect_quiet(client)


def search_video_messages_many(queries, limit_per_query=20, max_total=60):
    if not _TELEGRAM_DEP_OK:
        raise TelegramUnavailableError("Telethon não disponível.")
    limit_per_query = max(1, min(50, int(limit_per_query or 20)))
    max_total = max(1, min(100, int(max_total or 60)))
    return _run_sync(
        lambda: _search_many_saved_session_async(queries, limit_per_query, max_total),
        timeout=float(TELEGRAM_SEARCH_TIMEOUT_S + 30))


def _search_tokens(text):
    normalized = re.sub(r"[^\w\s]", " ", str(text or "").lower(), flags=re.UNICODE)
    return [token for token in re.split(r"\s+", normalized) if len(token) >= 2]


async def _search_global(client, query, limit, scope=None):
    """Fast server-side search, strictly filtered to subscribed channel/group peers."""
    from telethon.tl.types import InputMessagesFilterVideo, InputMessagesFilterDocument
    scope = scope if isinstance(scope, dict) else await _joined_media_scope(client)
    if not scope:
        return []

    async def one_filter(msg_filter):
        try:
            return await client(functions.messages.SearchGlobalRequest(
                q=query, filter=msg_filter, min_date=0, max_date=0,
                offset_rate=0, offset_peer=types.InputPeerEmpty(), offset_id=0,
                limit=min(max(1, int(limit)), 100)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            Kodi.log("TELEGRAM", "Busca global inscrita parcial falhou: %s" % exc, level="warning")
            return None

    # Video and generic-document filters are independent requests; execute them
    # together instead of doubling latency for every title variant.
    results = await asyncio.gather(
        one_filter(InputMessagesFilterVideo()), one_filter(InputMessagesFilterDocument()),
        return_exceptions=True)
    seen, out = set(), []
    for result in results:
        if result is None or isinstance(result, BaseException):
            continue
        for msg in getattr(result, "messages", []) or []:
            try:
                signed = str(utils.get_peer_id(msg.peer_id))
            except Exception:
                signed = ""
            entry = scope.get(signed)
            if not entry:
                # Hard privacy/scope gate: public/global result outside a joined
                # channel/group is ignored even if Telegram returned it.
                continue
            video = _video_from_message(msg, entry.get("descriptor"))
            if not video or video["key"] in seen:
                continue
            seen.add(video["key"])
            out.append(video)
            if len(out) >= limit:
                return out
    return out


def runtime_profile():
    """Return conservative runtime characteristics without platform-specific paths."""
    android = False
    total_memory_mb = 0
    try:
        import xbmc  # type: ignore
        android = bool(xbmc.getCondVisibility("System.Platform.Android"))
        label = str(xbmc.getInfoLabel("System.Memory(total)") or "")
        match = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(GB|MB)?", label, re.I)
        if match:
            value = float(match.group(1).replace(",", "."))
            unit = (match.group(2) or "MB").upper()
            total_memory_mb = int(value * 1024 if unit == "GB" else value)
    except Exception:
        pass
    low_resource = bool(android and (not total_memory_mb or total_memory_mb <= 2560))
    return {"android": android, "low_resource": low_resource, "memory_mb": total_memory_mb}


def recommended_probe_bytes():
    return (int(TG_ADAPTIVE_SAMPLE_BYTES_LOW_RESOURCE)
            if runtime_profile().get("low_resource") else int(TG_ADAPTIVE_SAMPLE_BYTES_DESKTOP))


def _search_fallback_concurrency():
    if runtime_profile().get("low_resource"):
        return max(1, int(TG_SEARCH_FALLBACK_CONCURRENCY_LOW_RESOURCE))
    return max(1, int(TG_SEARCH_FALLBACK_CONCURRENCY_DESKTOP))


async def _search_dialogs_fallback(client, query, limit, existing=None, scope=None):
    """Server-side per-dialog fallback restricted to subscribed channels/groups."""
    tokens = _search_tokens(query)
    if not tokens:
        return list(existing or [])
    scope = scope if isinstance(scope, dict) else await _joined_media_scope(client)
    out = list(existing or [])
    seen = {item.get("key") for item in out if item.get("key")}
    if not scope or len(out) >= limit:
        return out[:limit]

    semaphore = asyncio.Semaphore(_search_fallback_concurrency())

    async def search_entry(entry):
        entity = entry.get("entity")
        descriptor = entry.get("descriptor") or {}
        local = []
        if entity is None:
            return local
        try:
            async with semaphore:
                # Telegram performs the text search remotely inside this exact
                # dialog; only matching messages are transferred to Kodi.
                async for msg in client.iter_messages(entity, search=query, limit=8):
                    video = _video_from_message(msg, descriptor)
                    if video:
                        local.append(video)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            Kodi.log("TELEGRAM", "Busca em canal/grupo inscrito falhou: %s" % exc, level="debug")
        return local

    tasks = [asyncio.ensure_future(search_entry(entry)) for entry in scope.values()]
    try:
        for future in asyncio.as_completed(tasks):
            try:
                found = await future
            except asyncio.CancelledError:
                raise
            except Exception:
                found = []
            for video in found:
                key = video.get("key")
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(video)
                if len(out) >= limit:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    return out[:limit]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except BaseException:
                pass
    return out[:limit]


async def _search_hybrid(client, query, limit, scope=None):
    scope = scope if isinstance(scope, dict) else await _joined_media_scope(client)
    global_results = await _search_global(client, query, limit, scope=scope)
    if len(global_results) >= min(limit, 5):
        return global_results[:limit]
    return (await _search_dialogs_fallback(
        client, query, limit, global_results, scope=scope))[:limit]

def _build_input_peer(peer_id, peer_type="", peer_raw_id="", access_hash=""):
    try:
        raw = int(peer_raw_id) if str(peer_raw_id) else None
        ah = int(access_hash) if str(access_hash) else None
    except (TypeError, ValueError):
        raw, ah = None, None
    if peer_type == "channel" and raw is not None and ah is not None:
        return types.InputPeerChannel(raw, ah)
    if peer_type == "user" and raw is not None and ah is not None:
        return types.InputPeerUser(raw, ah)
    if peer_type == "chat" and raw is not None:
        return types.InputPeerChat(raw)
    try:
        return int(peer_id)
    except (TypeError, ValueError):
        return peer_id


async def _fetch_message(client, peer_id, message_id, peer_type="", peer_raw_id="", access_hash=""):
    peer = _build_input_peer(peer_id, peer_type, peer_raw_id, access_hash)
    try:
        entity = await client.get_input_entity(peer)
    except Exception:
        entity = peer
    return await client.get_messages(entity, ids=int(message_id))


async def _fetch_video_saved_session_async(peer_id, message_id, peer_type="", peer_raw_id="", access_hash=""):
    client = await _authorized_client()
    try:
        msg = await _fetch_message(client, peer_id, message_id, peer_type, peer_raw_id, access_hash)
        if not msg:
            raise RuntimeError("Mensagem do Telegram não encontrada ou sem acesso.")
        descriptor = {"peerId": str(peer_id or ""), "peerType": peer_type or "",
                      "peerRawId": str(peer_raw_id or ""), "accessHash": str(access_hash or "")}
        video = _video_from_message(msg, descriptor)
        if not video:
            raise RuntimeError("A mensagem não contém um vídeo reproduzível.")
        return video
    finally:
        await _disconnect_quiet(client)


def prepare_stream_url(peer_id, message_id, peer_type="", peer_raw_id="", access_hash=""):
    """Fetch one message in a transient Telegram session, then start loopback proxy."""
    video = fetch_stream_video(peer_id, message_id, peer_type, peer_raw_id, access_hash)
    return register_stream_url(video), video


def fetch_stream_video(peer_id, message_id, peer_type="", peer_raw_id="", access_hash=""):
    """Reload a stable Telegram descriptor for playback without starting I/O proxy."""
    return _run_sync(
        lambda: _fetch_video_saved_session_async(peer_id, message_id, peer_type, peer_raw_id, access_hash),
        timeout=30)


def _mp4_duration_seconds(sample):
    """Best-effort duration from an MP4/MOV mvhd box in the probe prefix."""
    data = bytes(sample or b"")
    pos = 0
    # mvhd is normally inside moov. Searching the bounded probe prefix is safer
    # and cheaper than loading the media into memory or invoking an external tool.
    while True:
        idx = data.find(b"mvhd", pos)
        if idx < 4:
            return 0.0
        try:
            size = int.from_bytes(data[idx - 4:idx], "big")
            payload = idx + 4
            if size < 24 or payload + min(size, 40) > len(data) + 8:
                pos = idx + 4
                continue
            version = data[payload]
            if version == 1:
                if payload + 32 > len(data):
                    return 0.0
                timescale = int.from_bytes(data[payload + 20:payload + 24], "big")
                duration = int.from_bytes(data[payload + 24:payload + 32], "big")
            else:
                if payload + 20 > len(data):
                    return 0.0
                timescale = int.from_bytes(data[payload + 12:payload + 16], "big")
                duration = int.from_bytes(data[payload + 16:payload + 20], "big")
            if timescale > 0 and duration > 0:
                return float(duration) / float(timescale)
        except Exception:
            pass
        pos = idx + 4


def media_required_mbps(video, sample=None):
    try:
        size = float(video.get("size") or 0)
        duration = float(video.get("duration") or 0)
    except Exception:
        return 0.0
    if duration <= 0 and sample:
        duration = _mp4_duration_seconds(sample)
        if duration > 0:
            try:
                video["duration"] = duration
            except Exception:
                pass
            Kodi.log("TELEGRAM", "Duração MP4 inferida do probe: %.2fs." % duration, level="debug")
    if size <= 0 or duration <= 0:
        return 0.0
    return (size * 8.0) / (duration * 1000.0 * 1000.0)


def adaptive_spool_plan(video, probe):
    """Choose an initial disk lead; producer always continues to 100%."""
    total = int(video.get("size") or 0)
    sample = bytes((probe or {}).get("sample") or b"")
    measured = max(0.0, float((probe or {}).get("mbps") or 0.0))
    required = max(0.0, float((probe or {}).get("required_mbps") or media_required_mbps(video, sample)))
    if total <= 0:
        raise RuntimeError("Tamanho do vídeo Telegram inválido.")
    if required <= 0 or measured <= 0:
        return {"mode": "full", "target_bytes": total, "measured_mbps": measured,
                "required_mbps": required, "reason": "bitrate_desconhecido"}
    safety_required = required * float(TG_ADAPTIVE_SAFETY_RATIO)
    ratio = measured / max(0.001, required)
    safe_ratio = measured / max(0.001, safety_required)
    bytes_per_second = (required * 1000.0 * 1000.0) / 8.0

    if ratio < float(TG_ADAPTIVE_FORCE_FULL_RATIO):
        target = total
        mode = "full"
        reason = "fonte_muito_abaixo"
    elif measured < safety_required:
        deficit = max(0.0, 1.0 - safe_ratio)
        target = int(total * deficit + bytes_per_second * float(TG_ADAPTIVE_DEFICIT_EXTRA_S))
        target = max(len(sample), min(total, target))
        mode = "full" if target >= int(total * 0.95) else "progressive"
        if mode == "full":
            target = total
        reason = "fonte_abaixo_com_margem"
    else:
        if ratio >= 1.50:
            seconds = int(TG_ADAPTIVE_FAST_HEADROOM_S)
        elif ratio >= 1.20:
            seconds = int(TG_ADAPTIVE_NORMAL_HEADROOM_S)
        elif ratio >= 1.05:
            seconds = int(TG_ADAPTIVE_TIGHT_HEADROOM_S)
        else:
            seconds = int(TG_ADAPTIVE_EDGE_HEADROOM_S)
        target = max(len(sample), min(total, int(bytes_per_second * seconds)))
        mode = "full" if target >= total else "progressive"
        reason = "fonte_suficiente"
    return {"mode": mode, "target_bytes": int(target), "measured_mbps": measured,
            "required_mbps": required, "ratio": ratio, "reason": reason}


async def _connection_count(client):
    # Retained for diagnostics/legacy proxy only. Progressive 1.4.9 media spool
    # deliberately uses one ordered sender after the 1.4.8 real log exposed
    # corruption on the experimental multi-sender playback path.
    try:
        me = await client.get_me()
        if bool(getattr(me, "premium", False)):
            return max(1, min(4, int(TG_TELETHON_CONNECTIONS_PREMIUM)))
    except Exception:
        pass
    return max(1, min(4, int(TG_TELETHON_CONNECTIONS_FREE)))


async def _probe_download_speed_async(video, sample_bytes):
    """Measure the exact conservative sender used by the progressive spool."""
    client = await _authorized_client()
    try:
        total = int(video.get("size") or 0)
        wanted = min(max(256 * 1024, int(sample_bytes)), total or int(sample_bytes))
        wanted -= wanted % (256 * 1024)
        wanted = max(256 * 1024, wanted)
        location = _document_location(video)
        dc_id, file_size, _ = _download_context(video)
        started = time.monotonic()
        payload = await _fetch_mtproto_chunk(
            client, location, 0, wanted, _mtproto_steady_size(wanted), dc_id, file_size)
        elapsed = max(0.001, time.monotonic() - started)
        if not payload:
            raise RuntimeError("Telegram não entregou dados no diagnóstico de velocidade.")
        mbps = (len(payload) * 8.0) / (elapsed * 1000.0 * 1000.0)
        required = media_required_mbps(video, payload)
        Kodi.log(
            "TELEGRAM",
            "Diagnóstico de taxa segura: %.2f Mb/s; mídia=%.2f Mb/s; AES=%s; sample=%d." %
            (mbps, required, _TELEGRAM_AES_BACKEND, len(payload)),
            level="info")
        return {"sample": payload, "mbps": mbps, "required_mbps": required,
                "connections": 1, "mode": "single-safe"}
    finally:
        await _disconnect_quiet(client)


def probe_download_speed(video, sample_bytes=None):
    if sample_bytes is None:
        sample_bytes = recommended_probe_bytes()
    return _run_sync(lambda: _probe_download_speed_async(video, sample_bytes), timeout=90)


def _temp_media_dir():
    try:
        import xbmcvfs  # type: ignore
        base = xbmcvfs.translatePath("special://temp/arvio/telegram/")
    except Exception:
        base = os.path.join(tempfile.gettempdir(), "arvio", "telegram")
    base = base or os.path.join(tempfile.gettempdir(), "arvio", "telegram")
    os.makedirs(base, exist_ok=True)
    return base


def cleanup_orphan_temp_files():
    """Reclaim only stale crash leftovers; active playback cleanup is immediate."""
    base = _temp_media_dir()
    removed = 0
    cutoff = time.time() - float(TG_TEMP_ORPHAN_MAX_AGE_S)
    try:
        for entry in os.listdir(base):
            if not entry.startswith("arvio_telegram_"):
                continue
            candidate = os.path.join(base, entry)
            try:
                if os.path.isfile(candidate) and os.path.getmtime(candidate) < cutoff:
                    os.remove(candidate)
                    removed += 1
            except Exception:
                pass
    except Exception:
        pass
    if removed:
        Kodi.log("TELEGRAM", "Limpeza de temporários órfãos: %d arquivo(s)." % removed, level="info")
    return removed


def _temp_media_path(video):
    base = _temp_media_dir()
    cleanup_orphan_temp_files()
    name = str(video.get("fileName") or "video.mp4")
    ext = os.path.splitext(name)[1].lower()
    if not ext or len(ext) > 8:
        ext = ".mp4"
    token = _fingerprint("%s|%s|%s|%s" % (
        video.get("peerId"), video.get("messageId"), video.get("size"), time.time()))
    # .part is visible to housekeeping; keep media extension last for local
    # playback after a full download.
    return os.path.join(base, "arvio_telegram_%s.part%s" % (token, ext))


def _check_temp_space(path, total):
    usage = shutil.disk_usage(os.path.dirname(path))
    reserve = max(int(TG_TEMP_RESERVE_MIN_BYTES), int(float(usage.total) * float(TG_TEMP_RESERVE_FRACTION)))
    required = int(total) + reserve
    if usage.free < required:
        raise RuntimeError(
            "Espaço temporário insuficiente: vídeo=%d MB, livre=%d MB, reserva protegida=%d MB." %
            (int(total / (1024 * 1024)), int(usage.free / (1024 * 1024)), int(reserve / (1024 * 1024))))
    return {"free": usage.free, "total": usage.total, "reserve": reserve, "required": required}


async def _download_local_safe_async(video, path, prefix, progress_callback=None, cancel_check=None):
    """Sequential ordered producer: bounded RAM, one MTProto sender, disk-backed."""
    total = int(video.get("size") or 0)
    prefix = bytes(prefix or b"")[:total]
    client = await _authorized_client()
    done = len(prefix)
    started = time.monotonic()
    try:
        with open(path, "wb") as fh:
            if prefix:
                fh.write(prefix)
                fh.flush()
            if progress_callback is not None:
                progress_callback(done, total, 0.0, 1)
            location = _document_location(video)
            dc_id, file_size, _ = _download_context(video)
            remaining = max(0, total - done)
            request_size = _mtproto_steady_size(256 * 1024)
            if remaining > 0:
                limit = max(1, int(math.ceil(float(remaining) / float(request_size))))
                async for chunk in client.iter_download(
                        location, offset=int(done), request_size=int(request_size), limit=limit,
                        dc_id=dc_id, file_size=file_size):
                    if cancel_check is not None and cancel_check():
                        raise TelegramOperationCancelled("Download progressivo Telegram cancelado.")
                    if not chunk:
                        continue
                    take = bytes(chunk[:remaining])
                    if not take:
                        break
                    fh.write(take)
                    fh.flush()
                    done += len(take)
                    remaining -= len(take)
                    if progress_callback is not None:
                        elapsed = max(0.001, time.monotonic() - started)
                        transferred = max(0, done - len(prefix))
                        mbps = (transferred * 8.0) / (elapsed * 1000.0 * 1000.0) if transferred else 0.0
                        progress_callback(done, total, mbps, 1)
                    if remaining <= 0:
                        break
        actual = os.path.getsize(path) if os.path.exists(path) else 0
        if actual != total:
            raise RuntimeError("Download progressivo incompleto (%d/%d bytes)." % (actual, total))
        return path
    finally:
        await _disconnect_quiet(client)


def start_progressive_download(video, prefix=b""):
    total = int(video.get("size") or 0)
    if total <= 0:
        raise RuntimeError("Tamanho do vídeo Telegram inválido.")
    path = _temp_media_path(video)
    space = _check_temp_space(path, total)
    prefix = bytes(prefix or b"")[:total]

    def producer(spool):
        def report(done, total_value, mbps, connections):
            spool.report(done, total_value, mbps, connections)
        _run_sync(lambda: _download_local_safe_async(
            video, path, prefix, progress_callback=report, cancel_check=spool.cancelled), timeout=None)

    spool = ProgressiveSpool(path, total, producer, prefix_size=len(prefix)).start()
    profile = runtime_profile()
    Kodi.log(
        "TELEGRAM",
        "Spool progressivo iniciado: disco, 1 conexão ordenada, AES=%s, perfil=%s, reserva=%d MB." %
        (_TELEGRAM_AES_BACKEND,
         "android-low" if profile.get("low_resource") else ("android" if profile.get("android") else "desktop"),
         int(space["reserve"] / (1024 * 1024))),
        level="info")
    return spool


def stop_progressive_download(spool, remove=True):
    if spool is None:
        return True
    try:
        spool.cancel()
        spool.join(timeout=8.0)
    except Exception:
        pass
    if remove:
        path = getattr(spool, "path", "")
        for _ in range(12):
            try:
                if path and os.path.isfile(path):
                    os.remove(path)
                if not path or not os.path.exists(path):
                    return True
            except Exception:
                pass
            time.sleep(0.25)
        Kodi.log("TELEGRAM", "Temporário ainda bloqueado; ficará para limpeza órfã: %s" % path,
                 level="warning")
        return False
    return True


def download_video_local(video, prefix=b"", progress_callback=None, cancel_check=None):
    """Compatibility helper: complete the safe disk spool synchronously."""
    spool = start_progressive_download(video, prefix=prefix)
    try:
        spool.wait_for(int(video.get("size") or 0), cancel_check=cancel_check,
                       progress_callback=(lambda status, _target: progress_callback(
                           status["downloaded"], status["total"], status["rate_mbps"],
                           status["connections"]) if progress_callback else None))
        if not spool.join(timeout=15.0):
            raise RuntimeError("Pré-download completou bytes sem encerrar o produtor.")
        if spool.error is not None:
            raise spool.error
        return spool.path
    except BaseException:
        stop_progressive_download(spool, remove=True)
        raise


def cleanup_local_video(path):
    try:
        base = os.path.abspath(_temp_media_dir())
        candidate = os.path.abspath(str(path or ""))
        if candidate.startswith(base + os.sep) and os.path.isfile(candidate) and os.path.basename(candidate).startswith("arvio_telegram_"):
            os.remove(candidate)
            return True
    except Exception:
        pass
    return False


# ---- byte-range proxy -----------------------------------------------------
_index = {}
_spool_index = {}
_index_lock = threading.Lock()
_proxy_server = None
_proxy_thread = None
_proxy_port = None
_proxy_activity_lock = threading.Lock()
_proxy_active_requests = 0
_proxy_last_activity = 0.0
_proxy_stream_lock = threading.Lock()


def _fingerprint(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def register_stream_url(video):
    stream_id = _fingerprint("%s|%s|%s|%s" % (
        video.get("peerId"), video.get("messageId"), video.get("fileName"), video.get("size")))
    with _index_lock:
        _index[stream_id] = video
    port = _start_proxy()
    return "http://127.0.0.1:%d/stream/%s" % (port, stream_id)



def register_spool_url(video, spool):
    stream_id = _fingerprint("spool|%s|%s|%s|%s" % (
        video.get("peerId"), video.get("messageId"), video.get("fileName"), video.get("size")))
    with _index_lock:
        _spool_index[stream_id] = {"video": video, "spool": spool}
    port = _start_proxy()
    return "http://127.0.0.1:%d/stream/%s" % (port, stream_id)

def _start_proxy():
    global _proxy_server, _proxy_thread, _proxy_port
    if _proxy_server is not None:
        return _proxy_port
    handler = _make_handler()
    from http.server import ThreadingHTTPServer

    class ProxyHTTPServer(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = True
        block_on_close = False

        def handle_error(self, request, client_address):
            # Kodi/libcurl commonly closes a probe/seek socket as soon as it
            # has enough bytes.  Treat that as normal proxy lifecycle noise.
            Kodi.log("TELEGRAM", "Conexão HTTP local encerrada por %s" % (client_address,), level="debug")

    last_error = None
    for port in (TG_PROXY_PORT, TG_PROXY_PORT + 1, TG_PROXY_PORT + 2, 0):
        try:
            server = ProxyHTTPServer(("127.0.0.1", port), handler)
            actual = int(server.server_address[1])
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            _proxy_server, _proxy_thread, _proxy_port = server, thread, actual
            Kodi.log("TELEGRAM", "Proxy local iniciado na porta %d." % actual)
            return actual
        except OSError as exc:
            last_error = exc
    raise RuntimeError("Não foi possível iniciar o proxy Telegram: %s" % last_error)


def proxy_port():
    return _proxy_port


def _proxy_request_begin():
    global _proxy_active_requests, _proxy_last_activity
    import time
    with _proxy_activity_lock:
        _proxy_active_requests += 1
        _proxy_last_activity = time.time()


def _proxy_request_end():
    global _proxy_active_requests, _proxy_last_activity
    import time
    with _proxy_activity_lock:
        _proxy_active_requests = max(0, _proxy_active_requests - 1)
        _proxy_last_activity = time.time()


def proxy_activity():
    with _proxy_activity_lock:
        return {"active": int(_proxy_active_requests), "last": float(_proxy_last_activity or 0.0)}


def proxy_can_stop(grace_s=TG_PROXY_GRACE_S):
    import time
    state = proxy_activity()
    if state["active"] > 0:
        return False
    if state["last"] <= 0:
        return True
    return (time.time() - state["last"]) >= float(grace_s)


def stop_proxy():
    global _proxy_server, _proxy_thread, _proxy_port, _proxy_active_requests, _proxy_last_activity
    server, thread = _proxy_server, _proxy_thread
    _proxy_server, _proxy_thread, _proxy_port = None, None, None
    if server is not None:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass
    if thread is not None:
        try:
            thread.join(timeout=2)
        except Exception:
            pass
    with _index_lock:
        _index.clear()
        _spool_index.clear()
    with _proxy_activity_lock:
        _proxy_active_requests = 0
        _proxy_last_activity = 0.0


def _parse_range(value, total):
    if not value or not value.startswith("bytes="):
        return 0, total - 1, False
    spec = value[6:].split(",", 1)[0].strip()
    if "-" not in spec:
        raise ValueError("range")
    left, right = spec.split("-", 1)
    if not left:
        suffix = int(right)
        if suffix <= 0:
            raise ValueError("range")
        start = max(total - suffix, 0)
        return start, total - 1, True
    start = int(left)
    if start < 0 or start >= total:
        raise ValueError("range")
    end = int(right) if right else total - 1
    end = min(end, total - 1)
    if end < start:
        raise ValueError("range")
    return start, end, True


def _mime(video):
    mime = video.get("mime") or "video/mp4"
    return "video/x-matroska" if mime == "application/x-matroska" else mime


def _stream_spool_range(spool, start, length, writer):
    cursor = int(start)
    remaining = int(length)
    logged_wait = False
    with open(spool.path, "rb") as fh:
        while remaining > 0:
            status = spool.status()
            if status["error"] is not None:
                raise RuntimeError("Spool Telegram falhou: %s" % status["error"])
            available = int(status["downloaded"]) - cursor
            if available <= 0:
                if status["done"]:
                    break
                if status["cancelled"]:
                    raise TelegramOperationCancelled("Spool Telegram cancelado.")
                if not logged_wait:
                    Kodi.log("TELEGRAM", "Proxy aguardando spool à frente: offset=%d, baixado=%d." %
                             (cursor, int(status["downloaded"])), level="debug")
                    logged_wait = True
                spool.wait_for_growth(cursor, timeout=0.40)
                continue
            logged_wait = False
            take_size = min(int(TG_SPOOL_READ_BYTES), available, remaining)
            fh.seek(cursor)
            data = fh.read(take_size)
            if not data:
                spool.wait_for_growth(cursor, timeout=0.10)
                continue
            writer.write(data)
            writer.flush()
            cursor += len(data)
            remaining -= len(data)
    return int(length) - remaining


def _make_handler():
    class RangeHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            return

        def _entry(self):
            parsed = urllib.parse.urlparse(self.path)
            if not parsed.path.startswith("/stream/"):
                return None
            stream_id = parsed.path.rsplit("/", 1)[-1]
            with _index_lock:
                if stream_id in _spool_index:
                    return _spool_index.get(stream_id)
                video = _index.get(stream_id)
                return {"video": video, "spool": None} if video else None

        def _video(self):
            entry = self._entry()
            return entry.get("video") if entry else None

        def do_HEAD(self):
            video = self._video()
            if not video:
                self.send_error(410, "Stream expirou")
                return
            total = int(video.get("size") or 0)
            self.send_response(200)
            self.send_header("Content-Type", _mime(video))
            self.send_header("Content-Length", str(total))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Connection", "close")
            self.end_headers()

        def do_GET(self):
            entry = self._entry()
            video = entry.get("video") if entry else None
            spool = entry.get("spool") if entry else None
            if not video:
                self.send_error(410, "Stream expirou")
                return
            total = int(video.get("size") or 0)
            if total <= 0:
                self.send_error(500, "Tamanho inválido")
                return
            try:
                start, end, partial = _parse_range(self.headers.get("Range"), total)
            except (ValueError, TypeError):
                self.send_response(416)
                self.send_header("Content-Range", "bytes */%d" % total)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            length = end - start + 1
            self.send_response(206 if partial else 200)
            self.send_header("Content-Type", _mime(video))
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            if partial:
                self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, total))
            self.send_header("Connection", "close")
            self.end_headers()
            Kodi.log("TELEGRAM", "Proxy GET bytes=%d-%d/%d" % (start, end, total), level="debug")
            _proxy_request_begin()
            try:
                self._stream_range(video, start, length, spool=spool)
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as exc:
                Kodi.log("TELEGRAM", "Proxy interrompido: %s" % exc, level="warning")
            finally:
                _proxy_request_end()

        def _stream_range(self, video, start, length, spool=None):
            if spool is not None:
                return _stream_spool_range(spool, start, length, self.wfile)
            # Legacy direct MTProto proxy retained as compatibility fallback.
            with _proxy_stream_lock:
                _run_sync(lambda: _stream_range_saved_session_async(video, start, length, self.wfile),
                          timeout=None)

    return RangeHandler


def _document_location(video):
    from telethon.tl.types import InputDocumentFileLocation
    document_id = int(video.get("documentId") or 0)
    access_hash = int(video.get("documentAccessHash") or 0)
    file_reference = video.get("fileReference") or b""
    if not document_id or not access_hash:
        raise RuntimeError("Descritor do arquivo Telegram incompleto.")
    return InputDocumentFileLocation(id=document_id, access_hash=access_hash,
                                     file_reference=file_reference, thumb_size="")


def _mtproto_request_size(length):
    """Return an upload.getFile request size that stays on Telethon's direct iterator.

    Telethon clamps request_size to 512 KiB, but computes chunk_size before that
    clamp. Passing the proxy's 2 MiB buffer therefore selects the indirect
    iterator, which may wait for several MTProto requests before yielding the
    first byte. Kodi/libcurl times out while that buffer is being assembled.
    Keep the transport request small/aligned and let Kodi's own cache perform
    the larger buffering.
    """
    size = min(int(TG_TELETHON_REQUEST_BYTES), max(4096, int(length or 0)))
    size -= size % 4096
    return max(4096, min(256 * 1024, size))


def _mtproto_steady_size(length):
    """Steady-state MTProto request size proven safe by the real Kodi log.

    Although Telethon exposes 512 KiB as its generic maximum, the tested
    Telegram/DC path returned LIMIT_INVALID for 512 KiB GetFile requests.
    256 KiB is already proven on-device, so keep every steady request at or
    below that boundary and gain throughput with a bounded pipeline instead.
    """
    size = min(int(TG_TELETHON_STREAM_BYTES), max(4096, int(length or 0)))
    size -= size % 4096
    return max(4096, min(256 * 1024, size))


def _is_limit_invalid(exc):
    try:
        from telethon.errors import LimitInvalidError
        if isinstance(exc, LimitInvalidError):
            return True
    except Exception:
        pass
    return "invalid limit" in str(exc or "").lower()


async def _fetch_mtproto_chunk(client, location, offset, wanted, request_size, dc_id, file_size):
    """Fetch one contiguous chunk, with safe LIMIT_INVALID fallback.

    A provider/DC can impose a stricter limit than Telethon's nominal maximum.
    Never let that abort the HTTP response and create a corrupt/partial MP4.
    If Telegram rejects a limit, halve it down to 64 KiB and assemble the
    original wanted window from multiple direct requests.
    """
    wanted = max(0, int(wanted or 0))
    if wanted <= 0:
        return b""
    size = max(4096, int(request_size or 0))
    size -= size % 4096
    size = max(4096, min(256 * 1024, size))
    while True:
        try:
            parts = []
            received = 0
            limit = max(1, int(math.ceil(float(wanted) / float(size))))
            async for chunk in client.iter_download(
                    location, offset=int(offset), request_size=int(size), limit=limit,
                    dc_id=dc_id, file_size=file_size):
                if not chunk:
                    break
                raw = bytes(chunk)
                parts.append(raw)
                received += len(raw)
                if received >= wanted:
                    break
            return b"".join(parts)[:wanted]
        except Exception as exc:
            if _is_limit_invalid(exc) and size > 64 * 1024:
                new_size = max(64 * 1024, size // 2)
                new_size -= new_size % 4096
                Kodi.log(
                    "TELEGRAM",
                    "Telegram recusou request=%d; repetindo bloco com %d byte(s)." %
                    (size, new_size),
                    level="warning")
                size = new_size
                continue
            raise


def _download_context(video):
    dc_id = int(video.get("documentDcId") or 0) or None
    file_size = int(video.get("size") or 0) or None
    msg_data = None
    try:
        peer = _build_input_peer(video.get("peerId") or "", video.get("peerType") or "",
                                 video.get("peerRawId") or "", video.get("accessHash") or "")
        message_id = int(video.get("messageId") or 0)
        if peer and message_id:
            msg_data = (peer, message_id)
    except Exception:
        msg_data = None
    return dc_id, file_size, msg_data


async def _stream_range_saved_session_async(video, offset, length, writer):
    """Stream a byte range with low first-byte latency and bounded read-ahead.

    The first 256 KiB stays conservative so Kodi can open the demuxer quickly.
    After that, several 256 KiB ``upload.getFile`` requests are kept in flight
    on the *same* TelegramClient.  The real 1.4.6 log proved 512 KiB is rejected
    by this Telegram/DC path with LIMIT_INVALID, so 1.4.7 keeps the proven-safe
    request size and gains throughput by pipelining rather than oversized RPCs.
    """
    client = await _authorized_client()
    pending = []
    try:
        location = _document_location(video)
        dc_id, file_size, _msg_data = _download_context(video)
        remaining = int(length)
        cursor = int(offset)
        delivered = 0
        transfer_started = time.monotonic()
        next_metric = 2 * 1024 * 1024

        # Fast first byte: keep the proven 256 KiB request from 1.4.3/1.4.4.
        first_wanted = min(remaining, int(TG_TELETHON_REQUEST_BYTES))
        if first_wanted > 0:
            first_request = _mtproto_request_size(first_wanted)
            payload = await _fetch_mtproto_chunk(
                client, location, cursor, first_wanted, first_request, dc_id, file_size)
            if payload:
                writer.write(payload)
                writer.flush()
                delivered += len(payload)
                remaining -= len(payload)
                cursor += len(payload)
                Kodi.log(
                    "TELEGRAM",
                    "Proxy primeiro bloco entregue: %d byte(s), request=%d" %
                    (len(payload), first_request),
                    level="debug")
            if not payload or remaining <= 0:
                return delivered

        # 1.4.9 integrity rule: the experimental multi-sender media path is
        # intentionally disabled. The 1.4.8 real-device log showed packet/NAL
        # corruption after that path became active. Keep the proven ordered
        # single-client pipeline as the only legacy direct-proxy transport.
        Kodi.log(
            "TELEGRAM",
            "Proxy legado em modo seguro: MTProto ordenado em uma sessão; multi-sender desativado.",
            level="debug")

        # Conservative fallback: bounded ordered pipeline on one Telethon
        # client, retained byte-for-byte in behaviour from 1.4.7.
        pipeline = max(1, min(12, int(TG_TELETHON_PIPELINE_CHUNKS)))
        step = max(4096, min(256 * 1024, int(TG_TELETHON_STREAM_BYTES)))
        schedule_offset = cursor
        schedule_remaining = remaining

        def schedule_one():
            nonlocal schedule_offset, schedule_remaining
            if schedule_remaining <= 0:
                return False
            wanted = min(step, schedule_remaining)
            request_size = _mtproto_steady_size(wanted)
            task = asyncio.ensure_future(_fetch_mtproto_chunk(
                client, location, schedule_offset, wanted, request_size, dc_id, file_size))
            pending.append((task, schedule_offset, wanted, request_size))
            schedule_offset += wanted
            schedule_remaining -= wanted
            return True

        for _ in range(pipeline):
            if not schedule_one():
                break

        Kodi.log(
            "TELEGRAM",
            "Proxy read-ahead ativo: %d chunk(s) x até %d byte(s) (mesma sessão)." %
            (pipeline, step),
            level="debug")

        while pending and remaining > 0:
            task, expected_offset, wanted, request_size = pending.pop(0)
            payload = await task
            if not payload:
                break
            take = bytes(payload[:remaining])
            writer.write(take)
            writer.flush()
            delivered += len(take)
            remaining -= len(take)
            cursor += len(take)
            if delivered >= next_metric:
                elapsed = max(0.001, time.monotonic() - transfer_started)
                mbps = (float(delivered) * 8.0) / (elapsed * 1000.0 * 1000.0)
                Kodi.log(
                    "TELEGRAM",
                    "Proxy taxa acumulada: %.2f Mb/s (%d byte(s) entregues)." %
                    (mbps, delivered),
                    level="debug")
                while next_metric <= delivered:
                    next_metric += 2 * 1024 * 1024

            # A short non-final Telegram reply means the continuous byte window
            # cannot be guaranteed; stop rather than creating a silent hole.
            if len(payload) < wanted and remaining > 0:
                Kodi.log(
                    "TELEGRAM",
                    "Read-ahead recebeu bloco curto em %d (%d/%d); encerrando faixa." %
                    (expected_offset, len(payload), wanted),
                    level="warning")
                break

            schedule_one()

        return delivered
    finally:
        for entry in pending:
            task = entry[0]
            if not task.done():
                task.cancel()
        if pending:
            try:
                await asyncio.gather(*(entry[0] for entry in pending), return_exceptions=True)
            except BaseException:
                pass
        await _disconnect_quiet(client)


async def _download_window_saved_session(video, offset, length):
    client = await _authorized_client()
    try:
        location = _document_location(video)
        request_size = _mtproto_request_size(length)
        limit = max(1, int(math.ceil(float(length) / float(request_size))))
        dc_id, file_size, _msg_data = _download_context(video)
        chunks, received = [], 0
        async for chunk in client.iter_download(location, offset=int(offset),
                                                 request_size=request_size, limit=limit,
                                                 dc_id=dc_id, file_size=file_size):
            if not chunk:
                break
            chunks.append(bytes(chunk))
            received += len(chunk)
            if received >= length:
                break
        return b"".join(chunks)[:int(length)]
    finally:
        await _disconnect_quiet(client)


def _run_sync_download(client, video, offset, length):
    # Compatibility helper used by the audit harness. `client` is deliberately
    # ignored so even tests cannot accidentally reintroduce cross-loop reuse.
    return _run_sync(lambda: _download_window_saved_session(video, offset, length), timeout=90)


def dependency_ok():
    return _TELEGRAM_DEP_OK


__all__ = ["dependency_ok", "restore_session", "start_qr_login", "complete_2fa_password",
           "disconnect", "cancel_pending_login", "clear_session_local", "has_saved_session", "is_connected",
           "get_state", "me_info", "search_video_messages", "search_video_messages_many", "prepare_stream_url",
           "register_stream_url", "stop_proxy", "shutdown_runtime", "proxy_port", "proxy_activity", "proxy_can_stop",
           "TelegramUnavailableError", "TelegramPasswordNeeded", "TelegramOperationCancelled", "_parse_range",
           "fetch_stream_video", "probe_download_speed", "media_required_mbps",
           "adaptive_spool_plan", "recommended_probe_bytes", "runtime_profile",
           "start_progressive_download", "stop_progressive_download", "register_spool_url",
           "download_video_local", "cleanup_local_video", "cleanup_orphan_temp_files"]
