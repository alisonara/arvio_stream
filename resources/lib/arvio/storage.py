# -*- coding: utf-8 -*-
"""Persistent state for Arvio Stream (JSON files in the addon profile dir).

Writes are atomic: data is flushed to a temporary file in the same directory
and then replaced. A failed write raises ``StorageError`` so the UI never
reports success when persistence actually failed.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading

from .kodi_utils import Kodi

_lock = threading.RLock()

_STREMIO_FILE = "arvio_stream_addons.json"
_XTREAM_FILE = "arvio_stream_xtream.json"
_TELEGRAM_FILE = "arvio_stream_telegram.json"
_XTREAM_CACHE_FILE = "arvio_stream_xtream_cache.json"


class StorageError(RuntimeError):
    pass


def _path(name):
    folder = Kodi.profile_path()
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, name)


def _read(name, default):
    with _lock:
        p = _path(name)
        if not os.path.exists(p):
            return default
        try:
            with open(p, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception as exc:
            Kodi.log("STORAGE", "Falha ao ler %s: %s" % (name, exc), level="warning")
            return default


def _write(name, value):
    with _lock:
        p = _path(name)
        folder = os.path.dirname(p)
        fd = None
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(prefix=".%s." % name, suffix=".tmp", dir=folder)
            try:
                os.chmod(tmp, 0o600)
            except Exception:
                pass
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                fd = None
                json.dump(value, handle, ensure_ascii=False, indent=2)
                handle.flush()
                try:
                    os.fsync(handle.fileno())
                except Exception:
                    pass
            os.replace(tmp, p)
            tmp = None
            try:
                os.chmod(p, 0o600)
            except Exception:
                pass
            return True
        except Exception as exc:
            Kodi.log("STORAGE", "Falha ao salvar %s: %s" % (name, exc), level="error")
            raise StorageError("Não foi possível salvar os dados do Arvio Stream.") from exc
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            if tmp and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except Exception:
                    pass


# ---- Stremio addons -------------------------------------------------------

def load_addons():
    value = _read(_STREMIO_FILE, [])
    return value if isinstance(value, list) else []


def save_addons(addons):
    return _write(_STREMIO_FILE, addons)


def upsert_addon(addon):
    addons = [a for a in load_addons() if a.get("manifestUrl") != addon.get("manifestUrl")]
    addons.insert(0, addon)
    save_addons(addons)
    return addon


def remove_addon(manifest_url):
    addons = [a for a in load_addons() if a.get("manifestUrl") != manifest_url]
    return save_addons(addons)


# ---- Xtream accounts ------------------------------------------------------

def load_xtream_accounts():
    value = _read(_XTREAM_FILE, [])
    return value if isinstance(value, list) else []


def save_xtream_accounts(accounts):
    return _write(_XTREAM_FILE, accounts)


def upsert_xtream(account):
    accounts = [a for a in load_xtream_accounts() if a.get("id") != account.get("id")]
    accounts.insert(0, account)
    save_xtream_accounts(accounts)
    return account


def set_xtream_enabled(account_id, enabled):
    accounts = load_xtream_accounts()
    for account in accounts:
        if account.get("id") == account_id:
            account["enabled"] = bool(enabled)
            break
    return save_xtream_accounts(accounts)


def remove_xtream(account_id):
    accounts = [a for a in load_xtream_accounts() if a.get("id") != account_id]
    return save_xtream_accounts(accounts)


# ---- Telegram -------------------------------------------------------------

def load_telegram_session():
    value = _read(_TELEGRAM_FILE, None)
    return value if isinstance(value, str) and value.strip() else None


def save_telegram_session(session):
    if not isinstance(session, str) or not session.strip():
        raise StorageError("Sessão Telegram inválida.")
    return _write(_TELEGRAM_FILE, session.strip())


def clear_telegram_session():
    return _write(_TELEGRAM_FILE, None)


def has_telegram_session():
    return bool(load_telegram_session())


# ---- Small persistent Xtream cache ----------------------------------------

def load_xtream_cache():
    value = _read(_XTREAM_CACHE_FILE, {})
    return value if isinstance(value, dict) else {}


def save_xtream_cache(value):
    return _write(_XTREAM_CACHE_FILE, value if isinstance(value, dict) else {})


def clear_xtream_cache():
    return _write(_XTREAM_CACHE_FILE, {})
