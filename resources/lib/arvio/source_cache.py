# -*- coding: utf-8 -*-
"""Short-lived source snapshots for Kodi directory refreshes.

Kodi may refresh the current plugin directory after STOP even when playback was
started with ``xbmc.Player``.  Reusing the just-resolved source list prevents a
second network search while keeping the snapshot deliberately short-lived.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import threading
import time

from .kodi_utils import Kodi

_FILE = "arvio_stream_source_snapshot.json"
_TTL = 600.0
_MAX_ITEMS = 16
_lock = threading.RLock()


def make_key(media_type, item_id, season=None, episode=None, video_id=None, canonical_id=None,
             source_signature=None):
    """Build a snapshot key bound to both the title and active source topology.

    ``source_signature`` is optional for backward compatibility. Cinemeta passes
    the current enabled Stremio/Xtream/Telegram topology so adding, removing or
    toggling a provider can never reuse a snapshot created for an older setup.
    """
    raw = "|".join(str(x if x is not None else "") for x in
                   (media_type, item_id, season, episode, video_id, canonical_id,
                    source_signature))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _path():
    folder = Kodi.profile_path()
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, _FILE)


def _encode(value):
    if isinstance(value, bytes):
        return {"__arvio_bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _decode(value):
    if isinstance(value, dict):
        if set(value) == {"__arvio_bytes__"}:
            try:
                return base64.b64decode(value["__arvio_bytes__"].encode("ascii"))
            except Exception:
                return b""
        return {k: _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    return value


def _read_all():
    try:
        with open(_path(), "r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _write_all(value):
    path = _path()
    folder = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(prefix=".arvio_sources.", suffix=".tmp", dir=folder)
    try:
        try:
            os.chmod(tmp, 0o600)
        except Exception:
            pass
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = None
            json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except Exception:
                pass
        os.replace(tmp, path)
        tmp = None
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
    finally:
        if fd is not None:
            try: os.close(fd)
            except Exception: pass
        if tmp:
            try: os.unlink(tmp)
            except Exception: pass


def load(key, ttl=_TTL):
    now = time.time()
    with _lock:
        data = _read_all()
        item = data.get(str(key))
        if not isinstance(item, dict) or now - float(item.get("ts") or 0) > float(ttl):
            return None
        streams = item.get("streams")
        if not isinstance(streams, list):
            return None
        return _decode(streams)


def save(key, streams):
    if not isinstance(streams, list) or not streams:
        return False
    now = time.time()
    with _lock:
        data = _read_all()
        clean = {}
        for k, item in data.items():
            try:
                if isinstance(item, dict) and now - float(item.get("ts") or 0) <= _TTL:
                    clean[k] = item
            except Exception:
                pass
        clean[str(key)] = {"ts": now, "streams": _encode(streams)}
        if len(clean) > _MAX_ITEMS:
            ordered = sorted(clean.items(), key=lambda kv: float(kv[1].get("ts") or 0), reverse=True)
            clean = dict(ordered[:_MAX_ITEMS])
        try:
            _write_all(clean)
            return True
        except Exception as exc:
            Kodi.log("CACHE", "Snapshot de fontes não pôde ser salvo: %s" % exc, level="debug")
            return False


def touch(key):
    """Refresh an existing snapshot timestamp after playback.

    Kodi may refresh the parent directory only after a long movie ends. A short
    discovery TTL alone would expire before STOP, so the playback route renews
    the exact source snapshot immediately before returning to the directory.
    """
    now = time.time()
    with _lock:
        data = _read_all()
        item = data.get(str(key))
        if not isinstance(item, dict) or not isinstance(item.get("streams"), list):
            return False
        item["ts"] = now
        data[str(key)] = item
        try:
            _write_all(data)
            return True
        except Exception as exc:
            Kodi.log("CACHE", "Snapshot não pôde ser renovado: %s" % exc, level="debug")
            return False


def clear():
    """Remove every short-lived source snapshot.

    Used after provider configuration changes. Failure is fail-open because the
    topology-aware cache key already prevents stale snapshot reuse.
    """
    with _lock:
        try:
            path = _path()
            if os.path.exists(path):
                os.unlink(path)
            return True
        except Exception as exc:
            Kodi.log("CACHE", "Snapshot de fontes não pôde ser limpo: %s" % exc, level="debug")
            return False
