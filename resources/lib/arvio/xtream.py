# -*- coding: utf-8 -*-
"""Xtream account + player_api client (live TV, VOD, series)."""

from __future__ import annotations

import base64
import concurrent.futures
import re
import time
import unicodedata
import urllib.parse

from . import http
from .kodi_utils import Kodi
from .storage import (load_xtream_accounts, save_xtream_accounts, upsert_xtream,
                      set_xtream_enabled as _storage_set_xtream_enabled,
                      remove_xtream as _storage_remove_xtream, load_xtream_cache,
                      save_xtream_cache)


class XtreamError(Exception):
    pass


DEFAULT_USER_AGENT = "VLC/3.0.20 LibVLC/3.0.20"
_ttl_cache = {}
_TTL_MAX_MS = 30 * 60 * 1000
_PERSIST_MAX = 48


def _persistent_key(key):
    if not isinstance(key, tuple):
        return str(key)
    return "|".join(str(part) for part in key)


def _persistent_get(key):
    try:
        cache = load_xtream_cache()
    except Exception as exc:
        # Cache is an optimization only. A profile/storage problem must never
        # block live navigation or turn a transient I/O failure into a playback failure.
        Kodi.log("XTREAM", "Cache persistente indisponível na leitura: %s" % exc, level="warning")
        return None
    entry = cache.get(_persistent_key(key))
    if not isinstance(entry, dict):
        return None
    try:
        age = time.time() * 1000 - float(entry.get("ts") or 0)
    except (TypeError, ValueError):
        return None
    if age >= _TTL_MAX_MS:
        return None
    return entry.get("value")


def _persistent_set(key, value):
    cache = load_xtream_cache()
    now = time.time() * 1000
    cache[_persistent_key(key)] = {"ts": now, "value": value}
    # Keep the cache intentionally small; it is a navigation accelerator, not a database.
    if len(cache) > _PERSIST_MAX:
        ordered = sorted(cache.items(), key=lambda item: float((item[1] or {}).get("ts") or 0), reverse=True)
        cache = dict(ordered[:_PERSIST_MAX])
    save_xtream_cache(cache)


def _cache_get(key, persistent=False):
    entry = _ttl_cache.get(key)
    if entry and time.time() * 1000 - entry[0] < _TTL_MAX_MS:
        return entry[1]
    if persistent:
        value = _persistent_get(key)
        if value is not None:
            _ttl_cache[key] = (time.time() * 1000, value)
            return value
    return None


def _cache_set(key, value, persistent=False):
    _ttl_cache[key] = (time.time() * 1000, value)
    if persistent:
        try:
            _persistent_set(key, value)
        except Exception as exc:
            Kodi.log("XTREAM", "Cache persistente indisponível: %s" % exc, level="warning")
    if len(_ttl_cache) > 24:
        now = time.time() * 1000
        for k in list(_ttl_cache):
            if now - _ttl_cache[k][0] > _TTL_MAX_MS:
                del _ttl_cache[k]


def _cache_invalidate_account(account_id):
    for key in list(_ttl_cache):
        if isinstance(key, tuple) and len(key) > 1 and key[1] == account_id:
            _ttl_cache.pop(key, None)
    try:
        cache = load_xtream_cache()
        needle = "|%s" % account_id
        changed = False
        for key in list(cache):
            if needle in key:
                cache.pop(key, None)
                changed = True
        if changed:
            save_xtream_cache(cache)
    except Exception as exc:
        Kodi.log("XTREAM", "Falha ao invalidar cache persistente: %s" % exc, level="warning")

def parse_xtream_input(raw):
    raw = (raw or "").strip()
    if not raw:
        return None
    if len(raw) >= 12 and "://" not in raw and " " not in raw:
        try:
            padded = raw.replace("-", "+").replace("_", "/")
            padded += "=" * ((4 - len(padded) % 4) % 4)
            decoded = base64.b64decode(padded).decode("utf-8", "replace").strip()
            if re.match(r"^https?://", decoded, re.I):
                raw = decoded
        except Exception:
            pass

    parts = [p for p in re.split(r"[\s\n\r\t,;|]+", raw) if p]

    def normalize_host(host):
        host = host.strip()
        for scheme in ("xtream://", "xstream://", "xtreamcodes://", "xc://"):
            if host.lower().startswith(scheme):
                host = host[len(scheme):]
                break
        host = re.sub(r"/+$", "", host)
        if host and not re.match(r"^https?://", host, re.I):
            host = "http://" + host
        return host

    if len(parts) >= 3:
        first = parts[0]
        parsed0 = urllib.parse.urlparse(first)
        scheme0, netloc0, base_path = "http", first, ""
        if re.match(r"^https?://", first, re.I):
            scheme0, netloc0 = parsed0.scheme, parsed0.netloc
            if re.search(r"/(get|xmltv|player_api)\.php$", parsed0.path, re.I):
                base_path = re.sub(r"/(get|xmltv|player_api)\.php$", "", parsed0.path, flags=re.I)
        if not netloc0:
            return None
        base = "%s://%s%s" % (scheme0, netloc0, base_path)
        return {"baseUrl": normalize_host(base).rstrip("/"),
                "username": parts[1], "password": parts[2]}

    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme in ("http", "https") and parsed.netloc:
        base = parsed.scheme + "://" + parsed.netloc
        if re.search(r"/(get|xmltv|player_api)\.php$", parsed.path, re.I):
            base += re.sub(r"/(get|xmltv|player_api)\.php$", "", parsed.path, flags=re.I)
        creds = xtream_credentials_from_url(parsed)
        if creds:
            return {"baseUrl": base.rstrip("/"), "username": creds[0], "password": creds[1]}
    return None


def xtream_credentials_from_url(parsed):
    qs = urllib.parse.parse_qs(parsed.query)
    username = qs.get("username") or qs.get("user") or qs.get("uname") or []
    password = qs.get("password") or qs.get("pass") or qs.get("pwd") or []
    if username and password and username[0].strip() and password[0].strip():
        return username[0].strip(), password[0].strip()
    return None


def build_get_m3u(info):
    return "%s/get.php?username=%s&password=%s&type=m3u_plus&output=m3u8" % (
        info["baseUrl"], urllib.parse.quote(info["username"], safe=""), urllib.parse.quote(info["password"], safe=""))


def player_api_url(info, action=None, **params):
    url = "%s/player_api.php?username=%s&password=%s" % (
        info["baseUrl"], urllib.parse.quote(info["username"], safe=""), urllib.parse.quote(info["password"], safe=""))
    if action:
        params = dict(params)
        params["action"] = action
    if params:
        url += "&" + urllib.parse.urlencode(params)
    return url


def _headers():
    return {"User-Agent": DEFAULT_USER_AGENT, "Accept": "*/*"}


def _api_json(info, action, timeout=None, **params):
    request_url = player_api_url(info, action, **params)
    if timeout is None:
        return http.get_json(request_url, headers=_headers())
    return http.get_json(request_url, headers=_headers(), timeout=float(timeout))


def _api_root(info):
    return http.get_json(player_api_url(info), headers=_headers())


def account_status(info):
    try:
        payload = _api_root(info)
    except http.HttpError as exc:
        raise XtreamError("Falha ao validar conta (%s)." % (exc.status or exc))
    user = payload.get("user_info") if isinstance(payload, dict) else None
    if not isinstance(user, dict):
        return {"valid": False, "status": "unknown", "allowed": []}
    status = str(user.get("status") or "").strip()
    active = status.lower() in ("active", "enabled", "1", "true") or (not status and user.get("auth") in (1, "1"))
    allowed = user.get("allowed_output_formats") or []
    if isinstance(allowed, str):
        allowed = [allowed]
    return {"valid": bool(active), "status": status or ("Active" if active else "unknown"),
            "allowed": [str(x).lower().lstrip(".") for x in allowed]}


def validate_account(info):
    return account_status(info).get("valid", False)


def add_account(name, raw):
    info = parse_xtream_input(raw)
    if not info:
        raise XtreamError("Não consegui entender a entrada. Use: servidor usuario senha\n"
                          "ou uma URL get.php/player_api com username e password.")
    account = {
        "id": "%s|%s" % (re.sub(r"[^a-z0-9]", "-", info["baseUrl"].lower()), info["username"]),
        "name": (name or info["username"]).strip() or info["username"],
        "baseUrl": info["baseUrl"], "username": info["username"], "password": info["password"],
        "enabled": True, "addedAt": int(time.time()),
    }
    previous = account_by_id(account["id"])
    if previous is not None:
        account["enabled"] = previous.get("enabled", True)
        account["addedAt"] = previous.get("addedAt", account["addedAt"])
    upsert_xtream(account)
    _cache_invalidate_account(account["id"])
    return account


def accounts():
    return load_xtream_accounts()


def enabled_accounts():
    return [a for a in load_xtream_accounts() if a.get("enabled", True)]


def account_by_id(account_id):
    for account in load_xtream_accounts():
        if account.get("id") == account_id:
            return account
    return None


def enabled_account_by_id(account_id):
    account = account_by_id(account_id)
    return account if account and account.get("enabled", True) else None


def _live_format(account):
    key = ("live_fmt", account.get("id"))
    cached = _cache_get(key)
    if cached:
        return cached
    fmt = "m3u8"
    try:
        status = account_status(account)
        allowed = status.get("allowed") or []
        if allowed:
            if "m3u8" in allowed:
                fmt = "m3u8"
            elif "ts" in allowed or "mpegts" in allowed:
                fmt = "ts"
            else:
                fmt = allowed[0]
    except Exception as exc:
        Kodi.log("XTREAM", "Não foi possível detectar output; mantendo m3u8: %s" % exc, level="warning")
    _cache_set(key, fmt)
    return fmt


def _live_url(info, stream_id, ext=None):
    ext = (ext or _live_format(info) or "m3u8").lower().lstrip(".")
    return "%s/live/%s/%s/%s.%s" % (
        info["baseUrl"], urllib.parse.quote(info["username"], safe=""), urllib.parse.quote(info["password"], safe=""),
        urllib.parse.quote(str(stream_id), safe=""), ext)


def _vod_url(info, stream_id, ext="mkv"):
    ext = (ext or "mkv").lstrip(".")
    return "%s/movie/%s/%s/%s.%s" % (
        info["baseUrl"], urllib.parse.quote(info["username"], safe=""), urllib.parse.quote(info["password"], safe=""),
        urllib.parse.quote(str(stream_id), safe=""), ext)


def _series_vod_url(info, stream_id, ext="mkv"):
    ext = (ext or "mkv").lstrip(".")
    return "%s/series/%s/%s/%s.%s" % (
        info["baseUrl"], urllib.parse.quote(info["username"], safe=""), urllib.parse.quote(info["password"], safe=""),
        urllib.parse.quote(str(stream_id), safe=""), ext)


def _api_list(account, action, timeout=None, **params):
    try:
        value = _api_json(account, action, timeout=timeout, **params)
    except http.HttpError as exc:
        raise XtreamError("Falha na API Xtream (%s)." % (exc.status or exc))
    return value if isinstance(value, list) else []


def live_groups(account):
    key = ("live_cats", account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    result = [{"category_id": str(cat.get("category_id", "")),
               "category_name": cat.get("category_name") or "Sem grupo"}
              for cat in _api_list(account, "get_live_categories") if isinstance(cat, dict)]
    _cache_set(key, result, persistent=True)
    return result


def live_streams(account, category_id=None):
    params = {"category_id": category_id} if category_id else {}
    result = []
    for stream in _api_list(account, "get_live_streams", **params):
        stream_id = stream.get("stream_id")
        if stream_id is None:
            continue
        ext = (stream.get("container_extension") or _live_format(account) or "m3u8").lstrip(".")
        result.append({
            "id": str(stream_id), "name": stream.get("name") or "Canal %s" % stream_id,
            "logo": stream.get("stream_icon") or "", "epg": stream.get("epg_channel_id") or "",
            "url": _live_url(account, stream_id, ext), "mime": "hls" if ext.lower() == "m3u8" else "direct",
            "headers": {"User-Agent": DEFAULT_USER_AGENT},
            "category_id": str(stream.get("category_id", "")),
        })
    return result


def live_streams_all(account):
    """Return all live channels, falling back to categories when required."""
    try:
        items = live_streams(account)
    except Exception as exc:
        Kodi.log("XTREAM", "TV sem categoria falhou; tentando grupos: %s" % exc, level="warning")
        items = []
    if items:
        return _dedupe_media_items(items) if "_dedupe_media_items" in globals() else items
    groups = live_groups(account)
    ids = [str(g.get("category_id") or "") for g in groups if str(g.get("category_id") or "")]
    collected = []
    if not ids:
        return collected
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(ids))) as executor:
        futures = [executor.submit(live_streams, account, category_id) for category_id in ids]
        for future in concurrent.futures.as_completed(futures):
            try:
                collected.extend(future.result() or [])
            except Exception as exc:
                Kodi.log("XTREAM", "Grupo de TV ignorado: %s" % exc, level="warning")
    seen, out = set(), []
    for item in collected:
        key = str((item or {}).get("id") or "")
        if key and key not in seen:
            seen.add(key); out.append(item)
    return out


def vod_groups(account, timeout=None):
    key = ("vod_cats", account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    result = [{"category_id": str(cat.get("category_id", "")),
               "category_name": cat.get("category_name") or "Filmes"}
              for cat in _api_list(account, "get_vod_categories", timeout=timeout) if isinstance(cat, dict)]
    _cache_set(key, result, persistent=True)
    return result


def _external_id_value(stream, info, keys):
    for source in (stream, info):
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return str(value).strip()
    return ""


def _normalize_imdb_id(value):
    text = str(value or "").strip().lower()
    match = re.search(r"tt\d+", text)
    return match.group(0) if match else ""


def _normalize_tmdb_id(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^tmdb[:/\s-]*", "", text)
    match = re.search(r"\d+", text)
    return match.group(0) if match else ""


def _external_ids(stream, info):
    imdb = _external_id_value(stream, info, ("imdb", "imdb_id", "imdbId", "imdbid"))
    tmdb = _external_id_value(stream, info, ("tmdb", "tmdb_id", "tmdbId", "tmdbid"))
    return _normalize_imdb_id(imdb), _normalize_tmdb_id(tmdb)


def _year4(value):
    match = re.search(r"(?:19|20)\d{2}", str(value or ""))
    return match.group(0) if match else ""


def vod_streams(account, category_id=None, timeout=None):
    # Category-scoped results are stable navigation data and are reused by the
    # exhaustive Cinemeta matcher. Without this cache a category-completeness
    # pass could re-download the same VOD lists for every title lookup.
    key = ("vod_list:%s" % (category_id or "all"), account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    params = {"category_id": category_id} if category_id else {}
    result = []
    for stream in _api_list(account, "get_vod_streams", timeout=timeout, **params):
        if not isinstance(stream, dict):
            continue
        stream_id = stream.get("stream_id")
        if stream_id is None:
            continue
        info = stream.get("info") if isinstance(stream.get("info"), dict) else {}
        ext = stream.get("container_extension") or info.get("container_extension") or "mkv"
        plot = stream.get("plot") or info.get("plot") or info.get("description") or ""
        imdb_id, tmdb_id = _external_ids(stream, info)
        result.append({
            "id": str(stream_id), "name": stream.get("name") or "Filme %s" % stream_id,
            "plot": plot, "poster": stream.get("stream_icon") or info.get("movie_image") or "",
            "year": stream.get("year") or info.get("year") or info.get("releasedate") or "",
            "rating": stream.get("rating") or info.get("rating") or "",
            "added": stream.get("added") or info.get("added") or "",
            "imdbId": imdb_id, "tmdbId": tmdb_id,
            "url": _vod_url(account, stream_id, ext), "ext": ext,
            "headers": {"User-Agent": DEFAULT_USER_AGENT},
            "category_id": str(stream.get("category_id", "")),
        })
    _cache_set(key, result, persistent=True)
    return result


def _dedupe_media_items(items):
    out, seen = [], set()
    for item in items or []:
        key = str((item or {}).get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _collect_by_categories(account, groups_func, items_func, cache_tag,
                           max_categories=None, budget_s=28.0):
    """Collect category-only panels with bounded work and no orphan workers.

    Returns ``(items, complete)``. Partial scans are deliberately not persisted
    as a complete catalog, so a slow panel can recover on a later attempt.
    """
    key = (cache_tag, account.get("id"))
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached, True
    request_timeout = max(3.0, min(10.0, float(budget_s)))
    try:
        groups = groups_func(account, timeout=request_timeout)
    except TypeError:
        groups = groups_func(account)
    all_ids = [str(g.get("category_id") or "") for g in groups if str(g.get("category_id") or "")]
    if not all_ids:
        return [], True
    if max_categories is None:
        ids = list(all_ids)
    else:
        ids = all_ids[:max(1, int(max_categories))]
    complete = len(ids) == len(all_ids)
    collected = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(ids)))
    def fetch_category(category_id):
        try:
            return items_func(account, category_id, timeout=request_timeout)
        except TypeError:
            return items_func(account, category_id)
    futures = [executor.submit(fetch_category, category_id) for category_id in ids]
    try:
        done, pending = concurrent.futures.wait(futures, timeout=float(budget_s))
        if pending:
            complete = False
            for future in pending:
                future.cancel()
        # Wait only for requests that had already started. Their underlying HTTP
        # calls are bounded by HTTP_TIMEOUT, and this prevents Kodi subinterpreters
        # from being destroyed while Python worker threads are still active.
        executor.shutdown(wait=True)
        processed = 0
        for future in futures:
            if future.cancelled() or not future.done():
                continue
            processed += 1
            try:
                collected.extend(future.result() or [])
            except Exception as exc:
                complete = False
                Kodi.log("XTREAM", "Categoria ignorada durante agregação: %s" % exc, level="warning")
    finally:
        # shutdown() is idempotent and protects exceptional paths above.
        try:
            executor.shutdown(wait=True)
        except Exception:
            pass
    collected = _dedupe_media_items(collected)
    if collected and complete:
        _cache_set(key, collected, persistent=True)
    if not complete:
        Kodi.log("XTREAM", "Varredura por categorias ficou parcial (%d/%d grupos concluídos)." %
                 (locals().get("processed", 0), len(all_ids)), level="warning")
    return collected, complete

def _catalog_with_category_coverage(account, base_items, groups_func, items_func,
                                    cache_tag, budget_s=28.0):
    """Complete a possibly-partial unfiltered Xtream catalog by category.

    Several Xtream panels return a non-empty *partial* list when category_id is
    omitted. Older Arvio code treated any non-empty response as complete, so a
    Cinemeta title stored in another category was invisible to the matcher.
    This helper proves coverage from category_id values and fetches only groups
    that are missing. If the base rows omit category_id, completeness cannot be
    proven and all declared groups are scanned once.
    """
    base = _dedupe_media_items(base_items)
    try:
        request_timeout = max(3.0, min(10.0, float(budget_s)))
        try:
            groups = groups_func(account, timeout=request_timeout) or []
        except TypeError:
            groups = groups_func(account) or []
    except Exception as exc:
        Kodi.log("XTREAM", "Categorias indisponíveis para completar catálogo: %s" % exc, level="warning")
        return base, False
    all_ids = [str(g.get("category_id") or "") for g in groups
               if str(g.get("category_id") or "")]
    all_ids = list(dict.fromkeys(all_ids))
    if not all_ids:
        return base, True

    present = {str((item or {}).get("category_id") or "") for item in base}
    present.discard("")
    # Presence of every category in an unfiltered response does *not* prove
    # completeness: many panels cap/paginate that response while still mixing
    # rows from all groups. Cinemeta lookup therefore performs one bounded,
    # category-complete pass and persists the merged result.
    scan_ids = list(all_ids)
    collected = []
    complete = True
    processed = 0
    if scan_ids:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(scan_ids)))
        def fetch_category(cid):
            try:
                return items_func(account, cid, timeout=request_timeout)
            except TypeError:
                return items_func(account, cid)
        futures = [executor.submit(fetch_category, cid) for cid in scan_ids]
        try:
            _done, pending = concurrent.futures.wait(futures, timeout=float(budget_s))
            if pending:
                complete = False
                for future in pending:
                    future.cancel()
            executor.shutdown(wait=True)
            for future in futures:
                if future.cancelled() or not future.done():
                    continue
                processed += 1
                try:
                    collected.extend(future.result() or [])
                except Exception as exc:
                    complete = False
                    Kodi.log("XTREAM", "Categoria ignorada ao completar catálogo: %s" % exc, level="warning")
        finally:
            try:
                executor.shutdown(wait=True)
            except Exception:
                pass
    merged = _dedupe_media_items(base + collected)
    Kodi.log(
        "XTREAM",
        "Cobertura %s: base=%d, grupos=%d, varridos=%d, consultados=%d, total=%d, completa=%s." %
        (cache_tag, len(base), len(all_ids), len(scan_ids), processed, len(merged), "sim" if complete else "não"),
        level="info")
    return merged, complete


def vod_streams_all(account):
    """Return a category-complete VOD catalog for Cinemeta/title lookup."""
    key = ("vod_all", account.get("id"))
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    try:
        base = vod_streams(account)
    except Exception as exc:
        Kodi.log("XTREAM", "Catálogo VOD sem categoria falhou; tentando grupos: %s" % exc, level="warning")
        base = []
    items, complete = _catalog_with_category_coverage(
        account, base, vod_groups, vod_streams, "VOD", budget_s=20.0)
    if not items and not base:
        items, complete = _collect_by_categories(
            account, vod_groups, vod_streams, "vod_by_categories", max_categories=None, budget_s=20.0)
    items = _dedupe_media_items(items)
    if items and complete:
        _cache_set(key, items, persistent=True)
    return items


def series_groups(account, timeout=None):
    key = ("ser_cats", account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    result = [{"category_id": str(cat.get("category_id", "")),
               "category_name": cat.get("category_name") or "Séries"}
              for cat in _api_list(account, "get_series_categories", timeout=timeout) if isinstance(cat, dict)]
    _cache_set(key, result, persistent=True)
    return result


def _first_url(value):
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, str) and item:
                return item
    return ""


def series_list(account, category_id=None, timeout=None):
    key = ("series_list:%s" % (category_id or "all"), account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    params = {"category_id": category_id} if category_id else {}
    result = []
    for series in _api_list(account, "get_series", timeout=timeout, **params):
        if not isinstance(series, dict):
            continue
        sid = series.get("series_id")
        if sid is None:
            continue
        info = series.get("info") if isinstance(series.get("info"), dict) else {}
        imdb_id, tmdb_id = _external_ids(series, info)
        result.append({
            "id": str(sid), "name": series.get("name") or "Série %s" % sid,
            "poster": series.get("cover") or series.get("stream_icon") or "",
            "backdrop": _first_url(series.get("backdrop_path")), "plot": series.get("plot") or info.get("plot") or "",
            "year": series.get("year") or info.get("year") or info.get("releaseDate") or "",
            "rating": series.get("rating") or info.get("rating") or "",
            "added": series.get("last_modified") or series.get("added") or info.get("added") or "",
            "imdbId": imdb_id, "tmdbId": tmdb_id,
            "category_id": str(series.get("category_id", "")),
        })
    _cache_set(key, result, persistent=True)
    return result


def series_list_all(account):
    """Return a category-complete series catalog for Cinemeta/title lookup."""
    key = ("series_all", account.get("id"))
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached
    try:
        base = series_list(account)
    except Exception as exc:
        Kodi.log("XTREAM", "Catálogo de séries sem categoria falhou; tentando grupos: %s" % exc, level="warning")
        base = []
    items, complete = _catalog_with_category_coverage(
        account, base, series_groups, series_list, "SÉRIES", budget_s=20.0)
    if not items and not base:
        items, complete = _collect_by_categories(
            account, series_groups, series_list, "series_by_categories", max_categories=None, budget_s=20.0)
    items = _dedupe_media_items(items)
    if items and complete:
        _cache_set(key, items, persistent=True)
    return items


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _number_from_text(text, kind="episode"):
    value = str(text or "")
    if kind == "season":
        patterns = (r'(?i)\bseason\s*(\d+)\b', r'(?i)\bs(\d{1,3})e\d{1,4}\b')
    else:
        patterns = (r'(?i)\bs\d{1,3}e(\d{1,4})\b', r'(?i)\bepisode\s*(\d+)\b', r'(?i)\bep\.?\s*(\d+)\b')
    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return _as_int(match.group(1))
    return None


def _episode_id(ep):
    for key in ("id", "stream_id", "episode_id"):
        if ep.get(key) not in (None, ""):
            return ep.get(key)
    return None


def _episode_record(account, ep, season_hint=None, episode_hint=None):
    if not isinstance(ep, dict):
        return None
    stream_id = _episode_id(ep)
    if stream_id is None:
        return None
    info = ep.get("info") if isinstance(ep.get("info"), dict) else {}
    title = ep.get("title") or ep.get("name") or info.get("name") or ""
    season = None
    for source in (ep, info):
        for key in ("season", "season_num", "season_number"):
            season = _as_int(source.get(key))
            if season is not None:
                break
        if season is not None:
            break
    if season is None:
        season = _number_from_text(title, "season")
    if season is None:
        season = _as_int(season_hint)
    if season is None:
        season = 1

    episode = None
    for source in (ep, info):
        for key in ("episode_num", "episode_number", "episode", "number", "num"):
            episode = _as_int(source.get(key))
            if episode is not None:
                break
        if episode is not None:
            break
    if episode is None:
        episode = _number_from_text(title, "episode")
    if episode is None:
        episode = _as_int(episode_hint) or 0

    ext = ep.get("container_extension") or info.get("container_extension") or "mkv"
    return {
        "id": str(stream_id), "season": int(season), "episode": int(episode),
        "title": title or "Episódio %s" % episode,
        "plot": ep.get("plot") or info.get("plot") or info.get("description") or "",
        "url": _series_vod_url(account, stream_id, ext), "ext": ext,
        "headers": {"User-Agent": DEFAULT_USER_AGENT},
    }


def _parse_episode_tree(account, root):
    found = []
    seen = set()

    def visit(node, season_hint=None, episode_hint=None, depth=0):
        if depth > 8:
            return
        if isinstance(node, list):
            for index, child in enumerate(node, 1):
                visit(child, season_hint, index if episode_hint is None else episode_hint, depth + 1)
            return
        if not isinstance(node, dict):
            return

        record = _episode_record(account, node, season_hint, episode_hint)
        if record is not None:
            key = record["id"]
            if key not in seen:
                seen.add(key)
                found.append(record)
            # Do not recursively parse metadata fields of a confirmed episode.
            return

        for key, child in node.items():
            if key in ("info", "seasons") and not isinstance(child, (list, dict)):
                continue
            next_season = season_hint
            next_episode = episode_hint
            key_text = str(key)
            numeric = _as_int(key_text)
            season_from_key = _number_from_text(key_text, "season")
            if isinstance(child, list) and (numeric is not None or season_from_key is not None):
                next_season = season_from_key if season_from_key is not None else numeric
                next_episode = None
            elif isinstance(child, dict) and numeric is not None:
                if season_hint is None:
                    next_season = season_from_key if season_from_key is not None else numeric
                    next_episode = None
                else:
                    next_episode = numeric
            visit(child, next_season, next_episode, depth + 1)

    visit(root)
    groups = {}
    for record in found:
        groups.setdefault(record["season"], []).append(record)
    for season in groups:
        groups[season].sort(key=lambda item: (item.get("episode", 0), item.get("title") or ""))
    return groups

def series_seasons(account, series_id):
    key = ("series_info:%s" % series_id, account["id"])
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        if isinstance(cached, dict):
            normalized = {}
            for season_key, items in cached.items():
                season_num = _as_int(season_key)
                if season_num is not None and isinstance(items, list):
                    normalized[season_num] = items
            return normalized
        return {}
    data = _series_info_raw(account, series_id)
    if not isinstance(data, dict) or not data:
        return {}
    root = data.get("episodes")
    if root is None:
        # Some panels wrap the episode tree in data/result. Keep this fallback
        # narrow so unrelated metadata is not interpreted as episodes.
        for key_name in ("data", "result"):
            wrapped = data.get(key_name)
            if isinstance(wrapped, dict) and "episodes" in wrapped:
                root = wrapped.get("episodes")
                break
    episodes = _parse_episode_tree(account, root if root is not None else {})
    _cache_set(key, episodes, persistent=True)
    return episodes

def update_account(account_id, patch):
    accounts_value = load_xtream_accounts()
    for account in accounts_value:
        if account.get("id") == account_id:
            account.update(patch)
            break
    save_xtream_accounts(accounts_value)
    _cache_invalidate_account(account_id)


def set_xtream_enabled(account_id, enabled):
    result = _storage_set_xtream_enabled(account_id, enabled)
    _cache_invalidate_account(account_id)
    return result


def remove_xtream(account_id):
    _cache_invalidate_account(account_id)
    return _storage_remove_xtream(account_id)


def remove_account(account_id):
    return remove_xtream(account_id)


def toggle_enabled(account_id):
    accounts_value = load_xtream_accounts()
    changed = False
    for account in accounts_value:
        if account.get("id") == account_id:
            account["enabled"] = not account.get("enabled", True)
            changed = True
            break
    if changed:
        save_xtream_accounts(accounts_value)
        _cache_invalidate_account(account_id)
    return changed

# ---- Conservative title lookup used by the Cinemeta aggregator ------------

def _match_text(value):
    """Normalize provider titles for Cinemeta↔Xtream matching.

    Xtream names commonly append years, quality, codec, language and release
    tags. Matching those decorations literally makes an otherwise identical
    title fail. Keep the semantic title and discard only well-known packaging
    noise; accents are folded so localized Latin titles remain comparable.
    """
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    # Bracket/parenthesis content is overwhelmingly provider/release metadata.
    text = re.sub(r"\[[^\]]*\]|\([^\)]*\)", " ", text)
    text = re.sub(r"\b(?:19|20)\d{2}\b", " ", text)
    text = re.sub(r"\bs\d{1,3}e\d{1,4}\b|\b\d{1,3}x\d{1,4}\b", " ", text)
    text = re.sub(r"\b(?:season|temporada|episode|episodio|episódio|ep)\s*\d+\b", " ", text)
    text = re.sub(
        r"\b(?:2160p|1080p|720p|480p|4k|uhd|hdr10?|dv|dolbyvision|bluray|blu-ray|bdrip|"
        r"webrip|web-dl|webdl|web|hdtv|remux|x264|x265|h264|h265|hevc|av1|aac|ac3|eac3|"
        r"dts|atmos|dual|multi|dublado|dubbed|legendado|subbed|latino|proper|repack|extended|"
        r"complete|completo|collection|colecao|coleção|pack|hdcam|ts|tc)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_TITLE_NOISE = {
    "the", "a", "an", "and", "of", "to", "in", "on", "for", "with",
    "complete", "series", "serie", "tv", "show", "season", "temporada",
    "episode", "episodio", "part", "collection", "pack", "movie", "filme",
}


def _title_tokens(value):
    return {token for token in _match_text(value).split()
            if len(token) >= 3 and token not in _TITLE_NOISE}


def _title_match_score(query, candidate, requested_year=None, candidate_year=None):
    """Score names using the same conservative intent as the Android ARVIO index."""
    query_n = _match_text(query)
    candidate_n = _match_text(candidate)
    if not query_n or not candidate_n:
        return 0
    if candidate_n == query_n:
        score = 120
    elif query_n in candidate_n:
        score = 90
    elif candidate_n in query_n:
        score = 70
    elif candidate_n.startswith(query_n) or query_n.startswith(candidate_n):
        score = 68
    else:
        q_tokens = _title_tokens(query_n)
        c_tokens = _title_tokens(candidate_n)
        if not q_tokens or not c_tokens:
            return 0
        overlap = len(q_tokens.intersection(c_tokens))
        coverage = float(overlap) / float(len(q_tokens))
        if overlap >= 2 and coverage >= 0.75:
            score = 75 + overlap
        elif overlap >= 2:
            score = 55 + overlap
        elif overlap == 1 and len(q_tokens) >= 3 and len(c_tokens) >= 3:
            score = 42
        elif overlap == 1 and len(q_tokens) <= 2:
            score = 35
        else:
            return 0

    requested = _year4(requested_year)
    candidate_y = _year4(candidate_year) or _year4(candidate)
    if requested and candidate_y:
        try:
            delta = abs(int(requested) - int(candidate_y))
            if delta == 0:
                score += 20
            elif delta == 1:
                score += 5
            else:
                score -= 60
        except (TypeError, ValueError):
            pass
    return max(0, score)


def _candidate_titles(query, alternate_titles=None):
    values = [query]
    values.extend(alternate_titles or [])
    out = []
    seen = set()
    for value in values:
        normalized = _match_text(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(str(value))
    return out


def _identity_score(item, imdb_id=None, tmdb_id=None):
    wanted_imdb = _normalize_imdb_id(imdb_id)
    wanted_tmdb = _normalize_tmdb_id(tmdb_id)
    item_imdb = _normalize_imdb_id(item.get("imdbId"))
    item_tmdb = _normalize_tmdb_id(item.get("tmdbId"))
    if wanted_imdb and item_imdb and wanted_imdb == item_imdb:
        return 10000
    if wanted_tmdb and item_tmdb and wanted_tmdb == item_tmdb:
        return 9500
    return 0


def _best_title_score(titles, candidate, year=None, candidate_year=None):
    return max((_title_match_score(title, candidate, year, candidate_year) for title in titles), default=0)


def _external_ids_from_payload(payload):
    imdb = tmdb = ""
    if not isinstance(payload, dict):
        return imdb, tmdb
    sources = [payload]
    for key in ("info", "movie_data", "series", "data", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            sources.append(value)
    for source in sources:
        if not imdb:
            imdb = _normalize_imdb_id(_external_id_value(source, {}, ("imdb", "imdb_id", "imdbId", "imdbid")))
        if not tmdb:
            tmdb = _normalize_tmdb_id(_external_id_value(source, {}, ("tmdb", "tmdb_id", "tmdbId", "tmdbid")))
    return imdb, tmdb


def _vod_info_raw(account, vod_id, timeout=None):
    key = ("vod_info_raw:%s" % vod_id, account.get("id"))
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached if isinstance(cached, dict) else {}
    try:
        data = _api_json(account, "get_vod_info", timeout=timeout, vod_id=vod_id)
    except Exception:
        return {}
    if isinstance(data, dict):
        _cache_set(key, data, persistent=True)
        return data
    return {}


def _series_info_raw(account, series_id, timeout=None):
    key = ("series_raw:%s" % series_id, account.get("id"))
    cached = _cache_get(key, persistent=True)
    if cached is not None:
        return cached if isinstance(cached, dict) else {}
    try:
        data = _api_json(account, "get_series_info", timeout=timeout, series_id=series_id)
    except Exception:
        return {}
    if isinstance(data, dict):
        _cache_set(key, data, persistent=True)
        return data
    return {}


def _bounded_identity_probe(account, items, imdb_id=None, tmdb_id=None, year=None,
                             kind="vod", max_probe=48, titles=None, budget_s=10.0):
    """Probe rich Xtream details under a strict wall-clock budget.

    Summary rows often omit IMDb/TMDB IDs. The old batched implementation could
    spend one full HTTP timeout per batch, keeping Kodi's progress dialog open
    for a minute or more on a slow panel. This variant keeps the same candidate
    cap but bounds the probe in wall-clock time and uses a shorter request timeout.
    """
    wanted_imdb = _normalize_imdb_id(imdb_id)
    wanted_tmdb = _normalize_tmdb_id(tmdb_id)
    if not (wanted_imdb or wanted_tmdb):
        return []
    requested_year = _year4(year)
    titles = list(titles or [])
    ranked = []
    for index, item in enumerate(items or []):
        name = str((item or {}).get("name") or "")
        item_year = _year4((item or {}).get("year")) or _year4(name)
        title_score = _best_title_score(titles, name, requested_year, item_year) if titles else 0
        year_score = 0
        if requested_year and item_year:
            try:
                delta = abs(int(requested_year) - int(item_year))
                year_score = 300 if delta == 0 else (120 if delta == 1 else -80)
            except (TypeError, ValueError):
                pass
        elif requested_year:
            year_score = 5
        try:
            freshness = int(str((item or {}).get("added") or "0"))
        except (TypeError, ValueError):
            freshness = 0
        try:
            numeric_id = int(str((item or {}).get("id") or "0"))
        except (TypeError, ValueError):
            numeric_id = 0
        ranked.append((title_score * 10 + year_score, freshness, numeric_id, -index, item))
    ranked.sort(key=lambda row: (-row[0], -row[1], -row[2], -row[3]))
    candidates = [row[4] for row in ranked[:max(1, int(max_probe or 1))]]
    if not candidates:
        return []

    request_timeout = max(2.0, min(4.0, float(budget_s)))
    started = time.monotonic()
    probed = 0
    matched = []

    def fetch(item):
        item_id = item.get("id")
        if kind == "vod":
            return item, _vod_info_raw(account, item_id, timeout=request_timeout)
        return item, _series_info_raw(account, item_id, timeout=request_timeout)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(candidates)))
    futures = [executor.submit(fetch, item) for item in candidates]
    pending = set(futures)
    try:
        while pending and time.monotonic() - started < float(budget_s):
            remaining = float(budget_s) - (time.monotonic() - started)
            done, pending = concurrent.futures.wait(
                pending, timeout=max(0.01, min(0.25, remaining)),
                return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                probed += 1
                try:
                    item, payload = future.result()
                except Exception:
                    continue
                payload = payload or {}
                found_imdb, found_tmdb = _external_ids_from_payload(payload)
                if ((wanted_imdb and found_imdb == wanted_imdb) or
                        (wanted_tmdb and found_tmdb == wanted_tmdb)):
                    entry = dict(item)
                    if found_imdb:
                        entry["imdbId"] = found_imdb
                    if found_tmdb:
                        entry["tmdbId"] = found_tmdb
                    info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
                    movie_data = payload.get("movie_data") if isinstance(payload.get("movie_data"), dict) else {}
                    entry["plot"] = (entry.get("plot") or info.get("plot") or info.get("description") or
                                     movie_data.get("plot") or "")
                    matched.append(entry)
            if matched:
                break
        for future in pending:
            future.cancel()
    finally:
        executor.shutdown(wait=True)

    if matched:
        Kodi.log("XTREAM", "Probe de identidade %s: %d detalhe(s), %d correspondência(s)." %
                 (kind, probed, len(matched)), level="info")
        return matched
    Kodi.log("XTREAM", "Probe de identidade %s: %d detalhe(s), sem ID correspondente (%.1fs)." %
             (kind, probed, time.monotonic() - started), level="debug")
    return []

def _rank_catalog_items(items, titles, year=None, imdb_id=None, tmdb_id=None):
    ranked = []
    query_norms = []
    for t in (titles or []):
        norm = _match_text(t)
        if norm:
            query_norms.append(norm)
    for item in items or []:
        identity = _identity_score(item, imdb_id, tmdb_id)
        name = str(item.get("name") or "")
        if identity >= 9500:
            ranked.append((identity, name, item))
            continue
        candidate_n = _match_text(name)
        if not candidate_n:
            continue
        if query_norms:
            if not any(qn in candidate_n or candidate_n in qn for qn in query_norms):
                continue
        title_score = _best_title_score(titles, name, year, item.get("year"))
        score = identity + title_score
        if score >= 25:
            ranked.append((score, name, item))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    return ranked



_LARGE_UNFILTERED_CATALOG = 5000


def _bounded_category_candidates(account, groups_func, items_func, titles, year=None,
                                 imdb_id=None, tmdb_id=None, max_categories=16,
                                 budget_s=8.0, label="VOD"):
    """Search category-only panels without turning a Cinemeta click into a full crawl.

    Categories are fetched in small batches and the function returns as soon as
    title/identity candidates appear.  It is deliberately a lookup fallback,
    not a complete catalog builder.
    """
    started = time.monotonic()
    request_timeout = max(2.0, min(3.0, float(budget_s)))
    try:
        try:
            groups = groups_func(account, timeout=request_timeout) or []
        except TypeError:
            groups = groups_func(account) or []
    except Exception as exc:
        Kodi.log("XTREAM", "%s: categorias indisponíveis no fallback: %s" % (label, exc), level="debug")
        return []
    year4 = _year4(year)
    title_tokens = set()
    for value in titles or []:
        title_tokens.update(_title_tokens(value))

    def group_priority(group):
        name = _match_text((group or {}).get("category_name") or "")
        score = 0
        if year4 and year4 in name:
            score += 100
        if any(token in name for token in title_tokens):
            score += 40
        if any(word in name for word in ("lancamento", "release", "novidade", "novo", "movie", "filme", "serie")):
            score += 10
        return score

    ordered = sorted(groups, key=group_priority, reverse=True)
    ids = [str((g or {}).get("category_id") or "") for g in ordered]
    ids = [cid for cid in ids if cid][:max(1, int(max_categories or 1))]
    if not ids:
        return []

    scanned = 0
    for pos in range(0, len(ids), 4):
        remaining = float(budget_s) - (time.monotonic() - started)
        if remaining <= 0:
            break
        batch = ids[pos:pos + 4]
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(batch))
        futures = []
        try:
            for cid in batch:
                def fetch(category_id=cid):
                    try:
                        return items_func(account, category_id, timeout=request_timeout)
                    except TypeError:
                        return items_func(account, category_id)
                futures.append(executor.submit(fetch))
            done, pending = concurrent.futures.wait(
                futures, timeout=max(0.1, min(remaining, request_timeout + 0.5)))
            rows = []
            for future in done:
                scanned += 1
                try:
                    rows.extend(future.result() or [])
                except Exception:
                    pass
            for future in pending:
                future.cancel()
        finally:
            executor.shutdown(wait=True)
        ranked = _rank_catalog_items(rows, titles, year, imdb_id, tmdb_id)
        if ranked:
            Kodi.log("XTREAM", "%s: fallback por categoria encontrou candidato após %d grupo(s)." %
                     (label, scanned), level="info")
            return [row[2] for row in ranked]
    Kodi.log("XTREAM", "%s: fallback por categoria encerrou em %.1fs (%d grupo(s)), sem candidato." %
             (label, time.monotonic() - started, scanned), level="debug")
    return []


def search_vod_by_title(query, year=None, limit_per_account=8, imdb_id=None, tmdb_id=None,
                        alternate_titles=None, fast_only=False, max_accounts=None, request_timeout=10.0):
    """Resolve a Cinemeta movie with a genuinely fast unfiltered-first path.

    Large unfiltered catalogs are searched directly and never followed by a
    redundant all-category crawl.  On a title miss, IMDb/TMDB rich details are
    probed under a strict budget; only small/partial panels get a bounded
    category fallback.
    """
    titles = _candidate_titles(query, alternate_titles)
    if not titles and not _normalize_imdb_id(imdb_id) and not _normalize_tmdb_id(tmdb_id):
        return []
    out = []
    accounts = enabled_accounts()
    if max_accounts is not None:
        accounts = accounts[:max(1, int(max_accounts))]
    for account in accounts:
        try:
            films = vod_streams(account, timeout=float(request_timeout))
        except Exception as exc:
            Kodi.log("XTREAM", "Fast-path VOD falhou em %s: %s" % (account.get("name"), exc), level="debug")
            films = []
        ranked = _rank_catalog_items(films, titles, year, imdb_id, tmdb_id)
        if ranked:
            Kodi.log("XTREAM", "Matcher VOD %s: fast-path=%d, candidatos=%d." %
                     (account.get("name") or "Conta", len(films), len(ranked)), level="info")
        elif fast_only:
            continue
        else:
            # If the summary list is huge, the observed real-panel behaviour is
            # that category calls merely re-return subsets of exactly the same
            # rows.  Probe identity against the already available catalog first.
            if films and (imdb_id or tmdb_id):
                matches = _bounded_identity_probe(
                    account, films, imdb_id=imdb_id, tmdb_id=tmdb_id, year=year,
                    kind="vod", max_probe=64, titles=titles, budget_s=min(6.0, float(request_timeout)))
                for film in matches:
                    ranked.append((10000 + _best_title_score(titles, film.get("name"), year, film.get("year")),
                                   str(film.get("name") or ""), film))
                ranked.sort(key=lambda row: (-row[0], row[1]))

            # Category fallback is useful only when the unfiltered endpoint is
            # plausibly partial.  Never crawl 49+ categories behind a Kodi
            # progress dialog when 5k+ rows are already in hand.
            if not ranked and len(films) < _LARGE_UNFILTERED_CATALOG:
                candidates = _bounded_category_candidates(
                    account, vod_groups, vod_streams, titles, year=year,
                    imdb_id=imdb_id, tmdb_id=tmdb_id, max_categories=16,
                    budget_s=min(6.0, float(request_timeout)), label="VOD")
                for film in candidates:
                    score = _identity_score(film, imdb_id, tmdb_id) + _best_title_score(
                        titles, film.get("name"), year, film.get("year"))
                    ranked.append((score, str(film.get("name") or ""), film))
                ranked.sort(key=lambda row: (-row[0], row[1]))
            elif not ranked and len(films) >= _LARGE_UNFILTERED_CATALOG:
                Kodi.log("XTREAM", "VOD: catálogo direto com %d itens; varredura total por categorias suprimida." %
                         len(films), level="info")

        Kodi.log("XTREAM", "Matcher VOD %s: catálogo=%d, candidatos=%d, imdb=%s, tmdb=%s." %
                 (account.get("name") or "Conta", len(films), len(ranked),
                  _normalize_imdb_id(imdb_id) or "-", _normalize_tmdb_id(tmdb_id) or "-"), level="info")
        for score, _, film in ranked[:max(1, int(limit_per_account or 1))]:
            entry = dict(film)
            entry["accountId"] = account.get("id")
            entry["accountName"] = account.get("name")
            entry["sourceKind"] = "xtream"
            entry["_xtream_score"] = score
            out.append(entry)
    return out


def search_series_by_title(query, year=None, limit_per_account=5, imdb_id=None, tmdb_id=None,
                           alternate_titles=None, fast_only=False, max_accounts=None, request_timeout=10.0):
    """Resolve series metadata without exhaustive category crawling."""
    titles = _candidate_titles(query, alternate_titles)
    if not titles and not _normalize_imdb_id(imdb_id) and not _normalize_tmdb_id(tmdb_id):
        return []
    out = []
    accounts = enabled_accounts()
    if max_accounts is not None:
        accounts = accounts[:max(1, int(max_accounts))]
    for account in accounts:
        try:
            items = series_list(account, timeout=float(request_timeout))
        except Exception as exc:
            Kodi.log("XTREAM", "Fast-path séries falhou em %s: %s" % (account.get("name"), exc), level="debug")
            items = []
        ranked = _rank_catalog_items(items, titles, year, imdb_id, tmdb_id)
        if ranked:
            Kodi.log("XTREAM", "Matcher séries %s: fast-path=%d, candidatos=%d." %
                     (account.get("name") or "Conta", len(items), len(ranked)), level="info")
        elif fast_only:
            continue
        else:
            if items and (imdb_id or tmdb_id):
                matches = _bounded_identity_probe(
                    account, items, imdb_id=imdb_id, tmdb_id=tmdb_id, year=year,
                    kind="series", max_probe=48, titles=titles, budget_s=min(6.0, float(request_timeout)))
                for series in matches:
                    ranked.append((10000 + _best_title_score(titles, series.get("name"), year, series.get("year")),
                                   str(series.get("name") or ""), series))
                ranked.sort(key=lambda row: (-row[0], row[1]))
            if not ranked and len(items) < _LARGE_UNFILTERED_CATALOG:
                candidates = _bounded_category_candidates(
                    account, series_groups, series_list, titles, year=year,
                    imdb_id=imdb_id, tmdb_id=tmdb_id, max_categories=16,
                    budget_s=min(6.0, float(request_timeout)), label="SÉRIES")
                for series in candidates:
                    score = _identity_score(series, imdb_id, tmdb_id) + _best_title_score(
                        titles, series.get("name"), year, series.get("year"))
                    ranked.append((score, str(series.get("name") or ""), series))
                ranked.sort(key=lambda row: (-row[0], row[1]))
            elif not ranked and len(items) >= _LARGE_UNFILTERED_CATALOG:
                Kodi.log("XTREAM", "SÉRIES: catálogo direto com %d itens; varredura total por categorias suprimida." %
                         len(items), level="info")
        Kodi.log("XTREAM", "Matcher séries %s: catálogo=%d, candidatos=%d, imdb=%s, tmdb=%s." %
                 (account.get("name") or "Conta", len(items), len(ranked),
                  _normalize_imdb_id(imdb_id) or "-", _normalize_tmdb_id(tmdb_id) or "-"), level="info")
        for score, _, series in ranked[:max(1, int(limit_per_account or 1))]:
            entry = dict(series)
            entry["accountId"] = account.get("id")
            entry["accountName"] = account.get("name")
            entry["sourceKind"] = "xtream"
            entry["_xtream_score"] = score
            out.append(entry)
    return out


def _episode_marker_matches(name, season, episode):
    # Episode markers must be inspected before title normalization because the
    # latter intentionally strips SxxEyy/season/episode packaging noise.
    raw = unicodedata.normalize("NFKD", str(name or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch)).lower()
    compact = re.sub(r"[^a-z0-9]+", "", raw)
    patterns = (
        "s%02de%02d" % (season, episode), "s%de%d" % (season, episode),
        "%dx%02d" % (season, episode), "%dx%d" % (season, episode),
    )
    if any(pattern in compact for pattern in patterns):
        return True
    ep_num = _number_from_text(name, "episode")
    season_num = _number_from_text(name, "season")
    if ep_num != episode or (season_num not in (None, season)):
        return False
    return not (season > 1 and season_num is None)


def _episode_name_match(name, titles, season, episode):
    if not _episode_marker_matches(name, season, episode):
        return 0
    cleaned = re.sub(r"(?i)s\d{1,3}e\d{1,4}|\d{1,3}x\d{1,4}|episode\s*\d+|ep\.?\s*\d+", " ", name or "")
    return _best_title_score(titles, cleaned)


def _search_episode_in_vod(query, season, episode, year=None, imdb_id=None, tmdb_id=None, alternate_titles=None, limit_per_account=4, request_timeout=10.0):
    """Fallback for panels that publish series episodes under get_vod_streams."""
    titles = _candidate_titles(query, alternate_titles)
    out = []
    for account in enabled_accounts():
        try:
            films = vod_streams(account, timeout=float(request_timeout))
        except Exception:
            continue
        ranked = []
        for film in films:
            name = film.get("name") or ""
            if not _episode_marker_matches(name, season, episode):
                continue
            name_score = _episode_name_match(name, titles, season, episode)
            identity = _identity_score(film, imdb_id, tmdb_id)
            if not name_score and not identity:
                continue
            ranked.append((identity + name_score, film))
        ranked.sort(key=lambda row: -row[0])
        for score, film in ranked[:max(1, int(limit_per_account or 1))]:
            entry = dict(film)
            entry["seriesName"] = query
            entry["accountId"] = account.get("id")
            entry["accountName"] = account.get("name")
            entry["sourceKind"] = "xtream"
            entry["_xtream_score"] = score
            out.append(entry)
    return out


def search_series_episode_by_title(query, season, episode, year=None, limit_per_account=5,
                                   imdb_id=None, tmdb_id=None, alternate_titles=None, fast_only=False,
                                   max_accounts=None, request_timeout=10.0):
    """Resolve Cinemeta title/S/E to real Xtream episode URLs with safe fallbacks."""
    season_num = _as_int(season)
    episode_num = _as_int(episode)
    if season_num is None or episode_num is None:
        return []
    candidates = search_series_by_title(
        query, year=year, limit_per_account=limit_per_account, imdb_id=imdb_id,
        tmdb_id=tmdb_id, alternate_titles=alternate_titles, fast_only=fast_only,
        max_accounts=max_accounts, request_timeout=request_timeout)
    out = []
    for series in candidates:
        account = enabled_account_by_id(series.get("accountId"))
        if not account:
            continue
        try:
            groups = series_seasons(account, series.get("id"))
        except Exception as exc:
            Kodi.log("XTREAM", "Episódios falharam em %s/%s: %s" %
                     (account.get("name"), series.get("name"), exc), level="warning")
            continue
        for item in groups.get(int(season_num), []):
            if _as_int(item.get("episode")) != int(episode_num):
                continue
            entry = dict(item)
            entry["seriesId"] = series.get("id")
            entry["seriesName"] = series.get("name")
            entry["accountId"] = account.get("id")
            entry["accountName"] = account.get("name")
            entry["sourceKind"] = "xtream"
            out.append(entry)
            break
    if out:
        return out
    if fast_only:
        return []
    return _search_episode_in_vod(
        query, int(season_num), int(episode_num), year=year, imdb_id=imdb_id,
        tmdb_id=tmdb_id, alternate_titles=alternate_titles,
        limit_per_account=limit_per_account, request_timeout=request_timeout)

