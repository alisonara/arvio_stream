# -*- coding: utf-8 -*-
"""Small fail-open title alias resolver for cross-language provider matching.

Cinemeta is predominantly English. Xtream libraries and Telegram filenames are
frequently localized.  The full ARVIO app solves this with localized TMDB
metadata; the Kodi addon keeps the same intent without shipping an API secret by
using Wikidata's public MediaWiki API only after a normal lookup returned no
match.  Failure never blocks source resolution.
"""
from __future__ import annotations

import threading
import urllib.parse

from . import http
from .kodi_utils import Kodi

_API = "https://www.wikidata.org/w/api.php"
_cache = {}
_lock = threading.Lock()


def _norm(value):
    import re, unicodedata
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _request(params, timeout=4):
    url = _API + "?" + urllib.parse.urlencode(params)
    return http.get_json(url, timeout=timeout)


def localized_titles(title, year=None):
    title = str(title or "").strip()
    if len(title) < 2:
        return []
    key = (_norm(title), str(year or "")[:4])
    with _lock:
        if key in _cache:
            return list(_cache[key])

    aliases = []
    try:
        search_text = title
        if year:
            search_text += " " + str(year)[:4]
        payload = _request({
            "action": "wbsearchentities", "search": search_text,
            "language": "en", "uselang": "en", "limit": 3, "format": "json",
        })
        results = payload.get("search") if isinstance(payload, dict) else []
        if not results and year:
            payload = _request({
                "action": "wbsearchentities", "search": title,
                "language": "en", "uselang": "en", "limit": 3, "format": "json",
            })
            results = payload.get("search") if isinstance(payload, dict) else []
        qid = str((results or [{}])[0].get("id") or "") if results else ""
        if qid:
            entity_data = _request({
                "action": "wbgetentities", "ids": qid,
                "props": "labels|aliases", "languages": "pt-br|pt|en",
                "languagefallback": "1", "format": "json",
            })
            entity = ((entity_data.get("entities") or {}).get(qid) or {}) if isinstance(entity_data, dict) else {}
            values = []
            for lang in ("pt-br", "pt", "en"):
                label = ((entity.get("labels") or {}).get(lang) or {}).get("value")
                if label:
                    values.append(label)
                for alias in (entity.get("aliases") or {}).get(lang) or []:
                    if isinstance(alias, dict) and alias.get("value"):
                        values.append(alias.get("value"))
            seen = {_norm(title)}
            for value in values:
                value = str(value or "").strip()
                norm = _norm(value)
                if value and norm and norm not in seen:
                    seen.add(norm)
                    aliases.append(value)
                    if len(aliases) >= 12:
                        break
    except Exception as exc:
        Kodi.log("META", "Aliases localizados indisponíveis: %s" % exc, level="debug")
        aliases = []

    with _lock:
        _cache[key] = list(aliases)
    if aliases:
        Kodi.log("META", "Aliases localizados: %d para '%s'" % (len(aliases), title), level="info")
    return aliases


def clear_cache():
    with _lock:
        _cache.clear()


def _claim_text(entity, prop):
    """Return simple string claim values from a Wikidata entity."""
    out = []
    claims = (entity or {}).get("claims") or {}
    for claim in claims.get(prop) or []:
        try:
            value = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value")
        except Exception:
            value = None
        if value not in (None, ""):
            out.append(str(value).strip())
    return out


def localized_titles_for_identity(title, year=None, imdb_id=None):
    """Resolve localized aliases, preferring the Wikidata entity with matching IMDb ID.

    This is Xtream-specific.  ``localized_titles`` above remains unchanged so the
    homologated Telegram search path keeps exactly the same alias behaviour.
    """
    import re
    title = str(title or "").strip()
    imdb = str(imdb_id or "").strip().lower()
    match = re.search(r"tt\d+", imdb)
    imdb = match.group(0) if match else ""
    if not imdb:
        return localized_titles(title, year=year)
    if len(title) < 2:
        return []

    key = ("identity", _norm(title), str(year or "")[:4], imdb)
    with _lock:
        if key in _cache:
            return list(_cache[key])

    aliases = []
    try:
        qids = []
        # Keep alias resolution bounded: at most two entity-search requests and
        # one batched entity fetch.  A metadata helper must never become the new
        # reason the Kodi source dialog stays open.
        searches = ["%s %s" % (title, str(year)[:4])] if year else []
        searches.append(title)
        for search_text in searches[:2]:
            payload = _request({
                "action": "wbsearchentities", "search": search_text,
                "language": "en", "uselang": "en", "limit": 5, "format": "json",
            }, timeout=2.5)
            for row in (payload.get("search") if isinstance(payload, dict) else []) or []:
                qid = str((row or {}).get("id") or "")
                if qid and qid not in qids:
                    qids.append(qid)
            if qids:
                break

        entities = {}
        if qids:
            entity_data = _request({
                "action": "wbgetentities", "ids": "|".join(qids[:5]),
                "props": "labels|aliases|claims", "languages": "pt-br|pt|es|en",
                "languagefallback": "1", "format": "json",
            }, timeout=2.5)
            if isinstance(entity_data, dict):
                entities = entity_data.get("entities") or {}

        selected = None
        for qid in qids:
            entity = entities.get(qid) or {}
            values = [value.lower() for value in _claim_text(entity, "P345")]
            if imdb in values:
                selected = entity
                break
        if selected is None and qids:
            selected = entities.get(qids[0]) or {}

        values = []
        if selected:
            for lang in ("pt-br", "pt", "es", "en"):
                label = ((selected.get("labels") or {}).get(lang) or {}).get("value")
                if label:
                    values.append(label)
                for alias in (selected.get("aliases") or {}).get(lang) or []:
                    if isinstance(alias, dict) and alias.get("value"):
                        values.append(alias.get("value"))
        seen = {_norm(title)}
        for value in values:
            value = str(value or "").strip()
            norm = _norm(value)
            if value and norm and norm not in seen:
                seen.add(norm)
                aliases.append(value)
                if len(aliases) >= 16:
                    break
    except Exception as exc:
        Kodi.log("META", "Aliases por IMDb indisponíveis: %s" % exc, level="debug")
        aliases = []

    # Do not chain another network resolver here. If identity-aware aliases are
    # unavailable, Xtream's bounded IMDb/TMDB detail probe is the fail-open path.

    with _lock:
        _cache[key] = list(aliases)
    if aliases:
        Kodi.log("META", "Aliases Xtream por identidade: %d para '%s' (%s)" %
                 (len(aliases), title, imdb), level="info")
    return aliases

