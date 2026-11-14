# -*- coding: utf-8 -*-
"""Stremio addon protocol client.

The module preserves Stremio IDs verbatim and treats installed addons as a
cooperating set: catalog/meta may come from one addon while streams/subtitles
can be supplied by other compatible addons.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import re
import urllib.parse

from . import http
from .kodi_utils import Kodi
from .const import STREMIO_STREAM_TIMEOUT, STREMIO_RESOLVE_BUDGET
from .storage import load_addons, save_addons, upsert_addon, remove_addon


CINEMETA_MANIFEST = "https://v3-cinemeta.strem.io/manifest.json"


class AddonError(Exception):
    pass


def normalize_manifest_url(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith("stremio://"):
        raw = "https://" + raw[len("stremio://"):]
    elif not re.match(r"^https?://", raw, re.I):
        raw = "https://" + raw
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    path = parsed.path or ""
    if not path.endswith("/manifest.json") and path != "/manifest.json":
        path = path.rstrip("/") + "/manifest.json"
    # Query parameters frequently contain an addon's configuration/token and
    # must survive normalization. Fragments are client-side only.
    return urllib.parse.urlunparse(parsed._replace(path=path, fragment=""))


def addon_base_url(manifest_url):
    parsed = urllib.parse.urlparse(manifest_url)
    query = parsed.query
    path = parsed.path
    if path.endswith("/manifest.json"):
        path = path[:-len("/manifest.json")]
    elif path == "/manifest.json":
        path = ""
    base = urllib.parse.urlunparse(parsed._replace(path=path.rstrip("/"), query="", fragment="")).rstrip("/")
    return base, query


def _with_config_query(url, query):
    if not query:
        return url
    return url + ("&" if "?" in url else "?") + query


def fetch_manifest(manifest_url):
    manifest_url = normalize_manifest_url(manifest_url)
    if not manifest_url:
        raise AddonError("URL do addon inválida.")
    try:
        manifest = http.get_json(manifest_url)
    except http.HttpError as exc:
        raise AddonError("Falha ao buscar o manifest (%s)." % (exc.status or exc))
    if not isinstance(manifest, dict) or "resources" not in manifest:
        raise AddonError("Resposta não parece um manifest do Stremio.")
    return manifest, manifest_url


def installed_addon_from_manifest(manifest, manifest_url):
    catalogs = []
    for catalog in manifest.get("catalogs") or []:
        if isinstance(catalog, dict) and catalog.get("type") and catalog.get("id"):
            catalogs.append({
                "type": str(catalog.get("type")),
                "id": str(catalog.get("id")),
                "name": catalog.get("name") or catalog.get("id"),
                "extra": catalog.get("extra") or [],
                "genres": catalog.get("genres") or [],
                "extraSupported": catalog.get("extraSupported") or [],
                "extraRequired": catalog.get("extraRequired") or [],
            })
    resources = []
    for resource in manifest.get("resources") or []:
        if isinstance(resource, str):
            resources.append(resource)
        elif isinstance(resource, dict) and resource.get("name"):
            resources.append({
                "name": resource.get("name"),
                "types": resource.get("types") or [],
                "idPrefixes": resource.get("idPrefixes") or [],
            })
    return {
        "id": manifest.get("id") or manifest_url,
        "name": manifest.get("name") or "Addon sem nome",
        "version": manifest.get("version") or "1.0.0",
        "manifestUrl": manifest_url,
        "description": manifest.get("description"),
        "logo": manifest.get("logo"),
        "background": manifest.get("background"),
        "types": manifest.get("types") or [],
        "idPrefixes": manifest.get("idPrefixes") or [],
        "resources": resources,
        "catalogs": catalogs,
        "enabled": True,
    }


def install_addon(raw_url):
    manifest, manifest_url = fetch_manifest(raw_url)
    addon = installed_addon_from_manifest(manifest, manifest_url)
    # Preserve an existing disabled state when reinstalling/updating the same URL.
    previous = find_addon(manifest_url)
    if previous is not None:
        addon["enabled"] = previous.get("enabled", True)
    upsert_addon(addon)
    return addon


def ensure_cinemeta_installed():
    """Return the persisted Cinemeta addon, installing it only when missing.

    A user-disabled Cinemeta remains disabled; opening the catalog must not
    silently override an explicit settings choice.
    """
    current = find_addon(CINEMETA_MANIFEST)
    if current is not None:
        return current
    try:
        return install_addon(CINEMETA_MANIFEST)
    except Exception as exc:
        raise AddonError("Não foi possível instalar o Cinemeta: %s" % exc)


def list_installed():
    return load_addons()


def set_addon_enabled(manifest_url, enabled):
    addons = load_addons()
    for addon in addons:
        if addon.get("manifestUrl") == manifest_url:
            addon["enabled"] = bool(enabled)
            break
    save_addons(addons)


def uninstall_addon(manifest_url):
    remove_addon(manifest_url)


def find_addon(manifest_url):
    for addon in load_addons():
        if addon.get("manifestUrl") == manifest_url:
            return addon
    return None


def enabled_addons():
    return [a for a in load_addons() if a.get("enabled", True)]


def _type_aliases(media_type):
    media_type = (media_type or "").lower()
    return {"movie": ("movie", "film"), "series": ("series", "tv", "show")}.get(media_type, (media_type,))


def supports_type(addon, media_type):
    declared = [str(t).lower() for t in (addon.get("types") or [])]
    if not declared:
        return True
    return any(alias in declared for alias in _type_aliases(media_type))


def resource_supports(addon, resource_name, media_type=None, item_id=None):
    resources = addon.get("resources") or []
    if not resources:
        return True
    matches = []
    for resource in resources:
        if isinstance(resource, str) and resource == resource_name:
            matches.append({"name": resource})
        elif isinstance(resource, dict) and resource.get("name") == resource_name:
            matches.append(resource)
    if not matches:
        return False
    for resource in matches:
        types = [str(t).lower() for t in (resource.get("types") or [])]
        if media_type and types and not any(alias in types for alias in _type_aliases(media_type)):
            continue
        prefixes = resource.get("idPrefixes") or addon.get("idPrefixes") or []
        if item_id and prefixes and not any(str(item_id).startswith(str(prefix)) for prefix in prefixes):
            continue
        return True
    return False


def has_resource(addon, resource):
    return resource_supports(addon, resource)


# ---- catalog browsing / search -------------------------------------------

def _catalog_extra_names(catalog):
    names = []
    for item in catalog.get("extra") or []:
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item.get("name")))
        elif isinstance(item, str):
            names.append(item)
    return names


def catalog_supports_search(catalog):
    return "search" in _catalog_extra_names(catalog)


def catalog_supports_skip(catalog):
    return "skip" in _catalog_extra_names(catalog)


def _catalog_url(base, query, media_type, catalog_id, search=None, extra=None, skip=None):
    """Build a Stremio catalog endpoint using protocol extraArgs in the path.

    Stremio catalog search/filter/pagination arguments belong after the catalog
    id (``.../catalog/type/id/search=...&skip=....json``), not in the HTTP query
    string. Any query already present on the configured manifest is preserved as
    addon configuration and appended after the resource URL.
    """
    url = "%s/catalog/%s/%s" % (
        base, urllib.parse.quote(str(media_type), safe=""),
        urllib.parse.quote(str(catalog_id), safe=""))
    params = []
    for item in extra or []:
        if isinstance(item, dict) and item.get("name"):
            key = str(item["name"])
            val = item.get("value")
            if isinstance(val, bool):
                val = "true" if val else "false"
            if val not in (None, ""):
                params.append((key, str(val)))
    if search is not None:
        params.append(("search", str(search)))
    if skip is not None:
        params.append(("skip", str(int(skip))))
    if params:
        # Quote keys/values while preserving the Stremio ``key=value&...`` shape
        # as one path segment. Slashes inside values are escaped.
        extra_args = urllib.parse.urlencode(params, doseq=True, quote_via=urllib.parse.quote)
        url += "/" + extra_args
    url += ".json"
    return _with_config_query(url, query)


def fetch_catalog(addon, media_type, catalog_id, search=None, extra=None, skip=None):
    base, query = addon_base_url(addon.get("manifestUrl") or "")
    url = _catalog_url(base, query, media_type, catalog_id, search, extra, skip)
    try:
        payload = http.get_json(url)
    except http.HttpError as exc:
        raise AddonError("Falha ao carregar catálogo (%s)." % (exc.status or exc))
    items = payload.get("metas") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return []
    return [normalize_meta(item, media_type, addon) for item in items if isinstance(item, dict)]


def search_addon(addon, media_type, query_text):
    if not addon.get("enabled", True) or not supports_type(addon, media_type):
        return []
    results = []
    catalogs = [c for c in (addon.get("catalogs") or []) if c.get("type") == media_type]
    searchable = [c for c in catalogs if catalog_supports_search(c)]
    # Spec-compliant path first. If an older addon forgot to declare the extra,
    # retain compatibility by trying its catalogs only when none is declared.
    candidates = searchable or catalogs
    for catalog in candidates:
        try:
            found = fetch_catalog(addon, media_type, catalog.get("id"), search=query_text)
        except AddonError:
            continue
        results.extend(found)
    # Compatibility with non-standard addons that expose a separate /search resource.
    if not results and resource_supports(addon, "search", media_type):
        base, query = addon_base_url(addon.get("manifestUrl") or "")
        search_url = "%s/search/%s/%s.json" % (base, media_type, urllib.parse.quote(query_text, safe=""))
        search_url = _with_config_query(search_url, query)
        try:
            payload = http.get_json(search_url)
            items = payload.get("metas") if isinstance(payload, dict) else payload
            if isinstance(items, list):
                results.extend(normalize_meta(item, media_type, addon) for item in items if isinstance(item, dict))
        except http.HttpError:
            pass
    return _dedupe_metas(results)


def _dedupe_metas(items):
    out, seen = [], set()
    for item in items:
        key = (item.get("type"), item.get("id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _cinemeta_catalog_ids():
    current_year = datetime.datetime.now().year
    return {
        "movie": [
            {"id": "top", "label": "Popular", "genre_required": False},
            {"id": "imdbRating", "label": "Destaques", "genre_required": False},
            {"id": "year", "label": "Lançamentos (%d)" % current_year,
             "genre_required": True, "preset_genre": str(current_year)},
            {"id": "year", "label": "Lançamentos", "genre_required": True},
        ],
        "series": [
            {"id": "top", "label": "Popular", "genre_required": False},
            {"id": "imdbRating", "label": "Destaques", "genre_required": False},
            {"id": "year", "label": "Lançamentos (%d)" % current_year,
             "genre_required": True, "preset_genre": str(current_year)},
            {"id": "year", "label": "Lançamentos", "genre_required": True},
        ],
    }


def cinemeta_categories(media_type):
    """Expose the stable user-facing Cinemeta categories used by ARVIO."""
    media_type = str(media_type or "").lower()
    addon = find_addon(CINEMETA_MANIFEST)
    available = set()
    if addon:
        available = {str(c.get("id")) for c in (addon.get("catalogs") or [])
                     if c.get("type") == media_type}
    categories = _cinemeta_catalog_ids().get(media_type, [])
    return [dict(c) for c in categories if not available or c.get("id") in available]


def cinemeta_search(media_type, query_text):
    addon = find_addon(CINEMETA_MANIFEST)
    if addon is None:
        try:
            addon = ensure_cinemeta_installed()
        except AddonError:
            return []
    if not addon.get("enabled", True):
        return []
    try:
        return search_addon(addon, media_type, query_text)
    except Exception as exc:
        Kodi.log("STREMIO", "Cinemeta search failed: %s" % exc, level="warning")
        return []


def cinemeta_catalog(media_type, catalog_id, genre=None, skip=None):
    addon = find_addon(CINEMETA_MANIFEST)
    if addon is None:
        addon = ensure_cinemeta_installed()
    if not addon.get("enabled", True):
        return []
    extra = [{"name": "genre", "value": genre}] if genre else None
    return fetch_catalog(addon, media_type, catalog_id, extra=extra, skip=skip)


def meta_id_of(item):
    return str(item.get("id") or "")


def _normalise_video(raw):
    if not isinstance(raw, dict) or not raw.get("id"):
        return None
    def _int_or_none(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return {
        "id": str(raw.get("id")),
        "title": raw.get("title") or raw.get("name") or "Episódio",
        "season": _int_or_none(raw.get("season")),
        "episode": _int_or_none(raw.get("episode") if raw.get("episode") is not None else raw.get("episodeNumber")),
        "released": raw.get("released"),
        "overview": raw.get("overview") or raw.get("description") or "",
        "thumbnail": raw.get("thumbnail") or raw.get("poster") or "",
    }


def normalize_meta(raw, media_type, addon):
    rid = str(raw.get("id") or "")
    titles = raw.get("name") or raw.get("title") or raw.get("releaseInfo") or rid
    if isinstance(titles, dict):
        title = titles.get("pt-BR") or titles.get("pt") or titles.get("en") or next(iter(titles.values()), rid)
    else:
        title = titles if isinstance(titles, str) else str(titles)
    videos = []
    for video in raw.get("videos") or []:
        parsed = _normalise_video(video)
        if parsed:
            videos.append(parsed)
    imdb_id = rid if rid.lower().startswith("tt") else (raw.get("imdb_id") or raw.get("imdbId") or "")
    external_ids = raw.get("externalIds") if isinstance(raw.get("externalIds"), dict) else {}
    if not imdb_id:
        imdb_id = external_ids.get("imdb") or external_ids.get("imdb_id") or ""
    imdb_match = re.search(r"(?i)tt\d+", str(imdb_id or ""))
    imdb_id = imdb_match.group(0).lower() if imdb_match else str(imdb_id or "").strip()
    tmdb_id = raw.get("tmdb_id") or raw.get("tmdbId") or external_ids.get("tmdb") or ""
    alternate_titles = []
    for value in (raw.get("originalName"), raw.get("originalTitle"), raw.get("name"), raw.get("title")):
        if isinstance(value, str) and value.strip() and value.strip() != title:
            alternate_titles.append(value.strip())
        elif isinstance(value, dict):
            for translated in value.values():
                if isinstance(translated, str) and translated.strip() and translated.strip() != title:
                    alternate_titles.append(translated.strip())
    aliases = raw.get("aliases") or raw.get("alternativeTitles") or []
    if isinstance(aliases, dict):
        aliases = list(aliases.values())
    if isinstance(aliases, (list, tuple)):
        for alias in aliases:
            if isinstance(alias, str) and alias.strip() and alias.strip() != title:
                alternate_titles.append(alias.strip())
    alternate_titles = list(dict.fromkeys(alternate_titles))[:12]
    return {
        "id": rid,
        "type": str(raw.get("type") or media_type),
        "addonName": addon.get("name"),
        "manifestUrl": addon.get("manifestUrl"),
        "title": title,
        "poster": raw.get("poster") or "",
        "backdrop": raw.get("background") or raw.get("backdrop") or "",
        "description": raw.get("description") or raw.get("overview") or "",
        "releaseInfo": raw.get("releaseInfo"),
        "year": raw.get("year"),
        "rating": raw.get("imdbRating") or raw.get("rating"),
        "imdbId": str(imdb_id or ""),
        "tmdbId": str(tmdb_id or ""),
        "alternateTitles": alternate_titles,
        "addonType": raw.get("type") or media_type,
        "videos": videos,
    }


# ---- meta / episodes ------------------------------------------------------

def resolve_meta(addon, media_type, item_id):
    if not resource_supports(addon, "meta", media_type, item_id):
        return None
    base, query = addon_base_url(addon.get("manifestUrl") or "")
    for request_type in stream_request_types(addon, media_type):
        url = "%s/meta/%s/%s.json" % (base, request_type, urllib.parse.quote(str(item_id), safe=":"))
        url = _with_config_query(url, query)
        try:
            payload = http.get_json(url)
        except http.HttpError:
            continue
        meta = payload.get("meta") if isinstance(payload, dict) else None
        if isinstance(meta, dict):
            return normalize_meta(meta, media_type, addon)
    return None


def _ordered_enabled(preferred_manifest_url=None):
    addons = enabled_addons()
    if not preferred_manifest_url:
        return addons
    return sorted(addons, key=lambda a: 0 if a.get("manifestUrl") == preferred_manifest_url else 1)


def resolve_meta_all(media_type, item_id, preferred_manifest_url=None, canonical_ids=None):
    ids = [str(item_id or "")]
    ids.extend(str(value or "") for value in (canonical_ids or []) if value)
    ids = list(dict.fromkeys(value for value in ids if value))
    for addon in _ordered_enabled(preferred_manifest_url):
        if not supports_type(addon, media_type):
            continue
        for candidate in ids:
            try:
                meta = resolve_meta(addon, media_type, candidate)
            except Exception as exc:
                Kodi.log("STREMIO", "Meta falhou em %s: %s" % (addon.get("name"), exc), level="warning")
                continue
            if meta:
                return meta
    return None


def series_seasons_from_meta(meta):
    groups = {}
    for video in (meta or {}).get("videos") or []:
        season = video.get("season")
        if season is None:
            continue
        groups.setdefault(int(season), []).append(video)
    for season in groups:
        groups[season].sort(key=lambda v: (v.get("episode") is None, v.get("episode") or 0, v.get("title") or ""))
    return groups


# ---- streams / subtitles --------------------------------------------------

def _header_map(value):
    if not isinstance(value, dict):
        return {}
    out = {}
    for key, val in value.items():
        if key and val not in (None, "") and key not in ("request", "response"):
            out[str(key)] = str(val)
    return out


def _stream_headers(stream, behavior):
    """Merge the header shapes used by common Stremio addons.

    Generic stream headers are the base, behaviorHints.headers can refine them,
    and proxyHeaders.request is the most specific request-header contract.
    """
    merged = {}
    merged.update(_header_map(stream.get("headers")))
    merged.update(_header_map((behavior or {}).get("headers")))
    proxy = (behavior or {}).get("proxyHeaders")
    if isinstance(proxy, dict):
        request = proxy.get("request")
        merged.update(_header_map(request if isinstance(request, dict) else proxy))
    return merged


def _human_size(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return str(value or "")
    if number <= 0:
        return ""
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(number)
    idx = 0
    while size >= 1024.0 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return ("%.2f %s" if idx >= 3 else "%.1f %s") % (size, units[idx])


def _normalize_stream(stream, addon):
    behavior = stream.get("behaviorHints") if isinstance(stream.get("behaviorHints"), dict) else {}
    filename = behavior.get("filename") or stream.get("filename") or ""
    raw_size = behavior.get("videoSize") or stream.get("size") or ""
    size = _human_size(raw_size)
    name = stream.get("name") or stream.get("title") or filename or addon.get("name")
    description = stream.get("description") or stream.get("title") or filename or ""
    quality_text = "%s %s %s %s" % (name, description, filename, size)
    return {
        "source": name,
        "addonName": addon.get("name"),
        "addonId": addon.get("id") or addon.get("manifestUrl"),
        "manifestUrl": addon.get("manifestUrl"),
        "quality": detect_quality(quality_text),
        "size": size,
        "videoSize": raw_size,
        "filename": filename,
        "url": stream.get("url") or stream.get("externalUrl"),
        "infoHash": stream.get("infoHash"),
        "fileIdx": stream.get("fileIdx"),
        "description": description,
        "behaviorHints": behavior,
        "headers": _stream_headers(stream, behavior),
        # Inline subtitles are cheap and stay attached to this particular stream.
        # The separate subtitles resource is intentionally resolved only at play time.
        "subtitles": stream.get("subtitles") or [],
        "ytId": stream.get("ytId"),
    }

def detect_quality(text):
    t = (text or "").lower()
    if "2160" in t or "4k" in t:
        return "4K"
    if "1080" in t:
        return "1080p"
    if "720" in t:
        return "720p"
    if "hdr" in t:
        return "HDR"
    if "480" in t:
        return "480p"
    return "HD"


def stream_ids(item_id, media_type, season=None, episode=None, video_id=None, canonical_ids=None):
    ids = []
    if video_id:
        ids.append(str(video_id))
    bases = [str(item_id or "")]
    for candidate in canonical_ids or []:
        candidate = str(candidate or "").strip()
        if candidate:
            bases.append(candidate)
    for iid in bases:
        if not iid:
            continue
        if media_type == "series" and season is not None and episode is not None:
            ids.append("%s:%s:%s" % (iid, season, episode))
        ids.append(iid)
    return list(dict.fromkeys(ids))


def stream_request_types(addon, media_type):
    aliases = list(_type_aliases(media_type))
    declared = [str(t).lower() for t in (addon.get("types") or [])]
    if not declared:
        return aliases
    matches = [alias for alias in aliases if alias in declared]
    return matches or [media_type]


def resolve_streams(addon, media_type, item_id, season=None, episode=None, video_id=None, canonical_ids=None, timeout=STREMIO_STREAM_TIMEOUT):
    if not addon.get("enabled", True) or not supports_type(addon, media_type):
        return []
    base, query = addon_base_url(addon.get("manifestUrl") or "")
    for rid in stream_ids(item_id, media_type, season, episode, video_id, canonical_ids):
        if not resource_supports(addon, "stream", media_type, rid):
            continue
        for request_type in stream_request_types(addon, media_type):
            url = "%s/stream/%s/%s.json" % (base, request_type, urllib.parse.quote(rid, safe=":"))
            url = _with_config_query(url, query)
            try:
                payload = http.get_json(url, timeout=timeout)
            except http.HttpError:
                continue
            streams = payload.get("streams") if isinstance(payload, dict) else []
            if isinstance(streams, list) and streams:
                return [_normalize_stream(s, addon) for s in streams if isinstance(s, dict)]
    return []


def fetch_subtitles(addon, media_type, item_id, season=None, episode=None, video_id=None, canonical_ids=None, timeout=STREMIO_STREAM_TIMEOUT):
    base, query = addon_base_url(addon.get("manifestUrl") or "")
    out = []
    for rid in stream_ids(item_id, media_type, season, episode, video_id, canonical_ids):
        if not resource_supports(addon, "subtitles", media_type, rid):
            continue
        for request_type in stream_request_types(addon, media_type):
            url = "%s/subtitles/%s/%s.json" % (base, request_type, urllib.parse.quote(rid, safe=":"))
            url = _with_config_query(url, query)
            try:
                payload = http.get_json(url, timeout=timeout)
            except http.HttpError:
                continue
            subs = payload.get("subtitles") if isinstance(payload, dict) else []
            if isinstance(subs, list):
                for sub in subs:
                    if isinstance(sub, dict) and sub.get("url"):
                        out.append({
                            "url": sub.get("url"),
                            "lang": sub.get("lang") or "",
                            "label": sub.get("label") or sub.get("name") or sub.get("lang") or "Legenda",
                        })
            if out:
                break
    return _dedupe_subtitles(out)


def _dedupe_subtitles(items):
    out, seen = [], set()
    for item in items:
        key = item.get("url")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _canonical_ids(item_id, canonical_id=None, tmdb_id=None):
    out = []
    canonical_id = str(canonical_id or "").strip()
    tmdb_id = str(tmdb_id or "").strip()
    if canonical_id:
        out.append(canonical_id)
    if tmdb_id:
        out.append(tmdb_id)
        if not tmdb_id.lower().startswith("tmdb:"):
            out.append("tmdb:" + tmdb_id)
    original = str(item_id or "")
    return list(dict.fromkeys(value for value in out if value and value != original))


def _provider_stream_key(stream):
    provider = str(stream.get("manifestUrl") or stream.get("addonId") or stream.get("addonName") or "")
    if stream.get("url"):
        identity = "url:" + str(stream.get("url"))
    elif stream.get("infoHash"):
        identity = "torrent:%s:%s" % (stream.get("infoHash"), stream.get("fileIdx"))
    elif stream.get("ytId"):
        identity = "youtube:" + str(stream.get("ytId"))
    else:
        identity = "opaque:%s" % (stream.get("source") or stream.get("description") or "")
    return provider + "|" + identity


def _run_parallel(jobs, worker, global_timeout=25, max_workers=4, area="STREMIO"):
    if not jobs:
        return []
    results = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(jobs)))
    future_map = {executor.submit(worker, job): job for job in jobs}
    try:
        try:
            for future in concurrent.futures.as_completed(future_map, timeout=global_timeout):
                job = future_map[future]
                try:
                    value = future.result()
                except Exception as exc:
                    addon = job if isinstance(job, dict) else None
                    Kodi.log(area, "%s falhou: %s" % ((addon or {}).get("name") or "Addon", exc),
                             level="warning")
                    continue
                if value:
                    results.extend(value if isinstance(value, list) else [value])
        except concurrent.futures.TimeoutError:
            Kodi.log(area, "Resolução atingiu o limite global de %ss." % global_timeout, level="warning")
            for future in future_map:
                future.cancel()
    finally:
        executor.shutdown(wait=True)
    return results


def resolve_streams_all(media_type, item_id, preferred_manifest_url=None,
                        season=None, episode=None, video_id=None, canonical_id=None,
                        tmdb_id=None):
    canonical = _canonical_ids(item_id, canonical_id, tmdb_id)
    addons = [a for a in _ordered_enabled(preferred_manifest_url) if supports_type(a, media_type)]

    def worker(addon):
        return resolve_streams(addon, media_type, item_id, season, episode, video_id,
                               canonical_ids=canonical, timeout=STREMIO_STREAM_TIMEOUT)

    streams = _run_parallel(addons, worker, global_timeout=STREMIO_RESOLVE_BUDGET, max_workers=4)
    out, seen = [], set()
    for stream in streams:
        key = _provider_stream_key(stream)
        if key in seen:
            continue
        seen.add(key)
        out.append(stream)
    return out


def resolve_subtitles_all(media_type, item_id, preferred_manifest_url=None,
                          season=None, episode=None, video_id=None, canonical_id=None,
                          tmdb_id=None, inline=None):
    """Resolve separate subtitle resources lazily, immediately before playback."""
    canonical = _canonical_ids(item_id, canonical_id, tmdb_id)
    addons = [a for a in _ordered_enabled(preferred_manifest_url) if supports_type(a, media_type)]

    def worker(addon):
        return fetch_subtitles(addon, media_type, item_id, season, episode, video_id,
                               canonical_ids=canonical, timeout=12)

    subtitles = list(inline or [])
    subtitles.extend(_run_parallel(addons, worker, global_timeout=18, max_workers=4))
    return _dedupe_subtitles(subtitles)
