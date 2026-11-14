# -*- coding: utf-8 -*-
"""Playback resolution for direct HTTP/HLS/DASH streams and Telegram proxy streams."""

from __future__ import annotations

import json
import re
import urllib.parse

try:
    import xbmc
    import xbmcgui
    import xbmcplugin
except ImportError:  # outside Kodi (tests)
    xbmc = xbmcgui = xbmcplugin = None  # type: ignore

from . import http
from .kodi_utils import Kodi


def _clean_url(url):
    url = (url or "").strip()
    if url.startswith("//"):
        url = "https:" + url
    return url




def _split_kodi_headers(url):
    """Split Kodi's ``url|Header=...`` convention into URL + header dict."""
    if "|" not in (url or ""):
        return url, {}
    base, encoded = url.split("|", 1)
    try:
        pairs = urllib.parse.parse_qsl(encoded, keep_blank_values=True)
        return base, {str(k): str(v) for k, v in pairs if k}
    except Exception:
        return base, {}


def _subtitle_urls(subtitles_raw):
    try:
        subs = json.loads(subtitles_raw or "[]")
    except Exception:
        subs = []
    urls, seen = [], set()
    for sub in subs if isinstance(subs, list) else []:
        sub_url = (sub or {}).get("url") if isinstance(sub, dict) else None
        if sub_url:
            value = str(sub_url)
            if value not in seen:
                seen.add(value)
                urls.append(value)
    return urls


def _headers_dict(headers_raw):
    if isinstance(headers_raw, dict):
        data = headers_raw
    else:
        try:
            data = json.loads(headers_raw or "{}")
        except Exception:
            data = {}
    if not isinstance(data, dict):
        return {}
    out = {}
    for key, value in data.items():
        if key and value not in (None, ""):
            out[str(key)] = str(value)
    return out


def _kodi_header_string(headers):
    if not headers:
        return ""
    return urllib.parse.urlencode(headers)


def _inputstream_available():
    if xbmc is None:
        return False
    try:
        return bool(xbmc.getCondVisibility("System.HasAddon(inputstream.adaptive)"))
    except Exception:
        return False


def _apply_headers_to_url(url, headers):
    encoded = _kodi_header_string(headers)
    if not encoded:
        return url
    if "|" in url:
        return url + "&" + encoded
    return url + "|" + encoded


def classify_stream(url, mime_hint="direct", headers=None, probe=True):
    url = _clean_url(url)
    hint = str(mime_hint or "direct").lower()
    lower = urllib.parse.urlparse(url).path.lower()
    if hint in ("hls", "application/vnd.apple.mpegurl") or lower.endswith(".m3u8"):
        return {"url": url, "kind": "hls", "contentType": ""}
    if hint in ("dash", "mpd", "application/dash+xml") or lower.endswith(".mpd"):
        return {"url": url, "kind": "dash", "contentType": ""}
    if not probe or not url.lower().startswith(("http://", "https://")):
        return {"url": url, "kind": "direct", "contentType": ""}
    if lower.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts", ".m2ts")):
        return {"url": url, "kind": "direct", "contentType": ""}
    return http.probe_stream(url, headers=headers, timeout=6)





def _video_info_dict(video_info, fallback_name):
    if isinstance(video_info, dict):
        data = dict(video_info)
    else:
        try:
            data = json.loads(video_info or "{}")
        except Exception:
            data = {}
    if not isinstance(data, dict):
        data = {}
    clean = {str(k): v for k, v in data.items() if v not in (None, "")}
    clean.setdefault("title", fallback_name)
    return clean


def _playing_label_and_info(video_info, fallback_name):
    """Build Kodi OSD metadata independently from the provider/source label."""
    info = _video_info_dict(video_info, fallback_name)
    base_title = str(info.get("tvshowtitle") or info.get("title") or fallback_name or "Stream").strip()
    season = info.get("season")
    episode = info.get("episode")
    try:
        season_num = int(season) if season not in (None, "") else None
    except (TypeError, ValueError):
        season_num = None
    try:
        episode_num = int(episode) if episode not in (None, "") else None
    except (TypeError, ValueError):
        episode_num = None

    if season_num is not None and episode_num is not None:
        label = "%s · S%02dE%02d" % (base_title, season_num, episode_num)
        info["title"] = label
        info["tvshowtitle"] = base_title
        info["season"] = season_num
        info["episode"] = episode_num
        info["mediatype"] = "episode"
        return label, info

    title = str(info.get("title") or fallback_name or "Stream").strip()
    year = str(info.get("year") or "").strip()
    match = re.search(r"(?:19|20)\d{2}", year)
    year4 = match.group(0) if match else ""
    label = title
    if year4 and not re.search(r"\(%s\)\s*$" % year4, title):
        label = "%s (%s)" % (title, year4)
    info["title"] = label
    if year4:
        info["year"] = year4
    if str(info.get("mediatype") or "").lower() not in ("episode", "tvshow"):
        info["mediatype"] = "movie"
    return label, info

def _prepare_playback(url, name, mime_hint="direct", subtitles_raw="[]",
                      headers_raw="{}", probe=True, mark_playable=False, video_info=None):
    """Build the final Kodi ListItem and URL without choosing the launch API."""
    url = _clean_url(url)
    if not url:
        return None, None, None

    base_url, embedded_headers = _split_kodi_headers(url)
    headers = dict(embedded_headers)
    headers.update(_headers_dict(headers_raw))
    resolved = classify_stream(base_url, mime_hint, headers=headers, probe=bool(probe))
    final_url = resolved.get("url") or base_url
    kind = resolved.get("kind") or "direct"

    display_label, playback_info = _playing_label_and_info(video_info, name)
    item = xbmcgui.ListItem(label=display_label)
    play_url = _apply_headers_to_url(final_url, headers)
    item.setPath(play_url)
    item.setInfo("video", playback_info)
    try:
        item.setContentLookup(False)
    except Exception:
        pass
    if mark_playable:
        item.setProperty("IsPlayable", "true")

    subs = _subtitle_urls(subtitles_raw)
    if subs:
        item.setSubtitles(subs)

    if kind in ("hls", "dash"):
        if kind == "hls":
            item.setMimeType("application/vnd.apple.mpegurl")
        else:
            item.setMimeType("application/dash+xml")
        if _inputstream_available():
            item.setProperty("inputstream", "inputstream.adaptive")
            item.setProperty("inputstreamaddon", "inputstream.adaptive")
            item.setProperty("inputstream.adaptive.manifest_type", "hls" if kind == "hls" else "mpd")
            header_string = _kodi_header_string(headers)
            if header_string:
                item.setProperty("inputstream.adaptive.stream_headers", header_string)
                item.setProperty("inputstream.adaptive.manifest_headers", header_string)
        elif kind == "hls":
            Kodi.log("PLAYER", "inputstream.adaptive ausente; usando player HLS nativo.", level="warning")
        else:
            Kodi.log("PLAYER", "DASH detectado, mas inputstream.adaptive não está instalado.", level="warning")

    return item, play_url, kind

def play_http(url, name, mime_hint="direct", subtitles_raw="[]", handle=-1,
              headers_raw="{}", probe=True, video_info=None):
    """Classic Kodi plugin resolver path retained for compatibility/tests."""
    item, _play_url, kind = _prepare_playback(
        url, name, mime_hint, subtitles_raw, headers_raw, probe, mark_playable=True, video_info=video_info)
    if item is None:
        Kodi.notify("URL de stream vazia.")
        if xbmcplugin is not None:
            xbmcplugin.setResolvedUrl(handle, succeeded=False, listitem=xbmcgui.ListItem())
        return False
    Kodi.log("PLAYER", "Resolvendo playback: %s (%s)" % (name, kind.upper()))
    xbmcplugin.setResolvedUrl(handle, succeeded=True, listitem=item)
    return True


def play_http_direct(url, name, mime_hint="direct", subtitles_raw="[]",
                     headers_raw="{}", probe=True, video_info=None):
    """Launch a stream with xbmc.Player(), not Kodi's IsPlayable resolver.

    The browser row remains a normal plugin action and the actual playing item is
    the resolved HTTP/HLS/DASH URL. Kodi can still refresh a plugin parent
    directory after STOP; router-level short-lived source snapshots make that
    refresh network-free.
    """
    item, play_url, kind = _prepare_playback(
        url, name, mime_hint, subtitles_raw, headers_raw, probe, mark_playable=False, video_info=video_info)
    if item is None:
        Kodi.notify("URL de stream vazia.")
        return False
    if xbmc is None:
        return False
    Kodi.log("PLAYER", "Iniciando via xbmc.Player: %s (%s)" % (name, kind.upper()))
    xbmc.Player().play(play_url, item)
    return True


def play_item(item, handle=-1):
    url = _clean_url(item.get("url") or "")
    if not url:
        return False
    return play_http(url, item.get("name") or "Stream", item.get("mime") or "direct",
                     json.dumps(item.get("subtitles") or []), handle,
                     json.dumps(item.get("headers") or {}))
