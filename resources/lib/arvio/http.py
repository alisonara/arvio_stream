# -*- coding: utf-8 -*-
"""HTTP helpers built on Python stdlib (no extra runtime deps)."""

from __future__ import annotations

import gzip
import json
import urllib.error
import urllib.parse
import urllib.request

from .const import HTTP_TIMEOUT, USER_AGENT


class HttpError(Exception):
    def __init__(self, status, message):
        super(HttpError, self).__init__(message)
        self.status = status


def _request(url, method="GET", data=None, headers=None, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, data=data, method=method)
    merged = {"User-Agent": USER_AGENT, "Accept": "*/*", "Accept-Encoding": "gzip"}
    if headers:
        merged.update(headers)
    for key, value in merged.items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return resp.status, resp.headers, raw
    except urllib.error.HTTPError as exc:
        body = exc.read() or b""
        raise HttpError(exc.code, body.decode("utf-8", "replace")) from exc
    except urllib.error.URLError as exc:
        raise HttpError(0, str(exc.reason)) from exc
    except (TimeoutError, OSError) as exc:
        raise HttpError(0, str(exc)) from exc


def get_text(url, headers=None, timeout=HTTP_TIMEOUT):
    _, _, raw = _request(url, headers=headers, timeout=timeout)
    return raw.decode("utf-8", "replace")


def get_json(url, headers=None, timeout=HTTP_TIMEOUT):
    status, _, raw = _request(url, headers=headers, timeout=timeout)
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except (ValueError, TypeError) as exc:
        # Panels/addons occasionally return an HTML error page with HTTP 200.
        # Keep the transport contract uniform so callers can fail open instead
        # of crashing the Kodi route with a JSONDecodeError.
        raise HttpError(status or 0, "Resposta JSON inválida") from exc


def get_bytes(url, headers=None, timeout=HTTP_TIMEOUT):
    _, _, raw = _request(url, headers=headers, timeout=timeout)
    return raw


def post_json(url, payload, headers=None, timeout=HTTP_TIMEOUT):
    body = json.dumps(payload).encode("utf-8")
    merged = {"Content-Type": "application/json"}
    if headers:
        merged.update(headers)
    return _request(url, method="POST", data=body, headers=merged, timeout=timeout)


def _kind_from(url, content_type="", prefix=b""):
    path = urllib.parse.urlparse(url or "").path.lower()
    ctype = str(content_type or "").lower().split(";", 1)[0].strip()
    if path.endswith(".m3u8") or "mpegurl" in ctype:
        return "hls"
    if path.endswith(".mpd") or ctype in ("application/dash+xml", "video/vnd.mpeg.dash.mpd"):
        return "dash"
    body = prefix.lstrip(b"\xef\xbb\xbf \t\r\n")
    if body.startswith(b"#EXTM3U"):
        return "hls"
    if body.startswith(b"<?xml") or body.startswith(b"<MPD"):
        text = body[:256].lower()
        if b"<mpd" in text:
            return "dash"
    return "direct"


def probe_stream(url, headers=None, timeout=6):
    """Resolve redirects and classify extensionless HTTP streams conservatively.

    HEAD is attempted first. If the provider rejects HEAD or returns an
    inconclusive content type, a tiny Range GET probes at most 256 bytes.
    """
    merged = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        merged.update(headers)

    final_url = url
    content_type = ""
    head_ok = False
    try:
        req = urllib.request.Request(url, method="HEAD")
        for key, value in merged.items():
            req.add_header(key, value)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = resp.geturl() or url
            content_type = resp.headers.get("Content-Type") or ""
            head_ok = True
            kind = _kind_from(final_url, content_type)
            if kind != "direct":
                return {"url": final_url, "kind": kind, "contentType": content_type,
                        "probed": "head"}
    except urllib.error.HTTPError as exc:
        # 405/501 are normal for providers that intentionally reject HEAD.
        if exc.code not in (400, 403, 405, 501):
            return {"url": final_url, "kind": _kind_from(final_url, content_type),
                    "contentType": content_type, "probed": "head-error"}
    except (urllib.error.URLError, ValueError, OSError):
        pass

    # Do not turn known regular media files into a second request merely because
    # their MIME type was generic.
    known_path = urllib.parse.urlparse(final_url).path.lower()
    if head_ok and known_path.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts", ".m2ts")):
        return {"url": final_url, "kind": "direct", "contentType": content_type,
                "probed": "head"}

    try:
        req = urllib.request.Request(final_url, method="GET")
        get_headers = dict(merged)
        get_headers["Range"] = "bytes=0-255"
        get_headers["Accept-Encoding"] = "identity"
        for key, value in get_headers.items():
            req.add_header(key, value)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = resp.geturl() or final_url
            content_type = resp.headers.get("Content-Type") or content_type
            prefix = resp.read(256)
            return {"url": final_url, "kind": _kind_from(final_url, content_type, prefix),
                    "contentType": content_type, "probed": "range"}
    except (urllib.error.HTTPError, urllib.error.URLError, ValueError, OSError):
        return {"url": final_url, "kind": _kind_from(final_url, content_type),
                "contentType": content_type, "probed": "fallback"}


def build_url(base, path, query=None):
    quoted = urllib.parse.quote(path, safe=":/?&=%[]")
    if not quoted.startswith("/"):
        quoted = "/" + quoted
    url = base.rstrip("/") + quoted
    if query:
        url += "?" + urllib.parse.urlencode(query)
    return url
