# -*- coding: utf-8 -*-
"""Thin wrappers around xbmc API so the rest of the addon stays testable."""

try:
    import xbmc
    import xbmcaddon
    import xbmcgui
    import xbmcplugin
    import xbmcvfs
except ImportError:  # pragma: no cover - only outside Kodi
    xbmc = xbmcaddon = xbmcgui = xbmcplugin = xbmcvfs = None  # type: ignore

from .const import ADDON_ID  # noqa: E402


def _sanitize_log_message(message):
    """Redact common credential shapes before anything reaches kodi.log."""
    import re
    text = str(message)
    # Query-string credentials used by Xtream and similar APIs.
    text = re.sub(r'(?i)(username|user|uname|password|pass|pwd|token|api[_-]?key|session)=([^&\s]+)',
                  lambda m: m.group(1) + '=***', text)
    # Xtream path credentials: /live|movie|series/<user>/<pass>/...
    text = re.sub(r'(?i)/(live|movie|series)/[^/\s]+/[^/\s]+/', r'/\1/***/***/', text)
    # Long StringSession-like opaque tokens should never be logged verbatim.
    text = re.sub(r'(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{80,}(?![A-Za-z0-9_-])', '<redacted-token>', text)
    return text


class Kodi(object):
    """Cached access to the current addon instance and common helpers."""

    _addon = None

    @classmethod
    def addon(cls):
        if cls._addon is None:
            cls._addon = xbmcaddon.Addon(ADDON_ID)
        return cls._addon

    @classmethod
    def get_setting(cls, key, default=""):
        try:
            value = cls.addon().getSettingString(key)
        except (AttributeError, TypeError):
            try:
                value = cls.addon().getSetting(key)
            except Exception:
                value = ""
        if value == "" or value is None:
            return default
        return value

    @classmethod
    def set_setting(cls, key, value):
        try:
            cls.addon().setSettingString(key, str(value))
        except (AttributeError, TypeError):
            cls.addon().setSetting(key, str(value))

    @classmethod
    def get_setting_bool(cls, key, default=False):
        value = cls.get_setting(key, "true" if default else "false")
        return str(value).strip().lower() in ("1", "true", "yes", "on")

    @classmethod
    def profile_path(cls):
        try:
            return xbmcvfs.translatePath(cls.addon().getAddonInfo("profile"))
        except (AttributeError, TypeError):
            return cls.addon().getAddonInfo("profile")

    @classmethod
    def auth_key(cls):
        """Random secret for one-time pairing codes."""
        import hashlib
        secret = cls.get_setting("pair_secret", "")
        if not secret:
            secret = hashlib.sha256(os_urandom(32)).hexdigest()
            cls.set_setting("pair_secret", secret)
        return secret

    @classmethod
    def local_ip(cls):
        try:
            ip = xbmc.getIPAddress()
            if ip and ip != "0.0.0.0":
                return ip
        except Exception:
            pass
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    @classmethod
    def ok_dialog(cls, heading, message):
        dialog = xbmcgui.Dialog()
        return dialog.ok(heading, message)

    @classmethod
    def yesno(cls, heading, message):
        dialog = xbmcgui.Dialog()
        return dialog.yesno(heading, message)

    @classmethod
    def input_dialog(cls, heading, default=""):
        dialog = xbmcgui.Dialog()
        return dialog.input(heading, defaultt=default, type=xbmcgui.INPUT_ALPHANUM)

    @classmethod
    def notify(cls, message, heading=ADDON_ID, icon=None):
        if icon is None and xbmcgui is not None:
            icon = xbmcgui.NOTIFICATION_INFO
        try:
            xbmcgui.Dialog().notification(heading, message, icon, 4000, sound=False)
        except Exception:
            pass

    @classmethod
    def log(cls, area, message, level="info"):
        """Write a sanitized component log entry to Kodi's log."""
        text = "[ARVIO][%s] %s" % (str(area or "CORE").upper(), _sanitize_log_message(message))
        if xbmc is None:
            return
        levels = {
            "debug": getattr(xbmc, "LOGDEBUG", 0),
            "info": getattr(xbmc, "LOGINFO", getattr(xbmc, "LOGNOTICE", 1)),
            "warning": getattr(xbmc, "LOGWARNING", 2),
            "error": getattr(xbmc, "LOGERROR", 3),
        }
        try:
            xbmc.log(text, levels.get(str(level).lower(), levels["info"]))
        except Exception:
            pass

    @classmethod
    def json_rpc(cls, method, params=None):
        import json
        return json.loads(xbmc.executeJSONRPC(json.dumps({
            "jsonrpc": "2.0", "method": method, "params": params or {}, "id": 1
        })))


def os_urandom(nbytes):
    import os
    return os.urandom(nbytes)