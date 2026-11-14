# -*- coding: utf-8 -*-
"""Kodi navigation router for Arvio Stream."""

from __future__ import annotations

import base64
import asyncio
import concurrent.futures
import hashlib
import json
import re
import sys
import threading
import time
import urllib.parse
import unicodedata

try:
    import xbmc
    import xbmcgui
    import xbmcplugin
except ImportError:  # outside Kodi (tests)
    xbmc = xbmcgui = xbmcplugin = None  # type: ignore

from . import (const, metadata_aliases, pairing, player, source_cache, stremio, storage, telegram_client,
               xtream)
from .kodi_utils import Kodi

PLUGIN = "plugin://%s" % const.ADDON_ID
TELEGRAM_QR_TIMEOUT = const.TELEGRAM_SEARCH_TIMEOUT_S * 12 + 15
CINEMETA = stremio.CINEMETA_MANIFEST
_handle = -1


def _h():
    return _handle


# ---- url helpers ----------------------------------------------------------

def enc(value):
    if value is None:
        return ""
    raw = value.encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def dec(value):
    try:
        return base64.urlsafe_b64decode(value.encode("ascii")).decode("utf-8")
    except Exception:
        return value or ""


def url(mode, **params):
    params = dict(params)
    params["mode"] = mode
    return PLUGIN + "/?" + urllib.parse.urlencode(params)


# ---- listitem helpers -----------------------------------------------------

def li(label, art=None, info=None, properties=None, is_playable=None):
    item = xbmcgui.ListItem(label=label)
    art = art or {}
    thumb = art.get("thumb") or art.get("poster") or ""
    if thumb:
        item.setArt({"thumb": thumb, "icon": thumb})
    fanart = art.get("fanart") or ""
    if fanart:
        item.setArt({"fanart": fanart})
    if info:
        item.setInfo("video", info)
    # Do not stamp IsPlayable=False on ordinary items. More importantly,
    # Arvio's stream rows intentionally omit IsPlayable entirely and launch
    # playback from the route through xbmc.Player(). This prevents Kodi from
    # treating the plugin:// row itself as the playing file and refreshing the
    # expensive Cinemeta directory after STOP.
    if is_playable is True:
        item.setProperty("IsPlayable", "true")
    for key, value in (properties or {}).items():
        if value is not None:
            item.setProperty(key, str(value))
    return item


def folder(target_url, label, art=None, info=None, properties=None):
    item = li(label, art=art, info=info, properties=properties)
    xbmcplugin.addDirectoryItem(_h(), target_url, item, isFolder=True)
    return item


def video_action(target_url, label, art=None, info=None, properties=None):
    """Add a video action without Kodi's IsPlayable plugin-resolver contract.

    The target route starts xbmc.Player() directly. Keeping the browser row as
    an action rather than the active plugin media URL prevents Kodi from
    re-running the parent Cinemeta search automatically after STOP.
    """
    item = li(label, art=art, info=info, properties=properties)
    xbmcplugin.addDirectoryItem(_h(), target_url, item, isFolder=False)
    return item


def playable(target_url, label, art=None, info=None, properties=None):
    # Backward-compatible internal alias. It deliberately does *not* set
    # IsPlayable; all playback is launched by xbmc.Player() in the route.
    return video_action(target_url, label, art=art, info=info, properties=properties)


def end(succeeded=True, content=None, cache=False):
    # Dynamic plugin listings are deliberately not cached to disk. Kodi can
    # otherwise show stale Telegram/account/on-off state after a new invocation.
    if content:
        try:
            xbmcplugin.setContent(_h(), content)
        except Exception:
            pass
    xbmcplugin.endOfDirectory(_h(), succeeded=succeeded, cacheToDisc=cache)

def _parse_argv():
    raw = sys.argv[2]
    query = raw.split("?", 1)[1] if "?" in raw else ""
    return {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}


# ---- shared art/info helpers ----------------------------------------------

def _art_for(meta):
    return {"thumb": meta.get("poster") or meta.get("logo") or "",
            "fanart": meta.get("backdrop") or meta.get("background") or ""}


def _info_for_meta(meta):
    info = {"title": meta.get("title"), "plot": meta.get("description") or ""}
    if meta.get("year"):
        info["year"] = str(meta["year"])
    if meta.get("releaseInfo"):
        info["premiered"] = str(meta["releaseInfo"])
    if meta.get("rating"):
        try:
            rating = float(meta["rating"])
            info["rating"] = rating / 10.0 if rating > 10.0 else rating
        except (TypeError, ValueError):
            pass
    return info


def _display_year(value):
    match = re.search(r"(?:19|20)\d{2}", str(value or ""))
    return match.group(0) if match else ""


def _meta_label(meta):
    title = str((meta or {}).get("title") or "").strip()
    year = _display_year((meta or {}).get("year") or (meta or {}).get("releaseInfo"))
    if year and title and not re.search(r"\(%s\)\s*$" % year, title):
        return "%s (%s)" % (title, year)
    return title


def _playback_info(media_type, title, year=None, season=None, episode=None):
    """Minimal metadata payload for the *playing* ListItem, not the source row."""
    info = {"title": str(title or "Stream")}
    if media_type == "series" and season is not None and episode is not None:
        info.update({"tvshowtitle": str(title or "Série"), "season": int(season),
                     "episode": int(episode), "mediatype": "episode"})
    else:
        year4 = _display_year(year)
        if year4:
            info["year"] = year4
        info["mediatype"] = "movie"
    return info


def _playback_info_param(media_type, title, year=None, season=None, episode=None):
    return enc(json.dumps(_playback_info(media_type, title, year, season, episode),
                          ensure_ascii=False, separators=(",", ":")))


# ---- QR pairing helper ----------------------------------------------------

def _run_pair_flow(title, hint_tail, handler):
    """Start local pairing, show QR and close it automatically on receipt."""
    server, port = _start_pairing_server()
    if server is None:
        return False
    code = server.create_code()
    ip = pairing.best_lan_ip()
    target = pairing.short_url(ip, port, code)
    result = {"payload": None}
    done = threading.Event()
    cancel = threading.Event()

    def poller():
        try:
            result["payload"] = server.wait_payload(code, timeout_s=240, cancel_event=cancel)
        finally:
            done.set()

    poll_thread = threading.Thread(target=poller, name="ArvioPairingWait", daemon=False)
    poll_thread.start()
    from .qr import show_qr_dialog
    show_qr_dialog(
        title,
        "Abra esta página no celular (na mesma rede Wi-Fi)\ne siga as instruções:\n\n%s\n\n%s" %
        (target, hint_tail),
        target, completion_event=done, timeout_s=240)

    cancel.set()
    try:
        server.stop()
    except Exception:
        pass
    poll_thread.join(timeout=3.0)
    payload = result.get("payload")
    if not payload:
        Kodi.notify("Pareamento cancelado (sem resposta do celular).")
        return False
    try:
        handler(payload)
        return True
    except Exception as exc:
        Kodi.ok_dialog(title, "Erro ao aplicar o que veio do celular: %s" % exc)
        return False

def _start_pairing_server():
    """Try a handful of ports before giving up on the pairing server."""
    last_error = None
    for candidate in (const.PAIR_PORT, const.PAIR_PORT + 1, const.PAIR_PORT + 2,
                      const.PAIR_PORT + 3, 0):
        server = pairing.PairingServer(port=candidate)
        try:
            port = server.start()
            return server, port
        except Exception as exc:
            last_error = exc
            try:
                server.stop()
            except Exception:
                pass
    Kodi.ok_dialog("QR pareamento",
                   "Não foi possível iniciar o servidor local de pareamento: %s"
                   % last_error)
    return None, 0


def _source_topology_signature():
    """Stable fingerprint of the currently enabled stream providers.

    The fingerprint intentionally contains no plaintext credentials in the
    snapshot file: the complete descriptor is SHA-256 hashed before use.
    """
    # Resolver generation invalidates snapshots created by older matching
    # algorithms even when the configured provider list itself did not change.
    parts = ["resolver:1.5.2"]
    try:
        parts.extend("st:%s" % (addon.get("manifestUrl") or "")
                     for addon in stremio.enabled_addons() or [])
    except Exception:
        parts.append("st:error")
    try:
        parts.extend("xc:%s" % (account.get("id") or "")
                     for account in xtream.enabled_accounts() or [])
    except Exception:
        parts.append("xc:error")
    try:
        saved = storage.load_telegram_session() or ""
        if saved:
            parts.append("tg:%s" % hashlib.sha256(saved.encode("utf-8")).hexdigest()[:20])
    except Exception:
        parts.append("tg:error")
    raw = "\n".join(sorted(set(parts)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _invalidate_source_snapshots(reason):
    source_cache.clear()
    Kodi.log("CACHE", "Snapshots de fontes invalidados: %s" % reason, level="info")


def _pair_stremio(payload):
    if payload.get("kind") != "stremio:addon":
        raise ValueError("Tipo de payload não suportado: %s" % payload.get("kind"))
    addon = stremio.install_addon(payload.get("url") or "")
    _invalidate_source_snapshots("addon Stremio adicionado via pareamento")
    Kodi.notify("Addon instalado: %s" % addon.get("name"))


def _pair_xtream(payload):
    if payload.get("kind") != "xtream":
        raise ValueError("Tipo de payload não suportado: %s" % payload.get("kind"))
    account = xtream.add_account(payload.get("name") or "", payload.get("line") or "")
    _invalidate_source_snapshots("conta Xtream adicionada via pareamento")
    Kodi.notify("Conta Xtream adicionada: %s" % account.get("name"))


def _pair_telegram(payload):
    if payload.get("kind") != "telegram:session":
        raise ValueError("Tipo de payload não suportado: %s" % payload.get("kind"))
    session = (payload.get("session") or "").strip()
    if not session:
        raise ValueError("Sessão vazia.")
    storage.save_telegram_session(session)
    if telegram_client.restore_session():
        _invalidate_source_snapshots("sessão Telegram adicionada via pareamento")
        Kodi.notify("Telegram conectado!")
    else:
        raise ValueError("A sessão enviada não pôde ser validada.")


# ---- main menu ------------------------------------------------------------

def cmd_main():
    folder(url("cinemeta_menu"), "Cinemeta (Catálogo)",
           info={"title": "Filmes e séries organizados por categorias, com fontes agregadas"})
    folder(url("stremio_menu"), "Stremio Addons",
           info={"title": "Instale addons, navegue catálogos e assista"})
    folder(url("xtream_menu"), "IPTV (Xtream)",
           info={"title": "TV ao vivo, filmes e séries das suas contas Xtream"})
    label = "Telegram"
    if telegram_client.has_saved_session():
        label += " (configurado)"
    folder(url("tg_menu"), label,
           info={"title": "Conecte sua conta Telegram e assista vídeos dos chats"})
    folder(url("settings_menu"), "Configurações",
           info={"title": "Gerenciar addons, contas e sessão Telegram"})
    end(content="files")

# ---- Cinemeta -------------------------------------------------------------

def _normalize_title(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _cinemeta_enabled():
    try:
        addon = stremio.ensure_cinemeta_installed()
    except Exception as exc:
        Kodi.ok_dialog("Cinemeta", "Não foi possível instalar o Cinemeta:\n%s" % exc)
        return None
    if not addon.get("enabled", True):
        Kodi.notify("Cinemeta está desativado. Ative em Configurações.")
        return None
    return addon


def cmd_cinemeta_menu():
    if _cinemeta_enabled() is None:
        end(succeeded=False)
        return
    folder(url("cinemeta_categories", type="movie"), "Filmes",
           info={"title": "Catálogo de filmes do Cinemeta"})
    folder(url("cinemeta_categories", type="series"), "Séries",
           info={"title": "Catálogo de séries do Cinemeta"})
    folder(url("cinemeta_search_prompt"), "Pesquisar",
           info={"title": "Buscar filme ou série por título"})
    end(content="files")


def cmd_cinemeta_categories():
    params = _parse_argv()
    media_type = params.get("type", "movie")
    if _cinemeta_enabled() is None:
        end(succeeded=False)
        return
    cats = stremio.cinemeta_categories(media_type)
    if not cats:
        Kodi.notify("Cinemeta não oferece categorias para '%s'." % media_type)
        end(succeeded=False)
        return
    for cat in cats:
        label = cat.get("label") or cat.get("id") or media_type
        folder(url("cinemeta_catalog", type=media_type, cat=enc(cat.get("id") or ""),
                   genre_required="1" if cat.get("genre_required") else "0",
                   preset_genre=enc(cat.get("preset_genre") or "")), label,
               info={"title": label})
    end(content="files")


def _cinemeta_catalog_genres(addon, media_type, cat_id):
    for catalog in (addon or {}).get("catalogs") or []:
        if catalog.get("type") == media_type and str(catalog.get("id")) == str(cat_id):
            values = [str(value) for value in (catalog.get("genres") or []) if value not in (None, "")]
            if values:
                return values
            for extra in catalog.get("extra") or []:
                if isinstance(extra, dict) and extra.get("name") == "genre":
                    return [str(value) for value in (extra.get("options") or []) if value not in (None, "")]
    return []


def cmd_cinemeta_catalog():
    params = _parse_argv()
    media_type = params.get("type", "movie")
    cat_id = dec(params.get("cat", ""))
    genre_required = params.get("genre_required") == "1"
    preset_genre = dec(params.get("preset_genre", "")) if params.get("preset_genre") else ""
    carried_genre = dec(params.get("genre", "")) if params.get("genre") else ""
    genre = carried_genre or preset_genre or ""
    try:
        skip = max(0, int(params.get("skip", 0)))
    except (TypeError, ValueError):
        skip = 0
    addon = _cinemeta_enabled()
    if addon is None:
        end(succeeded=False)
        return
    catalog = next((c for c in (addon.get("catalogs") or [])
                    if c.get("type") == media_type and str(c.get("id")) == cat_id), None)
    if genre_required and not genre:
        genres = _cinemeta_catalog_genres(addon, media_type, cat_id)
        if genres and xbmcgui is not None:
            idx = xbmcgui.Dialog().select("Ano / gênero", genres)
            if idx == -1:
                end(succeeded=False)
                return
            genre = genres[idx]
        else:
            genre = Kodi.input_dialog("Ano / gênero", "")
            if not genre:
                end(succeeded=False)
                return
    try:
        items = stremio.cinemeta_catalog(
            media_type, cat_id, genre=genre or None,
            skip=skip if catalog and stremio.catalog_supports_skip(catalog) else None)
    except Exception as exc:
        Kodi.ok_dialog("Erro ao carregar catálogo", str(exc))
        end(succeeded=False)
        return
    if not items:
        Kodi.notify("Catálogo vazio.")
        end(succeeded=False)
        return
    for meta in items:
        target = url("cinemeta_item", type=meta.get("type") or media_type,
                     id=enc(meta.get("id") or ""), title=enc(meta.get("title") or ""),
                     canonical=enc(meta.get("imdbId") or ""), tmdb=enc(meta.get("tmdbId") or ""),
                     year=enc(meta.get("year") or meta.get("releaseInfo") or ""),
                     alts=enc(json.dumps(meta.get("alternateTitles") or [], ensure_ascii=False)))
        folder(target, _meta_label(meta), art=_art_for(meta), info=_info_for_meta(meta))
    if catalog and stremio.catalog_supports_skip(catalog):
        next_skip = skip + len(items)
        folder(url("cinemeta_catalog", type=media_type, cat=enc(cat_id), skip=next_skip,
                   genre_required="1" if genre_required else "0", genre=enc(genre or ""),
                   preset_genre=enc(preset_genre or "")), "Próxima página ›",
               info={"title": "Carregar mais itens"})
    end(content="movies" if media_type == "movie" else "tvshows")


def cmd_cinemeta_search_prompt():
    query = Kodi.input_dialog("Pesquisar filme ou série", "")
    if not query:
        end(succeeded=False)
        return
    _cinemeta_search_results(query)


def _cinemeta_search_results(query):
    if _cinemeta_enabled() is None:
        end(succeeded=False)
        return
    results = []
    for media_type in ("movie", "series"):
        results.extend(stremio.cinemeta_search(media_type, query))
    if not results:
        Kodi.notify("Nenhum resultado para: %s" % query)
        end(succeeded=False)
        return
    seen = set()
    for meta in results:
        key = (meta.get("type"), meta.get("imdbId") or meta.get("id"))
        if key in seen:
            continue
        seen.add(key)
        target = url("cinemeta_item", type=meta.get("type") or "movie",
                     id=enc(meta.get("id") or ""), title=enc(meta.get("title") or ""),
                     canonical=enc(meta.get("imdbId") or ""), tmdb=enc(meta.get("tmdbId") or ""),
                     year=enc(meta.get("year") or meta.get("releaseInfo") or ""),
                     alts=enc(json.dumps(meta.get("alternateTitles") or [], ensure_ascii=False)))
        folder(target, _meta_label(meta), art=_art_for(meta), info=_info_for_meta(meta))
    end(content="movies")


def _safe_stremio_aggregate(media_type, item_id, season, episode, video_id, canonical_id, tmdb_id):
    started = time.monotonic()
    try:
        result = stremio.resolve_streams_all(
            media_type, item_id, season=season, episode=episode, video_id=video_id,
            canonical_id=canonical_id, tmdb_id=tmdb_id)
        Kodi.log("STREMIO", "Cinemeta: %d fonte(s) em %.1fs" %
                 (len(result or []), time.monotonic() - started), level="info")
        return result
    except Exception as exc:
        Kodi.log("STREMIO", "Agregação Cinemeta falhou: %s" % exc, level="warning")
        return []


def _decode_title_list(value):
    if not value:
        return []
    try:
        parsed = json.loads(dec(value))
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item or "").strip()][:12]


def _meta_alternate_titles(meta):
    values = (meta or {}).get("alternateTitles") or []
    return [str(item).strip() for item in values if str(item or "").strip()][:12]


def _safe_xtream_aggregate(media_type, title, year, season, episode, canonical_id, tmdb_id, alternate_titles):
    started = time.monotonic()

    def search(aliases, fast_only=False):
        if media_type == "movie":
            return xtream.search_vod_by_title(
                title, year=year, limit_per_account=8, imdb_id=canonical_id,
                tmdb_id=tmdb_id, alternate_titles=aliases, fast_only=fast_only,
                max_accounts=4, request_timeout=8.0)
        return xtream.search_series_episode_by_title(
            title, season, episode, year=year, limit_per_account=5,
            imdb_id=canonical_id, tmdb_id=tmdb_id, alternate_titles=aliases,
            fast_only=fast_only, max_accounts=4, request_timeout=8.0)

    aliases = list(alternate_titles or [])
    try:
        items = search(aliases, fast_only=True)
        if not items:
            try:
                localized = metadata_aliases.localized_titles_for_identity(
                    title, year=year, imdb_id=canonical_id)
            except Exception:
                localized = []
            if localized:
                aliases = list(dict.fromkeys(aliases + localized))
            items = search(aliases, fast_only=False)
    except Exception as exc:
        Kodi.log("XTREAM", "Agregação Cinemeta falhou: %s" % exc, level="warning")
        return []
    out = []
    for item in items:
        label_name = item.get("seriesName") or item.get("name") or item.get("title") or title
        out.append({
            "sourceKind": "xtream", "source": "Xtream (%s)" % (item.get("accountName") or "Conta"),
            "addonName": "Xtream (%s)" % (item.get("accountName") or "Conta"),
            "accountId": item.get("accountId") or "", "itemId": item.get("id") or "",
            "matchedTitle": label_name,
            "quality": stremio.detect_quality(label_name), "size": "", "url": item.get("url") or "",
            "description": item.get("plot") or "", "headers": item.get("headers") or {},
            "subtitles": [], "behaviorHints": {},
            "_xtream_score": item.get("_xtream_score") or 0,
            "_xtream_year": item.get("year") or year or "",
        })
    result = [item for item in out if item.get("url")]
    if result:
        result.sort(key=lambda stream: stream.get("_xtream_score") or 0, reverse=True)
        result = result[:1]
    Kodi.log("XTREAM", "Cinemeta: %d fonte(s) em %.1fs para '%s'" %
             (len(result), time.monotonic() - started, title), level="info")
    return result

def _tg_episode_matches(msg, season, episode):
    if season is None or episode is None:
        return True
    text = "%s %s" % (msg.get("fileName") or "", msg.get("caption") or "")
    normalized = _normalize_title(text).replace(" ", "")
    patterns = (
        "s%02de%02d" % (int(season), int(episode)),
        "s%de%d" % (int(season), int(episode)),
        "%dx%02d" % (int(season), int(episode)),
        "%dx%d" % (int(season), int(episode)),
    )
    return any(pattern in normalized for pattern in patterns)


_TG_STOPWORDS = {
    "a", "o", "as", "os", "de", "da", "do", "das", "dos", "e", "em", "um", "uma",
    "the", "a", "an", "of", "and", "in", "on", "to", "for", "with",
}


def _unique_title_candidates(title, alternate_titles=None):
    out, seen = [], set()
    for value in [title] + list(alternate_titles or []):
        value = str(value or "").strip()
        norm = _normalize_title(value)
        if value and norm and norm not in seen:
            seen.add(norm)
            out.append(value)
    return out[:16]


def _title_tokens(value):
    return [token for token in _normalize_title(value).split()
            if len(token) >= 3 and token not in _TG_STOPWORDS]


def _telegram_queries(titles, year=None, season=None, episode=None):
    values, seen = [], set()
    for title in titles:
        options = []
        if season is not None and episode is not None:
            options.append("%s S%02dE%02d" % (title, int(season), int(episode)))
        elif year:
            options.append("%s %s" % (title, str(year)[:4]))
        options.append(title)
        tokens = _title_tokens(title)
        # Token fallback from the older ARVIO Kodi implementation, bounded and
        # filtered later so a translated/punctuated filename is not missed just
        # because the exact Cinemeta phrase differs.
        if len(tokens) >= 2:
            options.append(" ".join(tokens[:3]))
        for query in options:
            key = _normalize_title(query)
            if key and key not in seen:
                seen.add(key)
                values.append(query)
            if len(values) >= 8:
                return values
    return values


def _telegram_match_score(msg, titles, year=None, season=None, episode=None):
    if season is not None and episode is not None and not _tg_episode_matches(msg, season, episode):
        return 0
    hay = _normalize_title("%s %s" % (msg.get("fileName") or "", msg.get("caption") or ""))
    if not hay:
        return 0
    best = 0
    for title in titles:
        score = xtream._title_match_score(title, hay, requested_year=year)
        best = max(best, score)
    return max(0, best)


def _safe_telegram_aggregate(title, season, episode, alternate_titles=None, year=None, canonical_id=None):
    if not telegram_client.dependency_ok() or not telegram_client.has_saved_session():
        return []
    titles = _unique_title_candidates(title, alternate_titles)
    queries = _telegram_queries(titles, year=year, season=season, episode=episode)
    if not queries:
        return []
    started = time.monotonic()
    try:
        messages = telegram_client.search_video_messages_many(queries, limit_per_query=20, max_total=60)
    except (telegram_client.TelegramOperationCancelled, asyncio.CancelledError, concurrent.futures.CancelledError) as exc:
        Kodi.log("TELEGRAM", "Busca Cinemeta cancelada/expirada sem erro fatal: %s" % exc, level="debug")
        return []
    except Exception as exc:
        Kodi.log("TELEGRAM", "Busca Cinemeta falhou: %s" % exc, level="warning")
        return []
    ranked = []
    for msg in messages:
        score = _telegram_match_score(msg, titles, year=year, season=season, episode=episode)
        if score:
            ranked.append((score, int(msg.get("size") or 0), msg))
    if not ranked:
        # Cinemeta normally gives us an IMDb id (tt...).  Prefer aliases tied to
        # that identity so an English Cinemeta title can resolve a Portuguese
        # Telegram filename without guessing by text alone.  This is strictly a
        # Telegram fallback; Xtream resolution is not changed here.
        try:
            localized = metadata_aliases.localized_titles_for_identity(
                title, year=year, imdb_id=canonical_id)
        except Exception:
            localized = []
        if not localized:
            localized = metadata_aliases.localized_titles(title, year=year)
        localized = [value for value in localized if _normalize_title(value) not in
                     {_normalize_title(item) for item in titles}]
        if localized:
            Kodi.log("TELEGRAM", "Aliases localizados por identidade: %d para '%s'" %
                     (len(localized), title), level="info")
        if localized:
            titles = _unique_title_candidates(title, list(alternate_titles or []) + localized)
            extra_queries = _telegram_queries(localized, year=year, season=season, episode=episode)
            try:
                messages = telegram_client.search_video_messages_many(
                    extra_queries, limit_per_query=20, max_total=60)
            except (telegram_client.TelegramOperationCancelled, asyncio.CancelledError, concurrent.futures.CancelledError) as exc:
                Kodi.log("TELEGRAM", "Busca localizada Cinemeta cancelada/expirada: %s" % exc, level="debug")
                messages = []
            except Exception as exc:
                Kodi.log("TELEGRAM", "Busca localizada Cinemeta falhou: %s" % exc, level="warning")
                messages = []
            for msg in messages:
                score = _telegram_match_score(msg, titles, year=year, season=season, episode=episode)
                if score:
                    ranked.append((score, int(msg.get("size") or 0), msg))
    ranked.sort(key=lambda row: (-row[0], -row[1]))
    out, seen = [], set()
    for _, _, msg in ranked:
        key = msg.get("key")
        if not key or key in seen:
            continue
        seen.add(key)
        size = int(msg.get("size") or 0)
        out.append({
            "sourceKind": "telegram", "source": "Telegram", "addonName": "Telegram",
            "quality": stremio.detect_quality(msg.get("fileName") or ""),
            "size": ("%.2f GB" % (size / 1e9)) if size >= 1e9 else ("%.0f MB" % (size / 1e6)),
            "description": msg.get("caption") or "", "headers": {}, "subtitles": [],
            "_tg_stream": True, "_tg_video": msg,
        })
        if len(out) >= 20:
            break
    Kodi.log("TELEGRAM", "Cinemeta: %d fonte(s) em %.1fs para '%s'" %
             (len(out), time.monotonic() - started, title), level="info")
    return out

def _aggregate_key(stream):
    if stream.get("_tg_stream"):
        video = stream.get("_tg_video") or {}
        return "telegram|%s|%s" % (video.get("peerId"), video.get("messageId"))
    provider = (stream.get("manifestUrl") or stream.get("accountId") or
                stream.get("addonId") or stream.get("addonName") or stream.get("source") or "")
    if stream.get("url"):
        identity = "url:" + str(stream.get("url"))
    elif stream.get("infoHash"):
        identity = "torrent:%s:%s" % (stream.get("infoHash"), stream.get("fileIdx"))
    elif stream.get("ytId"):
        identity = "youtube:" + str(stream.get("ytId"))
    else:
        identity = "opaque:" + str(stream.get("source") or stream.get("description") or "")
    return "%s|%s|%s" % (stream.get("sourceKind") or "stremio", provider, identity)


def _aggregate_streams(media_type, item_id, title, year=None, season=None, episode=None,
                       video_id=None, canonical_id=None, tmdb_id=None, alternate_titles=None):
    jobs = []
    # Provider scheduling preserves the operational contract from 1.4.2.
    # Diagnostics are best-effort only and must never decide whether a source runs.
    try:
        stremio_enabled = stremio.enabled_addons() or []
    except Exception:
        stremio_enabled = []
    stremio_stream_addons = [addon for addon in stremio_enabled
                             if stremio.has_resource(addon, "stream") and
                             stremio.supports_type(addon, media_type)]
    try:
        xtream_accounts = xtream.enabled_accounts() or []
    except Exception:
        xtream_accounts = []
    try:
        telegram_ready = bool(telegram_client.dependency_ok() and telegram_client.has_saved_session())
    except Exception:
        telegram_ready = False

    # Extra counts improve real-device diagnosis but are deliberately fail-open.
    try:
        stremio_total = len(stremio.list_installed() or [])
    except Exception:
        stremio_total = len(stremio_enabled)
    try:
        xtream_total = len(xtream.accounts() or [])
    except Exception:
        xtream_total = len(xtream_accounts)
    Kodi.log("ROUTER",
             "Cinemeta provedores: Stremio stream=%d/%d instalado(s), Xtream=%d/%d conta(s), Telegram=%s" %
             (len(stremio_stream_addons), stremio_total, len(xtream_accounts), xtream_total,
              "sim" if telegram_ready else "não"), level="info")
    if stremio_stream_addons:
        jobs.append(("Stremio", _safe_stremio_aggregate,
                     (media_type, item_id, season, episode, video_id, canonical_id, tmdb_id)))
    if xtream_accounts:
        jobs.append(("Xtream", _safe_xtream_aggregate,
                     (media_type, title, year, season, episode, canonical_id, tmdb_id,
                      list(alternate_titles or []))))
    if telegram_ready:
        jobs.append(("Telegram", _safe_telegram_aggregate,
                     (title, season, episode, list(alternate_titles or []), year, canonical_id)))
    if not jobs:
        return []

    streams, errors = [], []
    progress = None
    if xbmcgui is not None:
        try:
            progress = xbmcgui.DialogProgress()
            progress.create("Arvio Stream", "Buscando fontes...")
        except Exception:
            progress = None

    # Stremio generally finishes first; Xtream may need category fallbacks and
    # Telegram may need a server-side search. Give the sources enough room to
    # finish instead of discarding everything that has not completed at 25 s.
    # Each provider already has its own HTTP/Telegram timeouts, so 60 s is only
    # an outer safety ceiling, not the expected wait time.
    aggregate_timeout = 60.0
    started = time.monotonic()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(jobs)))
    futures = {executor.submit(func, *args): label for label, func, args in jobs}
    pending = set(futures)
    handled = set()
    completed = 0
    try:
        while pending:
            elapsed = time.monotonic() - started
            remaining = aggregate_timeout - elapsed
            if remaining <= 0:
                errors.append("limite global de %.0fs" % aggregate_timeout)
                break
            done, pending = concurrent.futures.wait(
                pending, timeout=min(0.35, remaining),
                return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                handled.add(future)
                completed += 1
                label = futures[future]
                try:
                    result = future.result() or []
                    streams.extend(result)
                    Kodi.log("ROUTER", "Cinemeta -> %s: %d fonte(s)" % (label, len(result)), level="info")
                except (asyncio.CancelledError, concurrent.futures.CancelledError,
                        telegram_client.TelegramOperationCancelled) as exc:
                    errors.append("%s: cancelado/expirado (%s)" % (label, exc))
                except Exception as exc:
                    errors.append("%s: %s" % (label, exc))
            if progress is not None:
                try:
                    percent = min(99, int(completed * 100.0 / len(jobs)))
                    progress.update(percent, "Buscando fontes... %d/%d" % (completed, len(jobs)))
                    if progress.iscanceled():
                        errors.append("cancelado pelo usuário")
                        break
                except Exception:
                    pass
        for future in pending:
            future.cancel()
    finally:
        # Never leave provider worker threads alive when a Kodi pluginsource
        # invocation returns. Each provider has its own bounded network timeout.
        executor.shutdown(wait=True)
        # A future that was already running cannot be cancelled. If it finishes
        # while shutdown() is joining the worker, keep its result instead of
        # silently dropping a late Xtream/Telegram provider.
        for future, label in futures.items():
            if future in handled or future.cancelled() or not future.done():
                continue
            handled.add(future)
            try:
                result = future.result() or []
                streams.extend(result)
                Kodi.log("ROUTER", "Cinemeta -> %s (conclusão tardia): %d fonte(s)" %
                         (label, len(result)), level="info")
            except (asyncio.CancelledError, concurrent.futures.CancelledError,
                    telegram_client.TelegramOperationCancelled) as exc:
                errors.append("%s: cancelado/expirado (%s)" % (label, exc))
            except Exception as exc:
                errors.append("%s: %s" % (label, exc))
        if progress is not None:
            try:
                progress.close()
            except Exception:
                pass
    if errors:
        Kodi.log("ROUTER", "Agregação Cinemeta: %s" % "; ".join(errors), level="warning")
    out, seen = [], set()
    for stream in streams:
        key = _aggregate_key(stream)
        if key in seen:
            continue
        seen.add(key)
        out.append(stream)
    Kodi.log("ROUTER", "Cinemeta total: %d fonte(s) para '%s' em %.1fs" %
             (len(out), title, time.monotonic() - started), level="info")
    return out

def _cinemeta_meta(media_type, item_id):
    addon = stremio.find_addon(CINEMETA)
    if addon and addon.get("enabled", True):
        try:
            return stremio.resolve_meta(addon, media_type, item_id)
        except Exception as exc:
            Kodi.log("STREMIO", "Meta Cinemeta falhou: %s" % exc, level="warning")
    return None


def cmd_cinemeta_item():
    params = _parse_argv()
    media_type = params.get("type", "movie")
    item_id = dec(params.get("id", ""))
    title = dec(params.get("title", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    year = dec(params.get("year", ""))
    alternate_titles = _decode_title_list(params.get("alts", ""))
    catalog_title = title
    meta = _cinemeta_meta(media_type, item_id)
    if meta:
        meta_title = meta.get("title") or ""
        if catalog_title and meta_title and _normalize_title(catalog_title) != _normalize_title(meta_title):
            alternate_titles = list(dict.fromkeys([catalog_title] + alternate_titles))
        title = meta_title or catalog_title
        canonical_id = meta.get("imdbId") or canonical_id
        tmdb_id = meta.get("tmdbId") or tmdb_id
        year = meta.get("year") or meta.get("releaseInfo") or year
        alternate_titles = list(dict.fromkeys(alternate_titles + _meta_alternate_titles(meta)))
    if media_type != "series":
        _show_aggregated_streams(media_type, item_id, title, year=year,
                                 canonical_id=canonical_id, tmdb_id=tmdb_id,
                                 alternate_titles=alternate_titles)
        return
    seasons = stremio.series_seasons_from_meta(meta)
    if seasons:
        for season_num in sorted(seasons):
            # Specials (season 0) are valid in Stremio metadata; keep them visible.
            label = "Especiais" if season_num == 0 else "Temporada %d" % season_num
            folder(url("cinemeta_series_eps", id=enc(item_id), title=enc(title), season=season_num,
                       canonical=enc(canonical_id), tmdb=enc(tmdb_id), year=enc(year or ""),
                       alts=enc(json.dumps(alternate_titles, ensure_ascii=False))),
                   label, info={"title": label})
        end(content="tvshows")
        return
    season = Kodi.input_dialog("Temporada", "1")
    episode = Kodi.input_dialog("Episódio", "1")
    try:
        season, episode = int(season or 0), int(episode or 0)
    except ValueError:
        season = episode = 0
    if season < 0 or episode <= 0:
        end(succeeded=False)
        return
    _show_aggregated_streams("series", item_id, title, year=year, season=season, episode=episode,
                             canonical_id=canonical_id, tmdb_id=tmdb_id,
                             alternate_titles=alternate_titles)


def cmd_cinemeta_series_eps():
    params = _parse_argv()
    item_id = dec(params.get("id", ""))
    title = dec(params.get("title", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    year = dec(params.get("year", ""))
    alternate_titles = _decode_title_list(params.get("alts", ""))
    try:
        season = int(params.get("season", 1))
    except (TypeError, ValueError):
        season = 1
    meta = _cinemeta_meta("series", item_id)
    if meta:
        meta_title = meta.get("title") or ""
        if title and meta_title and _normalize_title(title) != _normalize_title(meta_title):
            alternate_titles = list(dict.fromkeys([title] + alternate_titles))
        title = meta_title or title
        canonical_id = meta.get("imdbId") or canonical_id
        tmdb_id = meta.get("tmdbId") or tmdb_id
        year = meta.get("year") or meta.get("releaseInfo") or year
        alternate_titles = list(dict.fromkeys(alternate_titles + _meta_alternate_titles(meta)))
    seasons = stremio.series_seasons_from_meta(meta)
    episodes = seasons.get(season, [])
    if not episodes:
        Kodi.notify("Nenhum episódio encontrado nesta temporada.")
        end(succeeded=False)
        return
    for video in episodes:
        episode = video.get("episode")
        ep_title = video.get("title") or ("Episódio %s" % episode if episode is not None else "Episódio")
        label = ("E%02d · %s" % (episode, ep_title)) if episode is not None else ep_title
        folder(url("cinemeta_episode_streams", id=enc(item_id), title=enc(title),
                   video=enc(video.get("id") or ""), season=season, episode=episode if episode is not None else "",
                   canonical=enc(canonical_id), tmdb=enc(tmdb_id), year=enc(year or ""),
                   alts=enc(json.dumps(alternate_titles, ensure_ascii=False))), label,
               art={"thumb": video.get("thumbnail") or ""},
               info={"title": ep_title, "plot": video.get("overview") or "",
                     "season": season, "episode": episode if episode is not None else 0})
    end(content="episodes")


def cmd_cinemeta_episode_streams():
    params = _parse_argv()
    item_id = dec(params.get("id", ""))
    title = dec(params.get("title", ""))
    video_id = dec(params.get("video", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    year = dec(params.get("year", ""))
    alternate_titles = _decode_title_list(params.get("alts", ""))
    try:
        season = int(params.get("season")) if params.get("season") not in (None, "") else None
        episode = int(params.get("episode")) if params.get("episode") not in (None, "") else None
    except (TypeError, ValueError):
        season = episode = None
    _show_aggregated_streams("series", item_id, title, year=year, season=season,
                             episode=episode, video_id=video_id,
                             canonical_id=canonical_id, tmdb_id=tmdb_id,
                             alternate_titles=alternate_titles)


def _show_aggregated_streams(ctype, item_id, title, year=None, season=None, episode=None,
                             video_id=None, canonical_id=None, tmdb_id=None, alternate_titles=None):
    # xbmc.Player does not make the plugin row the playing URL, but Kodi may
    # still refresh the parent plugin directory after STOP. Reuse the exact
    # source list briefly so that refresh is instant and does not
    # trigger Telegram/Xtream/Stremio network discovery again.
    source_signature = _source_topology_signature()
    cache_key = source_cache.make_key(ctype, item_id, season, episode, video_id, canonical_id,
                                      source_signature=source_signature)
    streams = source_cache.load(cache_key)
    if streams:
        Kodi.log("ROUTER", "Cinemeta snapshot: %d fonte(s) reutilizada(s); sem nova busca." % len(streams),
                 level="info")
    else:
        streams = _aggregate_streams(
            ctype, item_id, title, year=year, season=season, episode=episode,
            video_id=video_id, canonical_id=canonical_id, tmdb_id=tmdb_id,
            alternate_titles=alternate_titles)
        if streams:
            source_cache.save(cache_key, streams)
    if not streams:
        Kodi.notify("Nenhuma fonte encontrada para '%s'." % title)
        end(succeeded=False)
        return
    for stream in streams:
        source = stream.get("addonName") or stream.get("source") or "Stream"
        quality = stream.get("quality") or "?"
        label = "%s | %s" % (source, quality)
        if stream.get("size"):
            label += " · %s" % stream["size"]
        info = {"title": title, "plot": stream.get("description") or ""}
        if year:
            info["year"] = _display_year(year) or str(year)
        if ctype == "series" and season is not None and episode is not None:
            info.update({"tvshowtitle": title, "season": season, "episode": episode})
        pinfo = _playback_info_param(ctype, title, year, season, episode)

        if stream.get("_tg_stream"):
            msg = stream.get("_tg_video") or {}
            video_action(url("tg_proxy_play", peer=enc(msg.get("peerId") or ""),
                         ptype=msg.get("peerType") or "", praw=msg.get("peerRawId") or "",
                         ah=msg.get("accessHash") or "", msg=msg.get("messageId") or 0,
                         name=enc(label), snap=cache_key, pinfo=pinfo), label, info=info)
            continue

        stream_url = stream.get("url")
        if stream_url:
            lower_url = stream_url.lower().split("|", 1)[0]
            mime = "hls" if ".m3u8" in lower_url else ("dash" if ".mpd" in lower_url else "direct")
            play_params = {
                "url": enc(stream_url), "name": enc(label), "mime": enc(mime),
                "subtitles": enc(json.dumps(stream.get("subtitles") or [])),
                "headers": enc(json.dumps(stream.get("headers") or {})),
                "snap": cache_key, "pinfo": pinfo,
            }
            if stream.get("manifestUrl"):
                # Preserve the 1.2 lazy subtitle flow for Stremio-derived sources.
                play_params.update({
                    "st_type": ctype, "st_id": enc(item_id),
                    "st_addon": enc(stream.get("manifestUrl") or ""), "st_video": enc(video_id or ""),
                    "st_season": season if season is not None else "",
                    "st_episode": episode if episode is not None else "",
                    "st_canonical": enc(canonical_id or ""), "st_tmdb": enc(tmdb_id or ""),
                })
            video_action(url("play_stream", **play_params), label, info=info)
        elif stream.get("ytId"):
            info["plot"] = (info.get("plot") or "") + "\n[YouTube: requer addon/player externo]"
            video_action(url("tg_play_unsupported", reason=enc("youtube")), label, info=info)
        else:
            info["plot"] = (info.get("plot") or "") + "\n[somente torrent - requer debrid/player externo]"
            video_action(url("tg_play_unsupported", reason=enc("torrent")), label, info=info)
    end(content="movies" if ctype == "movie" else "episodes")


# ---- Stremio --------------------------------------------------------------

def cmd_stremio_menu():
    addons = stremio.enabled_addons()
    folder(url("stremio_install"), "Adicionar addon (digitar URL)",
           info={"title": "Instala um addon Stremio a partir do link do manifest"})
    folder(url("stremio_pair_qr"), "Adicionar addon via QR (celular)",
           info={"title": "Mostra um QR; escaneie com o celular e cole o link do manifest"})
    folder(url("stremio_search_prompt"), "Pesquisar",
           info={"title": "Busca filmes e séries em todos os addons ativos"})
    for addon in addons:
        name = addon.get("name") or "Addon"
        folder(url("stremio_addon", addon=enc(addon.get("manifestUrl") or "")),
               name, art={"thumb": addon.get("logo") or ""},
               info={"title": addon.get("description") or name,
                     "plot": addon.get("description") or ""})
    end(content="files")

def cmd_stremio_install():
    raw = Kodi.input_dialog("URL do manifest do addon Stremio",
                            default="https://")
    if not raw:
        end(succeeded=False)
        return
    try:
        addon = stremio.install_addon(raw.strip())
        _invalidate_source_snapshots("addon Stremio adicionado")
        Kodi.notify("Addon instalado: %s" % addon.get("name"))
    except Exception as exc:
        Kodi.ok_dialog("Erro ao instalar", str(exc))
    cmd_stremio_menu()


def cmd_stremio_pair_qr():
    _run_pair_flow(
        "ARVIO - Adicionar addon Stremio",
        "Na aba 'Addon Stremio' cole o link do manifest (ex.: "
        "https://torrentio.strem.fun/manifest.json) e toque em Enviar.",
        _pair_stremio)
    cmd_stremio_menu()


def cmd_stremio_search_prompt():
    query = Kodi.input_dialog("Pesquisar (somente título)", "")
    if not query:
        end(succeeded=False)
        return
    _stremio_search_results(query)


def _stremio_search_results(query):
    addons = stremio.enabled_addons()
    # Stream-only providers (for example FrostStream) intentionally expose no
    # catalogs. They participate when a Cinemeta item is opened, but must not
    # create useless search jobs or be presented as catalog/search providers.
    searchable_addons = [a for a in addons if (a.get("catalogs") or []) or
                         stremio.resource_supports(a, "search")]
    jobs = [(addon, media_type) for media_type in ("movie", "series")
            for addon in searchable_addons]
    results = []
    progress = None
    if xbmcgui is not None:
        try:
            progress = xbmcgui.DialogProgress()
            progress.create("Arvio Stream", "Pesquisando nos addons ativos...")
        except Exception:
            progress = None
    executor = None
    try:
        if jobs:
            workers = min(4, len(jobs))
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
            future_map = {executor.submit(stremio.search_addon, addon, media_type, query): (addon, media_type)
                          for addon, media_type in jobs}
            completed = 0
            try:
                for future in concurrent.futures.as_completed(future_map, timeout=30):
                    completed += 1
                    addon, media_type = future_map[future]
                    if progress is not None:
                        try:
                            percent = int((completed * 100.0) / max(1, len(jobs)))
                            progress.update(percent, "Pesquisando: %s" % (addon.get("name") or "Addon"))
                            if progress.iscanceled():
                                for pending in future_map:
                                    pending.cancel()
                                Kodi.notify("Pesquisa cancelada.")
                                break
                        except Exception:
                            pass
                    try:
                        found = future.result()
                    except Exception as exc:
                        Kodi.log("STREMIO", "Busca falhou em %s: %s" % (addon.get("name"), exc), level="warning")
                        continue
                    for meta in found:
                        meta["addonName"] = addon.get("name")
                        results.append(meta)
            except concurrent.futures.TimeoutError:
                Kodi.log("STREMIO", "Busca global atingiu o limite de 30s.", level="warning")
                for pending in future_map:
                    pending.cancel()
    finally:
        if executor is not None:
            # Do not return from a Kodi subinterpreter while provider workers are
            # still alive; their HTTP calls already have bounded timeouts.
            try:
                executor.shutdown(wait=True)
            except Exception:
                pass
        if progress is not None:
            try:
                progress.close()
            except Exception:
                pass
    if not results:
        Kodi.notify("Nenhum resultado para: %s" % query)
        end(succeeded=False)
        return
    seen = set()
    for meta in results:
        canonical = meta.get("imdbId") or meta.get("id")
        key = (meta.get("type"), canonical)
        if key in seen:
            continue
        seen.add(key)
        target = url("stremio_item", addon=enc(meta.get("manifestUrl") or ""),
                     type=meta.get("type") or "movie", id=enc(meta.get("id") or ""),
                     canonical=enc(meta.get("imdbId") or ""), tmdb=enc(meta.get("tmdbId") or ""),
                     title=enc(meta.get("title") or ""))
        folder(target, meta.get("title"), art=_art_for(meta), info=_info_for_meta(meta))
    end(content="movies")

def cmd_stremio_addon():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    addon = stremio.find_addon(manifest_url)
    if not addon or not addon.get("enabled", True):
        Kodi.notify("Addon desativado ou não encontrado.")
        end(succeeded=False)
        return
    catalogs = addon.get("catalogs") or []
    if not catalogs:
        if stremio.resource_supports(addon, "stream"):
            Kodi.ok_dialog(
                addon.get("name") or "Addon Stremio",
                "Este addon atua como provedor de streams e não possui catálogo próprio. "
                "Ele será consultado automaticamente ao abrir filmes e episódios pelo Cinemeta."
            )
        else:
            Kodi.notify("Este addon não oferece catálogos navegáveis.")
        end(succeeded=False)
        return
    seen = set()
    for catalog in catalogs:
        ctype = catalog.get("type") or "movie"
        key = (ctype, catalog.get("id"))
        if key in seen:
            continue
        seen.add(key)
        label = catalog.get("name") or catalog.get("id")
        folder(url("stremio_catalog", addon=enc(manifest_url), type=ctype,
                   cat=enc(catalog.get("id") or "")), label, info={"title": label})
    end(content="files")

def cmd_stremio_catalog():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    ctype = params.get("type", "movie")
    cat_id = dec(params.get("cat", ""))
    try:
        skip = max(0, int(params.get("skip", 0)))
    except (TypeError, ValueError):
        skip = 0
    addon = stremio.find_addon(manifest_url)
    if not addon or not addon.get("enabled", True):
        end(succeeded=False); return
    catalog = next((c for c in (addon.get("catalogs") or [])
                    if c.get("type") == ctype and str(c.get("id")) == cat_id), None)
    try:
        items = stremio.fetch_catalog(addon, ctype, cat_id, skip=skip if catalog and stremio.catalog_supports_skip(catalog) else None)
    except Exception as exc:
        Kodi.ok_dialog("Erro ao carregar catálogo", str(exc)); end(succeeded=False); return
    if not items:
        Kodi.notify("Catálogo vazio.")
        end(succeeded=False); return
    for meta in items:
        target = url("stremio_item", addon=enc(manifest_url), type=meta.get("type") or ctype,
                     id=enc(meta.get("id") or ""), canonical=enc(meta.get("imdbId") or ""),
                     tmdb=enc(meta.get("tmdbId") or ""), title=enc(meta.get("title") or ""))
        folder(target, meta.get("title"), art=_art_for(meta), info=_info_for_meta(meta))
    if catalog and stremio.catalog_supports_skip(catalog):
        next_skip = skip + len(items)
        folder(url("stremio_catalog", addon=enc(manifest_url), type=ctype,
                   cat=enc(cat_id), skip=next_skip), "Próxima página ›",
               info={"title": "Carregar mais itens"})
    end(content="movies" if ctype == "movie" else "tvshows")

def cmd_stremio_item():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    ctype = params.get("type", "movie")
    item_id = dec(params.get("id", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    title = dec(params.get("title", ""))
    if ctype != "series":
        _show_streams(ctype, item_id, title, manifest_url, canonical_id=canonical_id, tmdb_id=tmdb_id)
        return

    meta = stremio.resolve_meta_all("series", item_id, manifest_url, [canonical_id, tmdb_id])
    seasons = stremio.series_seasons_from_meta(meta)
    if seasons:
        for season_num in sorted(seasons):
            label = "Temporada %d" % season_num
            folder(url("stremio_series_eps", addon=enc(manifest_url),
                       id=enc(item_id), title=enc(title), season=season_num,
                       canonical=enc(canonical_id), tmdb=enc(tmdb_id)),
                   label, info={"title": label})
        end(content="tvshows")
        return

    # Compatibility fallback for older stream-only addons that do not expose
    # meta.videos. This preserves existing functionality without inventing IDs
    # when proper episode metadata is available.
    season = Kodi.input_dialog("Temporada", "1")
    episode = Kodi.input_dialog("Episódio", "1")
    try:
        season, episode = int(season or 0), int(episode or 0)
    except ValueError:
        season = episode = 0
    if not season or not episode:
        end(succeeded=False)
        return
    _show_streams("series", item_id, title, manifest_url, season=season, episode=episode,
                  canonical_id=canonical_id, tmdb_id=tmdb_id)

def cmd_stremio_series_eps():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    item_id = dec(params.get("id", ""))
    title = dec(params.get("title", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    try:
        season = int(params.get("season", 1))
    except (TypeError, ValueError):
        season = 1
    meta = stremio.resolve_meta_all("series", item_id, manifest_url, [canonical_id, tmdb_id])
    seasons = stremio.series_seasons_from_meta(meta)
    for video in seasons.get(season, []):
        episode = video.get("episode") or 0
        ep_title = video.get("title") or ("Episódio %s" % episode)
        label = "E%02d · %s" % (episode, ep_title) if episode else ep_title
        folder(url("stremio_episode_streams", addon=enc(manifest_url), id=enc(item_id),
                   title=enc(title), video=enc(video.get("id") or ""), season=season,
                   episode=episode, canonical=enc(canonical_id), tmdb=enc(tmdb_id)), label,
               art={"thumb": video.get("thumbnail") or ""},
               info={"title": ep_title, "plot": video.get("overview") or "",
                     "season": season, "episode": episode})
    end(content="episodes")


def cmd_stremio_episode_streams():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    item_id = dec(params.get("id", ""))
    title = dec(params.get("title", ""))
    video_id = dec(params.get("video", ""))
    canonical_id = dec(params.get("canonical", ""))
    tmdb_id = dec(params.get("tmdb", ""))
    try:
        season = int(params.get("season", 0))
        episode = int(params.get("episode", 0))
    except (TypeError, ValueError):
        season = episode = 0
    _show_streams("series", item_id, title, manifest_url, season=season or None,
                  episode=episode or None, video_id=video_id, canonical_id=canonical_id,
                  tmdb_id=tmdb_id)


def _show_streams(ctype, item_id, title, manifest_url,
                  season=None, episode=None, video_id=None, canonical_id=None, tmdb_id=None):
    try:
        streams = stremio.resolve_streams_all(
            ctype, item_id, manifest_url, season=season, episode=episode,
            video_id=video_id, canonical_id=canonical_id, tmdb_id=tmdb_id)
    except Exception as exc:
        Kodi.ok_dialog("Erro ao buscar streams", str(exc))
        end(succeeded=False)
        return
    if not streams:
        Kodi.notify("Nenhum stream encontrado para '%s'." % title)
        end(succeeded=False)
        return
    for stream in streams:
        source = stream.get("addonName") or stream.get("source") or "Stream"
        quality = stream.get("quality") or "?"
        label = "%s | %s" % (source, quality)
        if stream.get("size"):
            label += " · %s" % stream["size"]
        info = {"title": title, "plot": stream.get("description") or ""}
        if ctype == "series" and season and episode:
            info.update({"season": season, "episode": episode})
        stream_url = stream.get("url")
        if stream_url:
            lower_url = stream_url.lower()
            mime = "hls" if ".m3u8" in lower_url else ("dash" if ".mpd" in lower_url else "direct")
            video_action(url("play_stream", url=enc(stream_url), name=enc(label), mime=enc(mime),
                         subtitles=enc(json.dumps(stream.get("subtitles") or [])),
                         headers=enc(json.dumps(stream.get("headers") or {})),
                         st_type=ctype, st_id=enc(item_id), st_addon=enc(manifest_url),
                         st_video=enc(video_id or ""), st_season=season or "",
                         st_episode=episode or "", st_canonical=enc(canonical_id or ""),
                         st_tmdb=enc(tmdb_id or "")),
                     label, info=info)
        elif stream.get("ytId"):
            info["plot"] = (info.get("plot") or "") + "\n[YouTube: requer addon/player externo]"
            video_action(url("tg_play_unsupported", reason=enc("youtube")), label, info=info)
        else:
            info["plot"] = (info.get("plot") or "") + "\n[somente torrent - requer debrid/player externo]"
            video_action(url("tg_play_unsupported", reason=enc("torrent")), label, info=info)
    end(content="movies" if ctype == "movie" else "episodes")

def cmd_tg_play_unsupported():
    end(succeeded=False)


def cmd_play_stream():
    params = _parse_argv()
    subtitles_raw = dec(params.get("subtitles", "[]"))
    st_item = dec(params.get("st_id", ""))
    if st_item:
        try:
            inline = json.loads(subtitles_raw or "[]")
        except Exception:
            inline = []
        try:
            season = int(params.get("st_season")) if params.get("st_season") else None
            episode = int(params.get("st_episode")) if params.get("st_episode") else None
        except (TypeError, ValueError):
            season = episode = None
        try:
            subtitles = stremio.resolve_subtitles_all(
                params.get("st_type") or "movie", st_item, dec(params.get("st_addon", "")),
                season=season, episode=episode, video_id=dec(params.get("st_video", "")),
                canonical_id=dec(params.get("st_canonical", "")), tmdb_id=dec(params.get("st_tmdb", "")),
                inline=inline)
            subtitles_raw = json.dumps(subtitles)
        except Exception as exc:
            Kodi.log("STREMIO", "Legendas lazy falharam; reprodução continua: %s" % exc, level="warning")
    try:
        player.play_http_direct(dec(params.get("url", "")),
                                dec(params.get("name", "Stream")),
                                dec(params.get("mime", "direct") or "direct"),
                                subtitles_raw, dec(params.get("headers", "{}")),
                                video_info=dec(params.get("pinfo", "{}")))
    finally:
        snap = params.get("snap") or ""
        if snap and source_cache.touch(snap):
            Kodi.log("ROUTER", "Snapshot Cinemeta renovado após playback.", level="debug")

# ---- Xtream ---------------------------------------------------------------

def cmd_xtream_menu():
    accounts = xtream.enabled_accounts()
    folder(url("xtream_qr_account"), "Adicionar conta Xtream via QR (celular)",
           info={"title": "Escaneie o QR e cole sua linha Xtream no celular"})
    folder(url("xtream_add_manual"), "Adicionar conta Xtream (digitar)",
           info={"title": "Digite: servidor usuario senha — ou uma URL get.php"})
    for account in accounts:
        label = account.get("name", "Conta")
        folder(url("xtream_account", account=enc(account.get("id") or "")),
               label, info={"title": label})
    end(content="files")

def cmd_xtream_add_manual():
    raw = Kodi.input_dialog("Linha Xtream (servidor usuario senha) ou URL", "")
    if not raw:
        end(succeeded=False)
        return
    name = Kodi.input_dialog("Nome da conta (opcional)")
    try:
        account = xtream.add_account(name or "", raw)
        _invalidate_source_snapshots("conta Xtream adicionada")
        Kodi.notify("Conta adicionada: %s" % account.get("name"))
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc))
    cmd_xtream_menu()


def cmd_xtream_qr_account():
    _run_pair_flow(
        "ARVIO - Adicionar conta Xtream",
        "Na aba 'Conta Xtream' cole sua linha (servidor usuario senha) ou a URL "
        "get.php e toque em Enviar.",
        _pair_xtream)
    cmd_xtream_menu()


def cmd_xtream_account():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        Kodi.notify("Conta desativada ou não encontrada. Use Configurações para gerenciar.")
        end(succeeded=False)
        return
    name = account.get("name", "Conta")
    folder(url("xtream_live_pick", account=enc(account_id)), "TV ao vivo",
           info={"title": "Canais ao vivo de %s" % name})
    folder(url("xtream_vod_pick", account=enc(account_id)), "Filmes (VOD)",
           info={"title": "Filmes de %s" % name})
    folder(url("xtream_series_pick", account=enc(account_id)), "Séries",
           info={"title": "Séries de %s" % name})
    folder(url("xtream_manage", account=enc(account_id)), "Gerenciar conta",
           info={"title": "Ativar, desativar ou remover"})
    end(content="files")

def cmd_xtream_manage():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    account = xtream.account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    options = ["Desativar conta" if account.get("enabled", True) else "Ativar conta",
               "Remover conta"]
    dialog = xbmcgui.Dialog()
    idx = dialog.select("Gerenciar: %s" % account.get("name"), options)
    if idx == -1:
        end(succeeded=False)
        return
    if options[idx] == "Desativar conta":
        xtream.set_xtream_enabled(account_id, False)
        _invalidate_source_snapshots("conta Xtream desativada")
    elif options[idx] == "Ativar conta":
        xtream.set_xtream_enabled(account_id, True)
        _invalidate_source_snapshots("conta Xtream ativada")
    elif options[idx] == "Remover conta":
        if Kodi.yesno("Remover conta", "Remover %s?" % account.get("name")):
            xtream.remove_xtream(account_id)
            _invalidate_source_snapshots("conta Xtream removida")
    cmd_xtream_menu()


def _pick_categories(kind, account_id, group_fn):
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    try:
        cats = group_fn(account)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc))
        end(succeeded=False)
        return
    folder(url(kind, account=enc(account_id), cat=enc("__all__")),
           "* Todos os grupos *", info={"title": "Listar todo o conteúdo"})
    for cat in cats:
        label = cat.get("category_name") or "Grupo"
        folder(url(kind, account=enc(account_id), cat=enc(cat.get("category_id") or "")),
               label, info={"title": label})
    end(content="files")

def cmd_xtream_live_pick():
    params = _parse_argv()
    _pick_categories("xtream_live", dec(params.get("account", "")), xtream.live_groups)


def cmd_xtream_live():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    cat_raw = dec(params.get("cat", ""))
    cat = None if cat_raw == "__all__" or not cat_raw else cat_raw
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    try:
        streams = xtream.live_streams(account, cat) if cat else xtream.live_streams_all(account)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc)); end(succeeded=False); return
    for stream in streams:
        label = stream.get("name")
        video_action(url("xtream_play", url=enc(stream.get("url") or ""), name=enc(label),
                     mime=enc(stream.get("mime") or "direct"),
                     headers=enc(json.dumps(stream.get("headers") or {}))),
                 label, art={"thumb": stream.get("logo") or ""}, info={"title": label})
    end(content="files")

def cmd_xtream_vod_pick():
    params = _parse_argv()
    _pick_categories("xtream_vod", dec(params.get("account", "")), xtream.vod_groups)


def cmd_xtream_vod():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    cat_raw = dec(params.get("cat", ""))
    cat = None if cat_raw == "__all__" or not cat_raw else cat_raw
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False); return
    try:
        films = xtream.vod_streams(account, cat) if cat else xtream.vod_streams_all(account)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc)); end(succeeded=False); return
    for film in films:
        info = {"title": film.get("name"), "plot": film.get("plot") or ""}
        if film.get("year"):
            info["year"] = film["year"]
        video_action(url("xtream_play", url=enc(film.get("url") or ""),
                     name=enc(film.get("name") or "Filme"), mime=enc("direct"),
                     headers=enc(json.dumps(film.get("headers") or {})),
                     pinfo=_playback_info_param("movie", film.get("name") or "Filme", film.get("year"))),
                 film.get("name"), art={"thumb": film.get("poster") or ""}, info=info)
    end(content="movies")

def cmd_xtream_series_pick():
    params = _parse_argv()
    _pick_categories("xtream_series", dec(params.get("account", "")), xtream.series_groups)


def cmd_xtream_series():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    cat_raw = dec(params.get("cat", ""))
    cat = None if cat_raw == "__all__" or not cat_raw else cat_raw
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    try:
        series = xtream.series_list(account, cat) if cat else xtream.series_list_all(account)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc))
        end(succeeded=False)
        return
    for ser in series:
        art = {"thumb": ser.get("poster") or "", "fanart": ser.get("backdrop") or ""}
        info = {"title": ser.get("name"), "plot": ser.get("plot") or ""}
        if ser.get("year"):
            info["year"] = ser["year"]
        folder(url("xtream_series_seasons", account=enc(account_id),
                   series=enc(ser.get("id") or ""), title=enc(ser.get("name") or "")),
               ser.get("name"), art=art, info=info)
    end(content="tvshows")


def cmd_xtream_series_seasons():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    series_id = dec(params.get("series", ""))
    title = dec(params.get("title", ""))
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    try:
        seasons = xtream.series_seasons(account, series_id)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc))
        end(succeeded=False)
        return
    for season_num in sorted(seasons.keys()):
        label = "Temporada %d" % season_num
        folder(url("xtream_series_eps", account=enc(account_id),
                   series=enc(series_id), season=season_num, title=enc(title)),
               label, info={"title": label})
    end(content="tvshows")


def cmd_xtream_series_eps():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    series_id = dec(params.get("series", ""))
    series_title = dec(params.get("title", "")) or "Série"
    try:
        season = int(params.get("season", 1))
    except (TypeError, ValueError):
        season = 1
    account = xtream.enabled_account_by_id(account_id)
    if not account:
        end(succeeded=False); return
    try:
        seasons = xtream.series_seasons(account, series_id)
    except Exception as exc:
        Kodi.ok_dialog("Erro", str(exc)); end(succeeded=False); return
    for ep in seasons.get(season, []):
        label = "E%02d · %s" % (ep.get("episode", 0), ep.get("title", ""))
        video_action(url("xtream_play", url=enc(ep.get("url") or ""), name=enc(label),
                     mime=enc("direct"), headers=enc(json.dumps(ep.get("headers") or {})),
                     pinfo=_playback_info_param("series", series_title, None, season, ep.get("episode", 0))),
                 label, info={"title": label, "tvshowtitle": series_title, "plot": ep.get("plot") or "",
                              "season": season, "episode": ep.get("episode", 0)})
    end(content="episodes")

def cmd_xtream_play():
    params = _parse_argv()
    player.play_http_direct(dec(params.get("url", "")), dec(params.get("name", "Canal")),
                            dec(params.get("mime", "direct") or "direct"), "[]",
                            dec(params.get("headers", "{}")),
                            video_info=dec(params.get("pinfo", "{}")))

# ---- Telegram -------------------------------------------------------------

def cmd_tg_menu():
    # Do not open a Telethon network client merely to render a Kodi directory.
    # Kodi may execute this route from a Dummy-* worker without an asyncio loop.
    # The saved StringSession is the durable state; it is restored lazily when
    # search or playback actually needs the network.
    connected = telegram_client.has_saved_session()
    if connected:
        folder(url("tg_search_prompt"), "Pesquisar vídeos nos canais/grupos inscritos",
               info={"title": "Busca somente em canais e grupos acessíveis à conta conectada"})
        folder(url("tg_disconnect"), "Desconectar Telegram",
               info={"title": "Encerra e remove a sessão do Telegram"})
    else:
        folder(url("tg_qr_login"), "Conectar Telegram (QR)",
               info={"title": "Escaneie o QR com o app do Telegram para entrar"})
        folder(url("tg_pair_session"), "Conectar Telegram via QR (celular)",
               info={"title": "Pareie com o celular e cole uma StringSession"})
    end(content="files")

def cmd_tg_pair_session():
    _run_pair_flow(
        "ARVIO - Conectar Telegram",
        "Na aba 'Telegram' cole a StringSession gerada no web client do ARVIO "
        "e toque em Enviar.",
        _pair_telegram)
    cmd_tg_menu()


def cmd_tg_qr_login():
    if not telegram_client.dependency_ok():
        Kodi.ok_dialog("Telegram", "A biblioteca do Telegram não está disponível nesta instalação do Kodi.")
        cmd_tg_menu(); return
    from .qr import show_qr_dialog
    qr_url = {"value": None}
    done = threading.Event()
    cancel = threading.Event()
    result = {}

    def on_qr(data_url):
        qr_url["value"] = data_url

    def worker():
        try:
            session, first, uid = telegram_client.start_qr_login(on_qr, cancel_event=cancel)
            result.update({"ok": True, "me": first})
        except telegram_client.TelegramPasswordNeeded:
            result.update({"ok": False, "password": True})
        except Exception as exc:
            if not cancel.is_set():
                result.update({"ok": False, "error": str(exc)})
        finally:
            done.set()

    qr_thread = threading.Thread(target=worker, name="ArvioTelegramQR", daemon=False)
    qr_thread.start()
    for _ in range(300):
        if done.is_set() or qr_url["value"]:
            break
        xbmc.sleep(100)
    if not qr_url["value"]:
        cancel.set()
        qr_thread.join(timeout=5.0)
        Kodi.ok_dialog("Telegram", result.get("error") or "Não foi possível gerar o código QR.")
        cmd_tg_menu(); return

    show_qr_dialog("ARVIO - Login Telegram",
                   "Telegram > Configurações > Dispositivos > Vincular aparelho",
                   qr_url["value"], completion_event=done,
                   timeout_s=TELEGRAM_QR_TIMEOUT)

    if not done.is_set():
        cancel.set()
    qr_thread.join(timeout=5.0)
    if qr_thread.is_alive():
        Kodi.log("TELEGRAM", "Thread de QR ainda ativa após cancelamento; aguardando encerramento do timeout interno.", level="warning")

    if result.get("password") or telegram_client.get_state().get("k") == "waitPassword":
        last_error = None
        for _ in range(3):
            password = Kodi.input_dialog("Senha de verificação em duas etapas do Telegram", "")
            if not password:
                telegram_client.cancel_pending_login()
                Kodi.ok_dialog("Telegram", "Login cancelado: a conta exige a senha de duas etapas.")
                cmd_tg_menu(); return
            try:
                _, first, _ = telegram_client.complete_2fa_password(password)
                Kodi.notify("Telegram conectado: %s" % first)
                cmd_tg_menu(); return
            except Exception as exc:
                last_error = exc
                Kodi.notify("Senha Telegram incorreta. Tente novamente.")
        telegram_client.cancel_pending_login()
        Kodi.ok_dialog("Telegram", "Não foi possível concluir a autenticação: %s" % last_error)
        cmd_tg_menu(); return

    if result.get("ok"):
        Kodi.notify("Telegram conectado: %s" % result.get("me", ""))
    else:
        Kodi.ok_dialog("Telegram", result.get("error") or "Falha na conexão.")
    cmd_tg_menu()

def cmd_tg_search_prompt():
    query = Kodi.input_dialog("Pesquisar vídeos no Telegram", "")
    if not query:
        end(succeeded=False)
        return
    # Move the container to a stable result URL. If Kodi refreshes the current
    # directory after STOP it reopens this query route, not the keyboard prompt.
    target = url("tg_search_results", q=enc(query))
    try:
        xbmc.executebuiltin("Container.Update(%s)" % target)
    except Exception:
        _tg_results(query)


def cmd_tg_search_results():
    params = _parse_argv()
    query = dec(params.get("q", ""))
    if not query:
        end(succeeded=False)
        return
    _tg_results(query)


def _tg_results(query):
    cache_key = source_cache.make_key("telegram-search", query)
    messages = source_cache.load(cache_key)
    if messages:
        Kodi.log("TELEGRAM", "Busca manual reutilizada do snapshot: %d resultado(s)." % len(messages),
                 level="info")
    else:
        try:
            messages = telegram_client.search_video_messages(query)
        except Exception as exc:
            Kodi.ok_dialog("Erro", str(exc)); end(succeeded=False); return
        if messages:
            source_cache.save(cache_key, messages)
    if not messages:
        Kodi.notify("Nenhum vídeo encontrado para: %s" % query)
        end(succeeded=False); return
    for msg in messages:
        name = msg.get("fileName") or "Vídeo"
        size = msg.get("size") or 0
        size_label = "%.2f GB" % (size / 1e9) if size >= 1e9 else "%.0f MB" % (size / 1e6)
        label = "%s · %s" % (name, size_label)
        video_action(url("tg_proxy_play", peer=enc(msg.get("peerId") or ""),
                     ptype=msg.get("peerType") or "", praw=msg.get("peerRawId") or "",
                     ah=msg.get("accessHash") or "", msg=msg.get("messageId") or 0,
                     name=enc(label)),
                 label, info={"title": name, "plot": msg.get("caption") or ""})
    end(content="files")

def cmd_tg_proxy_play():
    params = _parse_argv()
    peer_id = dec(params.get("peer", ""))
    message_id = params.get("msg", "")
    name = dec(params.get("name", "Vídeo"))
    playback_info = dec(params.get("pinfo", "{}"))
    peer_type = params.get("ptype", "")
    peer_raw_id = params.get("praw", "")
    access_hash = params.get("ah", "")
    snap = params.get("snap") or ""
    Kodi.log("TELEGRAM", "Play solicitado para message_id=%s; preparando spool adaptativo." % message_id,
             level="info")

    progress = None
    spool = None
    proxy_started = False
    try:
        video = telegram_client.fetch_stream_video(
            peer_id, message_id, peer_type, peer_raw_id, access_hash)

        probe = telegram_client.probe_download_speed(
            video, sample_bytes=telegram_client.recommended_probe_bytes())
        plan = telegram_client.adaptive_spool_plan(video, probe)
        target = int(plan.get("target_bytes") or int(video.get("size") or 0))
        total = int(video.get("size") or 0)
        measured = float(plan.get("measured_mbps") or 0.0)
        required = float(plan.get("required_mbps") or 0.0)
        mode = str(plan.get("mode") or "full")
        reason = str(plan.get("reason") or "")
        Kodi.log(
            "TELEGRAM",
            "Plano adaptativo: modo=%s, taxa=%.2f Mb/s, mídia=%.2f Mb/s, alvo=%d/%d MB, motivo=%s." %
            (mode, measured, required, int(target / (1024 * 1024)),
             int(total / (1024 * 1024)), reason), level="info")

        spool = telegram_client.start_progressive_download(video, prefix=probe.get("sample") or b"")
        try:
            progress = xbmcgui.DialogProgress()
            if mode == "full":
                progress.create("Arvio Stream - Telegram",
                                "Preparando vídeo no armazenamento para reprodução contínua...")
            else:
                progress.create("Arvio Stream - Telegram",
                                "Criando margem segura à frente do player...")
        except Exception:
            progress = None

        def cancelled():
            try:
                return bool(progress is not None and progress.iscanceled())
            except Exception:
                return False

        last_update = {"t": 0.0}

        def on_ready_progress(status, target_bytes):
            now = time.monotonic()
            if now - last_update["t"] < 0.25 and status["downloaded"] < target_bytes:
                return
            last_update["t"] = now
            downloaded = int(status["downloaded"] or 0)
            target_value = max(1, int(target_bytes or 1))
            percent = int(min(100, max(0, downloaded * 100.0 / target_value)))
            total_percent = int(min(100, max(0, downloaded * 100.0 / float(total or 1))))
            try:
                if progress is not None:
                    progress.update(
                        percent,
                        "Buffer Telegram: %.1f / %.1f MB · arquivo %d%% · %.2f Mb/s" %
                        (downloaded / (1024.0 * 1024.0), target_value / (1024.0 * 1024.0),
                         total_percent, float(status.get("rate_mbps") or 0.0)))
            except Exception:
                pass

        spool.wait_for(target, cancel_check=cancelled, progress_callback=on_ready_progress, timeout=None)
        if mode == "full" or target >= total:
            # For local-file playback wait until the producer has closed the
            # writer and disconnected Telegram. This avoids opening a complete
            # file while Windows/Android still considers it actively written.
            if not spool.join(timeout=15.0):
                raise RuntimeError("Pré-download terminou os bytes, mas o produtor não encerrou corretamente.")
            final_status = spool.status()
            if final_status.get("error") is not None:
                raise final_status["error"]
        try:
            if progress is not None:
                progress.close()
        except Exception:
            pass
        progress = None

        status = spool.status()
        if status.get("error") is not None:
            raise status["error"]
        Kodi.log(
            "TELEGRAM",
            "Margem inicial pronta: %d/%d byte(s); downloader continua até 100%%." %
            (int(status.get("downloaded") or 0), total), level="info")

        if snap:
            source_cache.touch(snap)

        if mode == "full" or int(status.get("downloaded") or 0) >= total:
            # Downloader reached EOF before PLAY. No HTTP waiting is required.
            media_url = spool.path
            Kodi.log("TELEGRAM", "Reprodução local após pré-download seguro.", level="info")
        else:
            # Kodi receives the declared final size and reads only committed
            # bytes; the independent producer keeps advancing on disk to EOF.
            media_url = telegram_client.register_spool_url(video, spool)
            proxy_started = True
            Kodi.log("TELEGRAM", "Reprodução progressiva via spool local; produtor independente ativo.",
                     level="info")

        if not player.play_http_direct(media_url, name, "direct", "[]", "{}", probe=False,
                                       video_info=playback_info):
            return

        # Keep this invocation alive for producer/proxy lifecycle. On STOP the
        # producer is cancelled immediately and its temp file is reclaimed.
        try:
            player_ref = xbmc.Player()
            monitor = xbmc.Monitor()
            started = False
            for _ in range(120):
                if monitor.abortRequested():
                    break
                if player_ref.isPlaying():
                    started = True
                    break
                xbmc.sleep(250)
            if started:
                misses = 0
                last_snapshot_touch = time.monotonic()
                while not monitor.abortRequested() and misses < 12:
                    if player_ref.isPlaying():
                        misses = 0
                        if snap and time.monotonic() - last_snapshot_touch >= 30.0:
                            source_cache.touch(snap)
                            last_snapshot_touch = time.monotonic()
                    else:
                        misses += 1
                    xbmc.sleep(500)
        except Exception as exc:
            Kodi.log("TELEGRAM", "Monitor do player encerrou: %s" % exc, level="warning")
    except telegram_client.TelegramOperationCancelled:
        Kodi.notify("Operação Telegram cancelada.")
    except RuntimeError as exc:
        # ProgressiveSpool raises RuntimeError on UI cancellation as well.
        if "cancel" in str(exc).lower():
            Kodi.notify("Operação Telegram cancelada.")
        else:
            Kodi.ok_dialog("Telegram", "Não foi possível preparar o vídeo: %s" % exc)
    except Exception as exc:
        Kodi.ok_dialog("Telegram", "Não foi possível preparar o vídeo: %s" % exc)
    finally:
        try:
            if progress is not None:
                progress.close()
        except Exception:
            pass
        if proxy_started:
            try:
                for _ in range(int(max(1, const.TG_PROXY_GRACE_S * 4))):
                    if telegram_client.proxy_can_stop(const.TG_PROXY_GRACE_S):
                        break
                    xbmc.sleep(250)
            except Exception:
                pass
            telegram_client.stop_proxy()
        if spool is not None:
            telegram_client.stop_progressive_download(spool, remove=True)
            Kodi.log("TELEGRAM", "STOP/teardown: downloader encerrado e temporário solicitado para remoção.",
                     level="info")
        if snap and source_cache.touch(snap):
            Kodi.log("ROUTER", "Snapshot Cinemeta renovado após playback Telegram.", level="debug")

def cmd_tg_disconnect():
    if Kodi.yesno("Telegram", "Desconectar a conta do Telegram?"):
        telegram_client.disconnect()
        _invalidate_source_snapshots("Telegram desconectado")
        Kodi.notify("Telegram desconectado.")
    cmd_tg_menu()


# ---- Settings -------------------------------------------------------------

def cmd_settings_menu():
    addons = stremio.list_installed()
    if addons:
        folder(url("settings_addons"), "Gerenciar addons (%d)" % len(addons),
               info={"title": "Ativar, desativar ou remover addons instalados"})
    accounts = xtream.accounts()
    if accounts:
        folder(url("settings_xtream"), "Gerenciar contas Xtream (%d)" % len(accounts),
               info={"title": "Ativar, desativar ou remover contas Xtream"})
    folder(url("settings_telegram_clear"), "Limpar sessão Telegram",
           info={"title": "Remove a sessão salva do Telegram"})
    end(content="files")


def cmd_settings_addons():
    addons = stremio.list_installed()
    if not addons:
        Kodi.notify("Nenhum addon instalado.")
        end(succeeded=False)
        return
    for addon in addons:
        label = addon.get("name", "Addon")
        if not addon.get("enabled", True):
            label += " [off]"
        folder(url("settings_addon", addon=enc(addon.get("manifestUrl") or "")),
               label, info={"title": addon.get("description") or label,
                            "plot": addon.get("description") or ""})
    end(content="files")


def cmd_settings_addon():
    params = _parse_argv()
    manifest_url = dec(params.get("addon", ""))
    addon = stremio.find_addon(manifest_url)
    if not addon:
        end(succeeded=False)
        return
    enabled = addon.get("enabled", True)
    options = ["Desativar" if enabled else "Ativar", "Remover"]
    dialog = xbmcgui.Dialog()
    idx = dialog.select("Addon: %s" % addon.get("name"), options)
    if idx == -1:
        end(succeeded=False)
        return
    if options[idx] in ("Desativar", "Ativar"):
        stremio.set_addon_enabled(manifest_url, not enabled)
        _invalidate_source_snapshots("estado do addon Stremio alterado")
    elif options[idx] == "Remover":
        if Kodi.yesno("Remover addon", "Remover %s?" % addon.get("name")):
            stremio.uninstall_addon(manifest_url)
            _invalidate_source_snapshots("addon Stremio removido")
    cmd_settings_addons()


def cmd_settings_xtream():
    accounts = xtream.accounts()
    if not accounts:
        Kodi.notify("Nenhuma conta Xtream.")
        end(succeeded=False)
        return
    for account in accounts:
        label = account.get("name", "Conta")
        if not account.get("enabled", True):
            label += " [off]"
        folder(url("settings_xtream_acc", account=enc(account.get("id") or "")),
               label, info={"title": label})
    end(content="files")


def cmd_settings_xtream_acc():
    params = _parse_argv()
    account_id = dec(params.get("account", ""))
    account = xtream.account_by_id(account_id)
    if not account:
        end(succeeded=False)
        return
    enabled = account.get("enabled", True)
    options = ["Desativar" if enabled else "Ativar", "Remover"]
    dialog = xbmcgui.Dialog()
    idx = dialog.select("Conta: %s" % account.get("name"), options)
    if idx == -1:
        end(succeeded=False)
        return
    if options[idx] in ("Desativar", "Ativar"):
        xtream.set_xtream_enabled(account_id, not enabled)
        _invalidate_source_snapshots("estado da conta Xtream alterado")
    elif options[idx] == "Remover":
        if Kodi.yesno("Remover conta", "Remover %s?" % account.get("name")):
            xtream.remove_xtream(account_id)
            _invalidate_source_snapshots("conta Xtream removida")
    cmd_settings_xtream()


def cmd_settings_telegram_clear():
    if Kodi.yesno("Telegram", "Limpar e desconectar a sessão salva do Telegram?"):
        try:
            telegram_client.clear_session_local()
            _invalidate_source_snapshots("sessão Telegram limpa")
            Kodi.notify("Sessão do Telegram limpa.")
        except Exception as exc:
            Kodi.ok_dialog("Telegram", "Não foi possível limpar a sessão: %s" % exc)
    cmd_settings_menu()

# ---- dispatch -------------------------------------------------------------

ROUTES = {
    "main": cmd_main,
    "cinemeta_menu": cmd_cinemeta_menu,
    "cinemeta_categories": cmd_cinemeta_categories,
    "cinemeta_catalog": cmd_cinemeta_catalog,
    "cinemeta_search_prompt": cmd_cinemeta_search_prompt,
    "cinemeta_item": cmd_cinemeta_item,
    "cinemeta_series_eps": cmd_cinemeta_series_eps,
    "cinemeta_episode_streams": cmd_cinemeta_episode_streams,
    "stremio_menu": cmd_stremio_menu,
    "stremio_install": cmd_stremio_install,
    "stremio_pair_qr": cmd_stremio_pair_qr,
    "stremio_search_prompt": cmd_stremio_search_prompt,
    "stremio_addon": cmd_stremio_addon,
    "stremio_catalog": cmd_stremio_catalog,
    "stremio_item": cmd_stremio_item,
    "stremio_streams": cmd_stremio_item,
    "stremio_series_eps": cmd_stremio_series_eps,
    "stremio_episode_streams": cmd_stremio_episode_streams,
    "tg_play_unsupported": cmd_tg_play_unsupported,
    "play_stream": cmd_play_stream,
    "xtream_menu": cmd_xtream_menu,
    "xtream_add_manual": cmd_xtream_add_manual,
    "xtream_qr_account": cmd_xtream_qr_account,
    "xtream_account": cmd_xtream_account,
    "xtream_manage": cmd_xtream_manage,
    "xtream_live_pick": cmd_xtream_live_pick,
    "xtream_live": cmd_xtream_live,
    "xtream_vod_pick": cmd_xtream_vod_pick,
    "xtream_vod": cmd_xtream_vod,
    "xtream_series_pick": cmd_xtream_series_pick,
    "xtream_series": cmd_xtream_series,
    "xtream_series_seasons": cmd_xtream_series_seasons,
    "xtream_series_eps": cmd_xtream_series_eps,
    "xtream_play": cmd_xtream_play,
    "tg_menu": cmd_tg_menu,
    "tg_qr_login": cmd_tg_qr_login,
    "tg_pair_session": cmd_tg_pair_session,
    "tg_search_prompt": cmd_tg_search_prompt,
    "tg_search_results": cmd_tg_search_results,
    "tg_proxy_play": cmd_tg_proxy_play,
    "tg_disconnect": cmd_tg_disconnect,
    "settings_menu": cmd_settings_menu,
    "settings_addons": cmd_settings_addons,
    "settings_addon": cmd_settings_addon,
    "settings_xtream": cmd_settings_xtream,
    "settings_xtream_acc": cmd_settings_xtream_acc,
    "settings_telegram_clear": cmd_settings_telegram_clear,
}


def dispatch():
    global _handle
    try:
        _handle = int(sys.argv[1])
    except (IndexError, ValueError):
        _handle = -1
    params = _parse_argv()
    mode = params.get("mode", "main") or "main"
    handler = ROUTES.get(mode)
    if handler is None:
        Kodi.notify("Modo desconhecido: %s" % mode)
        end(succeeded=False)
        return
    try:
        handler()
    except Exception as exc:
        Kodi.log("ROUTER", "Falha no modo %s: %s" % (mode, exc), level="error")
        try:
            Kodi.ok_dialog("Arvio Stream", "Erro: %s" % exc)
        except Exception:
            pass
        try:
            end(succeeded=False)
        except Exception:
            pass