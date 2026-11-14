# -*- coding: utf-8 -*-
"""Shared constants for Arvio Stream."""

ADDON_ID = "plugin.video.arvio-stream"
ADDON_NAME = "Arvio Stream"

# Telegram MTProto app credentials. Same public api_id/api_hash shipped in the
# ARVIO Android APK and browser bundle (they identify the ARVIO application,
# never the user account).
TELEGRAM_API_ID = 23905496
TELEGRAM_API_HASH = "1e48b355edfe55f9a4fbf8d3c2324628"

# Stream resolver behaviour.
TELEGRAM_SEARCH_TIMEOUT_S = 20
TELEGRAM_MAX_RESULTS = 100
TG_SEARCH_FALLBACK_CONCURRENCY_DESKTOP = 6
TG_SEARCH_FALLBACK_CONCURRENCY_LOW_RESOURCE = 3

# Local services.
PAIR_PORT = 8138        # phone -> Kodi pairing page (LAN)
TG_PROXY_PORT = 8139    # Telegram local HTTP proxy (loopback only)
TG_PROXY_CHUNK_BYTES = 2 * 1024 * 1024
TG_PROXY_GRACE_S = 3

# MTProto transport safety.
#
# Telegram upload.getFile can support larger protocol parts when alignment and
# 1 MiB boundaries are respected, and Telethon normally caps iter_download's
# request_size at 512 KiB. Arvio intentionally keeps 256 KiB as the production
# block because it is the size repeatedly validated by the real Kodi logs and
# does not depend on a dynamic boundary calculation. Larger values must not be
# treated as a generic drop-in tuning knob.
TG_TELETHON_REQUEST_BYTES = 256 * 1024  # first block / fast first byte
TG_TELETHON_STREAM_BYTES = 256 * 1024   # conservative ordered media block
TG_TELETHON_PIPELINE_CHUNKS = 10        # legacy proxy fallback only

# Multi-sender remains bounded and experimental. 1.4.9 does NOT use it for the
# progressive media spool because the 1.4.8 real log showed packet corruption
# after the multi-sender path became active. Integrity wins over raw speed.
TG_TELETHON_CONNECTIONS_FREE = 2
TG_TELETHON_CONNECTIONS_PREMIUM = 4

# Adaptive Telegram progressive spool.
TG_ADAPTIVE_SAMPLE_BYTES_DESKTOP = 1024 * 1024
TG_ADAPTIVE_SAMPLE_BYTES_LOW_RESOURCE = 512 * 1024
TG_ADAPTIVE_SAFETY_RATIO = 1.15
TG_ADAPTIVE_FORCE_FULL_RATIO = 0.75
TG_ADAPTIVE_FAST_HEADROOM_S = 20
TG_ADAPTIVE_NORMAL_HEADROOM_S = 35
TG_ADAPTIVE_TIGHT_HEADROOM_S = 60
TG_ADAPTIVE_EDGE_HEADROOM_S = 120
TG_ADAPTIVE_DEFICIT_EXTRA_S = 30
TG_SPOOL_READ_BYTES = 256 * 1024

# Temporary-storage policy. Progressive Telegram buffering is disk-backed, not
# RAM-backed. Keep meaningful room for Android/Kodi/system operation.
TG_TEMP_RESERVE_MIN_BYTES = 1024 * 1024 * 1024       # 1 GiB
TG_TEMP_RESERVE_FRACTION = 0.10                      # or 10% of filesystem
TG_TEMP_ORPHAN_MAX_AGE_S = 12 * 60 * 60             # crash leftovers only

# Stremio addon network timeouts (seconds).
HTTP_TIMEOUT = 20
STREMIO_STREAM_TIMEOUT = 15
STREMIO_RESOLVE_BUDGET = 25
USER_AGENT = "ArvioStream/1.4.9 Kodi"

ROUTE_SEP = "|||"
