"""
hls/ — HLS (m3u8) stream downloader module.

Detects HLS/m3u8 links, downloads the stream (parallel, SOCKS5-proxy-aware
segment fetcher with a direct-ffmpeg fallback) and remuxes it into an MP4 with
the *same* stream-copy method the bot already uses for MKV → MP4
(``app/utils/video_compressor.py``), before the file is uploaded through the
shared Direct pipeline (backup group, caption, splitting, thumbnail).

Module layout
-------------
* ``playlist.py``   — dependency-free M3U8 parser (variants, segments, keys, fMP4)
* ``client.py``     — proxy-aware HTTP client for playlists / segments / key files
* ``downloader.py`` — download + remux orchestration, cleanup, self-test
* ``handler.py``    — Telegram entry point + job (mirrors ``app/direct/handler.py``)

Nothing in this package imports from ``app.bot`` at module level, so it can be
imported (and its self-tests run) on its own:

    python -m app.hls.playlist
    python -m app.hls.downloader
"""
