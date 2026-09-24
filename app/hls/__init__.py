"""
hls/ — HLS (m3u8) stream downloader module.

Detects HLS/m3u8 links, downloads the stream (parallel, SOCKS5-proxy-aware
segment fetcher with a direct-ffmpeg fallback) and remuxes it into an MP4 with
the *same* stream-copy method the bot already uses for MKV → MP4
(``app/utils/video_compressor.py``), before the file is uploaded through the
shared Direct pipeline (backup group, caption, splitting, thumbnail).

It also accepts a video *page* link: the m3u8 embedded in the page is extracted
and used instead, because some CDNs only honour playlist URLs minted for the
client that loaded the page. That makes signed, client-bound streams work where
a playlist URL copied out of a browser would answer 403. When the URL is built
inside obfuscated JavaScript (no m3u8 in the HTML at all), an optional headless
Chromium opens the page once and captures the first manifest it requests.

Module layout
-------------
* ``playlist.py``   — dependency-free M3U8 parser (variants, segments, keys, fMP4)
* ``client.py``     — proxy-aware HTTP client for playlists / segments / key files
* ``extract.py``    — find the manifest inside an HTML page (page → m3u8)
* ``browser.py``    — optional headless-Chromium fallback for JS-only players
* ``downloader.py`` — download + remux orchestration, cleanup, self-test
* ``handler.py``    — Telegram entry points + job (mirrors ``app/direct/handler.py``)

Nothing in this package imports from ``app.bot`` at module level, so it can be
imported (and its self-tests run) on its own:

    python -m app.hls.playlist
    python -m app.hls.extract
    python -m app.hls.browser
    python -m app.hls.downloader
"""
