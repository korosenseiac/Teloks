"""
torrent/trackers.py — Public BitTorrent tracker list for improved peer
discovery and download speed.

Curated from https://github.com/ngosang/trackerslist and similar sources.
Having many trackers dramatically increases the number of peers found,
which directly improves download speed — especially for popular torrents.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Public tracker list (UDP → HTTP → WSS)
# ---------------------------------------------------------------------------

PUBLIC_TRACKERS: list[str] = [
    # ---- UDP trackers (fastest, lowest overhead) ----
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.pomf.se:80/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://tracker.monitorit4.me:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://tracker.theoks.net:6969/announce",
    "udp://tracker1.bt.moack.co.kr:80/announce",
    "udp://tracker2.dler.org:80/announce",
    "udp://tracker.bittor.pw:1337/announce",
    "udp://tracker.4.babico.name.tr:3131/announce",
    "udp://p4p.arenabg.com:1337/announce",
    "udp://explodie.org:6969/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://movies.zsw.ca:6969/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://tracker.altrosky.nl:6969/announce",
    "udp://tracker.cubonegro.lol:6969/announce",
    "udp://tracker.filemail.com:6969/announce",
    "udp://tracker.fnix.net:6969/announce",
    "udp://tracker.jonaslsa.com:6969/announce",
    "udp://tracker.lelux.fi:6969/announce",
    "udp://tracker.leech.ie:1337/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://tracker.ccp.ovh:6969/announce",
    "udp://tracker.army:6969/announce",
    "udp://bt1.archive.org:6969/announce",
    "udp://bt2.archive.org:6969/announce",
    "udp://ipv4.tracker.harry.lu:80/announce",
    "udp://fe.dealclub.de:6969/announce",
    "udp://mail.realliferpg.de:6969/announce",
    "udp://tracker.srv00.com:6969/announce",
    "udp://www.torrent.eu.org:451/announce",
    "udp://tracker.dump.cl:6969/announce",
    "udp://tracker.breizh.pm:6969/announce",
    "udp://retracker.lanta-net.ru:2710/announce",
    "udp://9.rarbg.com:2810/announce",

    # ---- HTTP/HTTPS trackers (wider compatibility) ----
    "http://tracker.opentrackr.org:1337/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "https://tracker.nanoha.org:443/announce",
    "https://tracker.lilithraws.org:443/announce",
    "https://tracker.tamersunion.org:443/announce",
    "https://trackme.theom.nz:443/announce",
    "http://tracker1.bt.moack.co.kr:80/announce",
    "http://tracker.bt4g.com:2095/announce",
    "https://tracker.gbitt.info:443/announce",
    "https://tracker.loligirl.cn:443/announce",
    "http://tracker.files.fm:6969/announce",
    "https://tr.burnabyhighstar.com:443/announce",
    "https://tracker.cloudit.top:443/announce",
    "https://tracker.yemekyedim.com:443/announce",
    "http://montreal.nyap2p.com:8080/announce",
    "http://nyaa.tracker.wf:7777/announce",
    "http://open.acgnxtracker.com:80/announce",
    "http://share.camoe.cn:8080/announce",
    "http://t.nyaatracker.com:80/announce",
    "http://tracker.dler.org:6969/announce",
    "http://tracker.gbitt.info:80/announce",
    "http://tracker.renfei.net:8080/announce",

    # ---- WebSocket trackers ----
    "wss://tracker.openwebtorrent.com:443/announce",
    "wss://tracker.btorrent.xyz:443/announce",
    "wss://tracker.files.fm:7073/announce",
]


def get_tracker_string() -> str:
    """Return comma-separated tracker list for aria2c ``--bt-tracker`` flag."""
    return ",".join(PUBLIC_TRACKERS)


def get_tracker_count() -> int:
    """Return number of trackers in the list."""
    return len(PUBLIC_TRACKERS)
