#!/usr/bin/env python3
"""
DJ Library Core — MusicBrainz Lookup Helper
=============================================
Multi-strategy MusicBrainz recording search.
Tries structured queries first, then reversed, then free-text fallback.
Logs every attempt so the operator can see exactly what happened.

No API key required — uses User-Agent identification per MB guidelines.
Degrades gracefully if network is unavailable or rate-limited.
"""

import json
import re
import time
from pathlib import Path

import requests

# ─── Configuration ────────────────────────────────────────────────
_CONFIG_PATH = Path(__file__).parent.parent / "data" / "musicbrainz_config.json"

_DEFAULT_CONFIG = {
    "app_name": "NGKsPlayerNative-DJLibraryCore",
    "app_version": "1.0",
    "contact": "ngks-operator@localhost",
    "api_base": "https://musicbrainz.org/ws/2",
    "max_results": 8,
    "timeout_seconds": 15,
}


def _load_config():
    if _CONFIG_PATH.exists():
        try:
            with open(_CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)
            merged = dict(_DEFAULT_CONFIG)
            merged.update(cfg)
            return merged
        except Exception:
            pass
    return dict(_DEFAULT_CONFIG)


def _user_agent(cfg):
    return f"{cfg['app_name']}/{cfg['app_version']} ({cfg['contact']})"


# ─── Sanitization ────────────────────────────────────────────────

def sanitize_text(raw):
    """Normalize a raw string for query use."""
    s = raw.strip()
    # Normalize apostrophes / quotes
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    s = s.replace("`", "'")
    # Remove path fragments
    if "/" in s or "\\" in s:
        s = Path(s).stem
    # Strip extension if present
    if re.search(r'\.\w{2,4}$', s):
        s = re.sub(r'\.\w{2,4}$', '', s)
    # Remove leading track numbers like "017 - " or "03. "
    s = re.sub(r'^\d{1,3}\s*[-.\)]\s*', '', s)
    # Remove bracket junk like [Official Video], (Remastered), {HQ}
    s = re.sub(r'[\[\({][^)\]\}]*[\]\)}]', '', s)
    # Remove pipe junk like "| Interscope", "| Napalm Records"
    s = re.sub(r'\s*\|.*$', '', s)
    # Collapse underscores to spaces
    s = s.replace("_", " ")
    # Collapse multiple spaces/dashes
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(" -")
    return s


def _split_artist_title(text):
    """Split 'Artist - Title' or 'Title - Artist' from a dash-separated string."""
    # Try the most common separator: space-dash-space
    if " - " in text:
        parts = text.split(" - ", 1)
        return parts[0].strip(), parts[1].strip()
    # Try bare dash
    if " – " in text:  # en-dash
        parts = text.split(" – ", 1)
        return parts[0].strip(), parts[1].strip()
    return "", text.strip()


# ─── Query execution ─────────────────────────────────────────────

def _run_query(cfg, query_str, limit=None):
    """Execute a single MusicBrainz recording search, return (results, error)."""
    url = f"{cfg['api_base']}/recording/"
    params = {
        "query": query_str,
        "fmt": "json",
        "limit": limit or cfg["max_results"],
    }
    headers = {
        "User-Agent": _user_agent(cfg),
        "Accept": "application/json",
    }
    try:
        resp = requests.get(url, params=params, headers=headers,
                            timeout=cfg["timeout_seconds"])
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        return [], "timeout"
    except requests.exceptions.ConnectionError:
        return [], "no_network"
    except requests.exceptions.HTTPError as e:
        return [], f"http_{e.response.status_code}" if e.response else "http_err"
    except Exception as e:
        return [], str(e)

    try:
        data = resp.json()
    except Exception:
        return [], "bad_json"

    results = []
    for rec in data.get("recordings", []):
        artist_credit = ""
        for ac in rec.get("artist-credit", []):
            if isinstance(ac, dict):
                artist_credit += ac.get("name", "")
                artist_credit += ac.get("joinphrase", "")
        release_title = ""
        release_date = ""
        releases = rec.get("releases", [])
        if releases:
            release_title = releases[0].get("title", "")
            release_date = releases[0].get("date", "")
        results.append({
            "artist": artist_credit.strip(),
            "title": rec.get("title", ""),
            "release": release_title,
            "release_date": release_date,
            "score": rec.get("score", 0),
            "mbid": rec.get("id", ""),
        })
    return results, None


# ─── Multi-strategy lookup ───────────────────────────────────────

def lookup_recording(artist="", title="", filename="", extra_query=""):
    """
    Multi-strategy MusicBrainz lookup.

    Returns:
        (best_results, error_or_None, attempt_log)
        attempt_log: list of dicts {label, query, count, error, chosen}
    """
    cfg = _load_config()
    log = []  # list of {label, query, count, error, chosen}

    # Sanitize inputs
    s_artist = sanitize_text(artist) if artist else ""
    s_title = sanitize_text(title) if title else ""
    s_filename = sanitize_text(filename) if filename else ""

    # Also try splitting filename on dash
    fn_left, fn_right = "", ""
    if s_filename and " - " in s_filename:
        fn_left, fn_right = _split_artist_title(s_filename)
    elif s_filename and " – " in s_filename:
        fn_left, fn_right = _split_artist_title(s_filename)

    # Build ordered list of (label, query_string) attempts
    # Strategy: try free-text first (fast, broad), then structured (precise)
    attempts = []

    # A. Free-text: artist + title combined (fastest, usually works)
    if s_artist or s_title:
        free = f"{s_artist} {s_title}".strip()
        if free:
            attempts.append(("A: free-text artist+title", free))

    # B. Free-text from filename (fast broad search)
    if s_filename:
        attempts.append(("B: free-text filename", s_filename))

    # C. Structured: parsed artist + title
    if s_artist and s_title:
        attempts.append((
            "C: artist+title (structured)",
            f'artist:"{s_artist}" AND recording:"{s_title}"',
        ))

    # D. Reversed structured: swap artist/title
    if s_artist and s_title:
        attempts.append((
            "D: reversed artist/title",
            f'artist:"{s_title}" AND recording:"{s_artist}"',
        ))

    # E. From filename dash split — left=title, right=artist
    if fn_left and fn_right:
        attempts.append((
            "E: filename left=title right=artist",
            f'artist:"{fn_right}" AND recording:"{fn_left}"',
        ))

    # F. From filename dash split — left=artist, right=title
    if fn_left and fn_right:
        attempts.append((
            "F: filename left=artist right=title",
            f'artist:"{fn_left}" AND recording:"{fn_right}"',
        ))

    # G. Loose structured (no quotes, just field hints)
    if s_artist and s_title:
        attempts.append((
            "G: loose artist+title",
            f'artist:{s_artist} AND recording:{s_title}',
        ))

    if extra_query:
        attempts.append(("X: extra query", extra_query))

    if not attempts:
        return [], "No search terms provided", log

    # Execute attempts in order, stop at first good result (score >= 80)
    best_results = []
    best_label = ""
    net_error = None

    for label, qstr in attempts:
        results, err = _run_query(cfg, qstr)
        entry = {
            "label": label,
            "query": qstr,
            "count": len(results),
            "error": err,
            "chosen": False,
        }
        log.append(entry)

        if err:
            if err == "no_network":
                net_error = err
                break  # no point retrying on network failure
            if err == "timeout":
                net_error = err
                # Don't break — simpler queries may succeed
            continue

        # Accept if we got results with score >= 60
        if results and results[0]["score"] >= 60:
            entry["chosen"] = True
            best_results = results
            best_label = label
            break

        # Keep the best so far even if score < 60
        if results and (not best_results or
                        results[0]["score"] > best_results[0]["score"]):
            best_results = results
            best_label = label

    # Mark the chosen attempt
    if best_results and not any(e["chosen"] for e in log):
        for e in log:
            if e["label"] == best_label:
                e["chosen"] = True
                break

    if net_error == "no_network":
        return [], "MusicBrainz unreachable (no network?)", log
    if net_error == "timeout":
        return [], "MusicBrainz request timed out", log

    return best_results, None, log


def ensure_config():
    """Create default config file if it doesn't exist."""
    if not _CONFIG_PATH.exists():
        _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(_DEFAULT_CONFIG, f, indent=2)
    return _CONFIG_PATH
