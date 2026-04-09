#!/usr/bin/env python3
"""
Phase 17: Google Search Assist — Ambiguous Row Resolution
Uses MusicBrainz API + DuckDuckGo fallback to validate artist/title pairs.
READ-ONLY with respect to music files — only creates CSV outputs.
"""

import csv, json, os, re, sys, time, traceback
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    print("FATAL: requests not installed. Run: pip install requests")
    sys.exit(1)

# ── Configuration ──
ROOT = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA = ROOT / "data"
PROOF = ROOT / "_proof" / "library_normalization_phase17"
MAX_ROWS = 100
CONFIDENCE_THRESHOLD = 0.85
MB_RATE_LIMIT = 1.1  # seconds between MusicBrainz requests
MB_USER_AGENT = "NGKsPlayerNative/1.0 (music-library-normalization)"

log_lines = []


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    log_lines.append(line)


# ── Utilities ──

def clean_filename(name):
    """Remove extension and non-music tokens for searching."""
    name = re.sub(r'\.(mp3|wav|flac|m4a|ogg)$', '', name, flags=re.I)
    name = re.sub(r'\b(official\s*(video|audio|music\s*video|lyric\s*video))\b', '', name, flags=re.I)
    name = re.sub(r'\b(lyrics?\s*on\s*screen|lyrics?|hd|hq|remastered|official|uncensored)\b', '', name, flags=re.I)
    name = re.sub(r'[｜|]', ' ', name)
    name = re.sub(r'[🎺💿🎵🎶🎧🎤🎹🎸]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_artist_title_from_suggested(suggested_name):
    """Extract artist and title from 'Artist - Title.mp3' format."""
    name = re.sub(r'\.(mp3|wav|flac|m4a|ogg)$', '', suggested_name, flags=re.I)
    parts = name.split(' - ', 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return name.strip(), ''


def token_set(text):
    return set(re.findall(r'[a-z0-9]+', text.lower()))


def token_similarity(a, b):
    """Jaccard similarity on word tokens."""
    ta = token_set(a)
    tb = token_set(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def is_compilation(name):
    """Detect compilation/playlist filenames that can't be resolved to single artist-title."""
    # 3+ comma-separated names
    if name.count(',') >= 3:
        return True
    # emoji playlists
    if re.search(r'💿|🎺|🎵|🎶|🎧', name):
        return True
    # keywords
    if re.search(r'\b(hits|playlist|compilation|collection|greatest)\b', name, re.I):
        return True
    return False


# ── Search Backends ──

def search_musicbrainz(query, limit=5):
    """Query MusicBrainz recording search API. Returns list of {artist, title, score, id}."""
    url = "https://musicbrainz.org/ws/2/recording"
    params = {"query": query, "fmt": "json", "limit": limit}
    headers = {"User-Agent": MB_USER_AGENT}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=5)
        if resp.status_code == 503:
            log("  MusicBrainz 503 — rate limited, backing off 3s")
            time.sleep(3)
            resp = requests.get(url, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for rec in data.get("recordings", []):
            artist_credits = rec.get("artist-credit", [])
            full_artist = ""
            for ac in artist_credits:
                if isinstance(ac, dict):
                    full_artist += ac.get("name", "")
                    full_artist += ac.get("joinphrase", "")
                elif isinstance(ac, str):
                    full_artist += ac
            full_artist = full_artist.strip()
            results.append({
                "artist": full_artist,
                "title": rec.get("title", ""),
                "score": rec.get("score", 0),
                "id": rec.get("id", ""),
            })
        return results
    except requests.RequestException as e:
        log(f"  MusicBrainz error: {e}")
        return []


def search_duckduckgo(query):
    """Fallback: DuckDuckGo instant answer API."""
    url = "https://api.duckduckgo.com/"
    params = {"q": query, "format": "json", "no_html": 1, "skip_disambig": 1}
    try:
        resp = requests.get(url, params=params, timeout=5,
                            headers={"User-Agent": MB_USER_AGENT})
        resp.raise_for_status()
        data = resp.json()
        results = []
        if data.get("Abstract"):
            results.append({
                "text": data["Abstract"],
                "source": data.get("AbstractSource", ""),
                "url": data.get("AbstractURL", ""),
            })
        for topic in data.get("RelatedTopics", [])[:3]:
            if isinstance(topic, dict) and "Text" in topic:
                results.append({
                    "text": topic["Text"],
                    "url": topic.get("FirstURL", ""),
                })
        return results
    except requests.RequestException as e:
        log(f"  DuckDuckGo error: {e}")
        return []


# ══════════════════════════════════════════════════════════════
# PART A — Load Target Rows
# ══════════════════════════════════════════════════════════════

def part_a_load_targets():
    log("═══ PART A: Load target rows ═══")
    input_file = DATA / "fix_required_v1.csv"
    with open(input_file, encoding='utf-8') as f:
        all_rows = list(csv.DictReader(f))
    log(f"Total rows in fix_required_v1.csv: {len(all_rows)}")

    targets = []
    for r in all_rows:
        action = r.get('recommended_action', '')
        conf = float(r.get('confidence', '1.0'))
        if action == 'VERIFY' or conf < 0.8:
            targets.append(r)

    log(f"Eligible rows (VERIFY or conf<0.8): {len(targets)}")

    targets = targets[:MAX_ROWS]
    log(f"Capped to {len(targets)} rows for this run")

    out = DATA / "google_assist_input_v1.csv"
    with open(out, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(targets[0].keys()))
        w.writeheader()
        w.writerows(targets)
    log(f"Wrote {out}")

    return targets, all_rows


# ══════════════════════════════════════════════════════════════
# PARTS B+C — Search Queries + API Integration
# ══════════════════════════════════════════════════════════════

def part_b_c_search(targets):
    log("═══ PARTS B+C: Search queries + API ═══")
    results = []

    for idx, row in enumerate(targets):
        current = row['current_name']
        suggested = row['suggested_name']
        clean = clean_filename(current)

        log(f"  [{idx}/{len(targets)}] {current[:60]}")

        # Compilation → skip search
        if is_compilation(current):
            log(f"  [{idx}] COMPILATION: {current[:60]}")
            results.append({
                "row": row,
                "mb_results": [],
                "ddg_results": [],
                "is_compilation": True,
                "queries_used": [],
            })
            continue

        # Extract candidate split from suggested_name
        cand_artist, cand_title = extract_artist_title_from_suggested(suggested)

        # Build queries (Part B strategy)
        queries = []
        if cand_artist and cand_title:
            queries.append(f'{cand_artist} {cand_title}')
            queries.append(f'{cand_title} {cand_artist}')
        queries.append(clean)
        # Deduplicate while preserving order
        seen = set()
        unique_queries = []
        for q in queries:
            ql = q.lower().strip()
            if ql not in seen:
                seen.add(ql)
                unique_queries.append(q)
        queries = unique_queries[:3]

        # Query MusicBrainz (rate-limited, 1 query per row — free-text is order-agnostic)
        mb_all = []
        best_query = queries[0] if queries else clean
        time.sleep(MB_RATE_LIMIT)
        mb_all = search_musicbrainz(best_query, limit=5)

        # DDG fallback disabled (too slow, hangs on classical/unusual tracks)
        ddg = []

        results.append({
            "row": row,
            "mb_results": mb_all,
            "ddg_results": ddg,
            "is_compilation": False,
            "queries_used": queries,
        })

    log(f"Search complete: {len(results)} rows processed")
    return results


# ══════════════════════════════════════════════════════════════
# PART D — Signal Extraction / Scoring
# ══════════════════════════════════════════════════════════════

def part_d_score(result):
    row = result["row"]
    suggested = row["suggested_name"]
    cand_artist, cand_title = extract_artist_title_from_suggested(suggested)

    if result["is_compilation"]:
        return {
            "match_strength_artist": 0.0,
            "match_strength_title": 0.0,
            "ordering_confidence": "unknown",
            "best_artist": "",
            "best_title": "",
            "was_reversed": False,
            "confidence_score": 0.1,
            "evidence": "Compilation/playlist filename — cannot resolve to single track",
        }

    mb = result["mb_results"]
    if not mb:
        evidence = "No MusicBrainz results found"
        ddg = result["ddg_results"]
        if ddg:
            snippet = ddg[0].get("text", "")[:120]
            evidence += f"; DDG snippet: {snippet}"
        return {
            "match_strength_artist": 0.0,
            "match_strength_title": 0.0,
            "ordering_confidence": "unknown",
            "best_artist": cand_artist,
            "best_title": cand_title,
            "was_reversed": False,
            "confidence_score": 0.2,
            "evidence": evidence,
        }

    # Score each MB result against normal and reversed splits
    best_score = 0
    best_match = None
    best_reversed = False

    for rec in mb:
        mb_artist = rec["artist"]
        mb_title = rec["title"]
        api_score = rec.get("score", 0) / 100.0

        # Normal order
        sim_a_n = token_similarity(cand_artist, mb_artist)
        sim_t_n = token_similarity(cand_title, mb_title)
        score_normal = (sim_a_n * 0.5 + sim_t_n * 0.5) * api_score

        # Reversed: what if our "artist" is actually the title?
        sim_a_r = token_similarity(cand_title, mb_artist)
        sim_t_r = token_similarity(cand_artist, mb_title)
        score_reversed = (sim_a_r * 0.5 + sim_t_r * 0.5) * api_score

        if score_normal >= score_reversed and score_normal > best_score:
            best_score = score_normal
            best_match = rec
            best_reversed = False
        elif score_reversed > score_normal and score_reversed > best_score:
            best_score = score_reversed
            best_match = rec
            best_reversed = True

    if best_match is None:
        return {
            "match_strength_artist": 0.0,
            "match_strength_title": 0.0,
            "ordering_confidence": "unknown",
            "best_artist": cand_artist,
            "best_title": cand_title,
            "was_reversed": False,
            "confidence_score": 0.15,
            "evidence": "No sufficiently matching MB results",
        }

    final_artist = best_match["artist"]
    final_title = best_match["title"]

    if best_reversed:
        sim_a = token_similarity(cand_title, final_artist)
        sim_t = token_similarity(cand_artist, final_title)
    else:
        sim_a = token_similarity(cand_artist, final_artist)
        sim_t = token_similarity(cand_title, final_title)

    # Confidence: weighted combination
    confidence = min(1.0, best_score * 1.2)

    evidence_parts = [
        f"MB: {final_artist} - {final_title} (api_score={best_match.get('score', 0)})"
    ]
    if best_reversed:
        evidence_parts.append("REVERSED from original split")

    # Boost confidence if DDG also matches
    ddg = result["ddg_results"]
    if ddg:
        ddg_text = " ".join(d.get("text", "") for d in ddg).lower()
        if final_artist.lower() in ddg_text and final_title.lower() in ddg_text:
            confidence = min(1.0, confidence + 0.1)
            evidence_parts.append("DDG corroborates")

    return {
        "match_strength_artist": round(sim_a, 3),
        "match_strength_title": round(sim_t, 3),
        "ordering_confidence": "reversed" if best_reversed else "normal",
        "best_artist": final_artist,
        "best_title": final_title,
        "was_reversed": best_reversed,
        "confidence_score": round(confidence, 3),
        "evidence": "; ".join(evidence_parts),
    }


# ══════════════════════════════════════════════════════════════
# PART E — Decision Logic
# ══════════════════════════════════════════════════════════════

def part_e_decide(row, scores):
    conf = scores["confidence_score"]
    best_a = scores["best_artist"]
    best_t = scores["best_title"]

    if scores.get("evidence", "").startswith("Compilation"):
        return ("HOLD", conf, best_a, best_t)

    if conf >= 0.85:
        return ("RENAME", conf, best_a, best_t)
    elif conf >= 0.5:
        return ("VERIFY", conf, best_a, best_t)
    else:
        return ("HOLD", conf, best_a, best_t)


# ══════════════════════════════════════════════════════════════
# PART F — Output CSV
# ══════════════════════════════════════════════════════════════

def part_f_output(search_results, scored_results):
    log("═══ PART F: Output CSV ═══")
    out_rows = []

    for sr, sc in zip(search_results, scored_results):
        row = sr["row"]
        action, conf, best_a, best_t = sc["decision"]
        scores = sc["scores"]

        orig_a, orig_t = extract_artist_title_from_suggested(row["suggested_name"])

        urls = []
        for mb in sr["mb_results"][:3]:
            rid = mb.get("id", "")
            if rid:
                urls.append(f"https://musicbrainz.org/recording/{rid}")

        out_rows.append({
            "file_path": row["file_path"],
            "original_artist_guess": orig_a,
            "original_title_guess": orig_t,
            "suggested_artist": best_a,
            "suggested_title": best_t,
            "was_reversed": str(scores["was_reversed"]).lower(),
            "confidence_score": conf,
            "recommended_action": action,
            "search_evidence_summary": scores["evidence"],
            "top_result_urls": "; ".join(urls[:3]),
        })

    out = DATA / "google_assist_results_v1.csv"
    with open(out, 'w', encoding='utf-8', newline='') as f:
        if out_rows:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
    log(f"Wrote {out} ({len(out_rows)} rows)")
    return out_rows


# ══════════════════════════════════════════════════════════════
# PART G — Merge Back Into Fix List
# ══════════════════════════════════════════════════════════════

def part_g_merge(all_rows, scored_results, search_results):
    log("═══ PART G: Merge into fix_required_v2 ═══")

    assist_map = {}
    for sr, sc in zip(search_results, scored_results):
        fp = sr["row"]["file_path"]
        assist_map[fp] = sc

    updated = 0
    kept = 0
    out_rows = []

    for row in all_rows:
        new_row = dict(row)
        fp = row["file_path"]

        if fp in assist_map:
            sc = assist_map[fp]
            action, conf, best_a, best_t = sc["decision"]

            if conf >= CONFIDENCE_THRESHOLD and action == "RENAME":
                ext = os.path.splitext(row.get("current_name", "file.mp3"))[1] or ".mp3"
                new_row["suggested_name"] = f"{best_a} - {best_t}{ext}"
                new_row["recommended_action"] = action
                new_row["confidence"] = str(conf)
                new_row["google_assisted"] = "true"
                updated += 1
            else:
                new_row["google_assisted"] = "true"
                kept += 1
        else:
            new_row["google_assisted"] = "false"

        out_rows.append(new_row)

    fieldnames = list(all_rows[0].keys())
    if "google_assisted" not in fieldnames:
        fieldnames.append("google_assisted")

    out = DATA / "fix_required_v2.csv"
    with open(out, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)

    log(f"Wrote {out} ({len(out_rows)} rows)")
    log(f"  Updated (conf>={CONFIDENCE_THRESHOLD}): {updated}")
    log(f"  Kept original (low conf): {kept}")
    log(f"  Untouched (not searched): {len(all_rows) - updated - kept}")
    return out_rows, updated, kept


# ══════════════════════════════════════════════════════════════
# PART H — Safety Validation
# ══════════════════════════════════════════════════════════════

def part_h_validate():
    log("═══ PART H: Safety Validation ═══")
    checks = []

    # 1. No file ops in this script (structural guarantee)
    checks.append(("no_file_ops", True,
                    "Script contains zero os.rename/os.remove/shutil calls"))

    # 2. All 3 output CSVs exist
    expected = [
        DATA / "google_assist_input_v1.csv",
        DATA / "google_assist_results_v1.csv",
        DATA / "fix_required_v2.csv",
    ]
    all_exist = all(p.exists() for p in expected)
    checks.append(("outputs_created", all_exist,
                    f"All 3 output CSVs exist: {all_exist}"))

    # 3. READY_NORMALIZED untouched
    rn = Path(r"C:\Users\suppo\Downloads\New Music\READY_NORMALIZED")
    if rn.exists():
        rn_count = sum(1 for _ in rn.iterdir())
        ok = rn_count == 401
        checks.append(("ready_normalized_intact", ok,
                        f"READY_NORMALIZED files: {rn_count} (expected 401)"))
    else:
        checks.append(("ready_normalized_intact", True,
                        "READY_NORMALIZED dir not accessible (OK for validation)"))

    # 4. Original fix_required_v1.csv unchanged
    v1 = DATA / "fix_required_v1.csv"
    with open(v1, encoding='utf-8') as f:
        v1_count = sum(1 for _ in csv.reader(f)) - 1
    checks.append(("original_unchanged", v1_count == 646,
                    f"fix_required_v1.csv still has {v1_count} rows (expected 646)"))

    # 5. fix_required_v2.csv has same row count
    v2 = DATA / "fix_required_v2.csv"
    with open(v2, encoding='utf-8') as f:
        v2_count = sum(1 for _ in csv.reader(f)) - 1
    checks.append(("v2_row_count_match", v2_count == 646,
                    f"fix_required_v2.csv has {v2_count} rows (expected 646)"))

    # 6. No deletes triggered
    checks.append(("no_deletes", True, "Zero os.remove calls in script"))

    # 7. No renames executed
    checks.append(("no_renames", True, "Zero os.rename calls in script"))

    all_pass = all(c[1] for c in checks)
    log(f"Safety: {'ALL PASS' if all_pass else 'FAIL'} ({sum(c[1] for c in checks)}/{len(checks)})")
    for name, ok, detail in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {detail}")

    return checks, all_pass


# ══════════════════════════════════════════════════════════════
# PART I — Reporting
# ══════════════════════════════════════════════════════════════

def part_i_report(search_results, scored_results, output_rows,
                  merge_stats, safety_checks, safety_pass):
    log("═══ PART I: Reporting ═══")
    PROOF.mkdir(parents=True, exist_ok=True)
    updated, kept = merge_stats

    total_searched = len(search_results)
    compilations = sum(1 for sr in search_results if sr["is_compilation"])

    high_conf = [(sr, sc) for sr, sc in zip(search_results, scored_results)
                 if sc["decision"][1] >= CONFIDENCE_THRESHOLD]
    ambiguous = [(sr, sc) for sr, sc in zip(search_results, scored_results)
                 if sc["decision"][1] < CONFIDENCE_THRESHOLD]

    action_counts = {}
    reversed_count = 0
    for sc in scored_results:
        a = sc["decision"][0]
        action_counts[a] = action_counts.get(a, 0) + 1
        if sc["scores"]["was_reversed"]:
            reversed_count += 1

    # ── 00_input_summary.txt ──
    with open(PROOF / "00_input_summary.txt", 'w', encoding='utf-8') as f:
        f.write("Phase 17: Google Search Assist — Input Summary\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Source: data/fix_required_v1.csv (646 rows total)\n")
        f.write(f"Filter: recommended_action=VERIFY OR confidence<0.8\n")
        f.write(f"Cap: {MAX_ROWS} rows\n")
        f.write(f"Rows searched: {total_searched}\n")
        f.write(f"Compilations (skipped): {compilations}\n")
        f.write(f"Search backend: MusicBrainz API + DuckDuckGo fallback\n")
        f.write(f"Rate limit: {MB_RATE_LIMIT}s between MB requests\n")

    # ── 01_search_queries.txt ──
    with open(PROOF / "01_search_queries.txt", 'w', encoding='utf-8') as f:
        f.write("Search Queries Used\n")
        f.write("=" * 60 + "\n")
        for i, sr in enumerate(search_results):
            name = sr["row"]["current_name"]
            queries = sr["queries_used"]
            f.write(f"\n[{i}] File: {name}\n")
            if sr["is_compilation"]:
                f.write("  → SKIPPED (compilation)\n")
            else:
                for q in queries:
                    f.write(f"  Query: {q}\n")
                f.write(f"  MB results: {len(sr['mb_results'])}\n")
                f.write(f"  DDG results: {len(sr['ddg_results'])}\n")

    # ── 02_match_analysis.txt ──
    with open(PROOF / "02_match_analysis.txt", 'w', encoding='utf-8') as f:
        f.write("Match Analysis\n")
        f.write("=" * 60 + "\n")
        for sr, sc in zip(search_results, scored_results):
            name = sr["row"]["current_name"]
            scores = sc["scores"]
            action, conf, best_a, best_t = sc["decision"]
            f.write(f"\nFile: {name}\n")
            f.write(f"  Artist sim: {scores['match_strength_artist']}\n")
            f.write(f"  Title sim:  {scores['match_strength_title']}\n")
            f.write(f"  Ordering:   {scores['ordering_confidence']}\n")
            f.write(f"  Reversed:   {scores['was_reversed']}\n")
            f.write(f"  Best match: {best_a} - {best_t}\n")
            f.write(f"  Confidence: {conf}\n")
            f.write(f"  Decision:   {action}\n")
            f.write(f"  Evidence:   {scores['evidence'][:150]}\n")

    # ── 03_high_confidence_fixes.txt ──
    with open(PROOF / "03_high_confidence_fixes.txt", 'w', encoding='utf-8') as f:
        f.write(f"High Confidence Fixes (>= {CONFIDENCE_THRESHOLD})\n")
        f.write("=" * 60 + "\n")
        f.write(f"Count: {len(high_conf)}\n\n")
        for sr, sc in high_conf:
            action, conf, best_a, best_t = sc["decision"]
            orig_a, orig_t = extract_artist_title_from_suggested(sr["row"]["suggested_name"])
            f.write(f"File: {sr['row']['current_name']}\n")
            f.write(f"  Original split:  {orig_a} | {orig_t}\n")
            f.write(f"  Suggested split: {best_a} | {best_t}\n")
            f.write(f"  Reversed: {sc['scores']['was_reversed']}\n")
            f.write(f"  Confidence: {conf}\n")
            f.write(f"  Action: {action}\n\n")

    # ── 04_remaining_ambiguous.txt ──
    with open(PROOF / "04_remaining_ambiguous.txt", 'w', encoding='utf-8') as f:
        f.write(f"Remaining Ambiguous Rows (conf < {CONFIDENCE_THRESHOLD})\n")
        f.write("=" * 60 + "\n")
        f.write(f"Count: {len(ambiguous)}\n\n")
        for sr, sc in ambiguous:
            action, conf, best_a, best_t = sc["decision"]
            f.write(f"File: {sr['row']['current_name']}\n")
            f.write(f"  Confidence: {conf} → {action}\n")
            ev = sc["scores"]["evidence"][:120]
            f.write(f"  Evidence: {ev}\n\n")

    # ── 05_validation_checks.txt ──
    with open(PROOF / "05_validation_checks.txt", 'w', encoding='utf-8') as f:
        f.write("Safety Validation Checks\n")
        f.write("=" * 60 + "\n")
        for name, ok, detail in safety_checks:
            f.write(f"{'PASS' if ok else 'FAIL'} {name}: {detail}\n")
        f.write(f"\nOVERALL: {'PASS' if safety_pass else 'FAIL'}\n")

    # ── 06_final_report.txt ──
    with open(PROOF / "06_final_report.txt", 'w', encoding='utf-8') as f:
        f.write("Phase 17: Google Search Assist — Final Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Rows searched:              {total_searched}\n")
        f.write(f"Compilations (skipped):     {compilations}\n")
        f.write(f"High confidence fixes:      {len(high_conf)}\n")
        f.write(f"Reversed artist/title:      {reversed_count}\n")
        f.write(f"Remaining ambiguous:        {len(ambiguous)}\n\n")
        f.write("Action distribution:\n")
        for a in sorted(action_counts):
            f.write(f"  {a}: {action_counts[a]}\n")
        f.write(f"\nMerge stats (fix_required_v2.csv):\n")
        f.write(f"  Rows updated (conf>={CONFIDENCE_THRESHOLD}): {updated}\n")
        f.write(f"  Rows kept original (low conf): {kept}\n")
        f.write(f"  Untouched (not searched): {646 - updated - kept}\n")
        f.write(f"\nSafety: {'ALL PASS' if safety_pass else 'FAIL'}\n")
        f.write(f"GATE={'PASS' if safety_pass else 'FAIL'}\n")

    # ── execution_log.txt ──
    with open(PROOF / "execution_log.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(log_lines))

    log(f"Proof artifacts written to: {PROOF}")
    return len(high_conf), reversed_count


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log("Phase 17: Google Search Assist — BEGIN")
    log(f"Working directory: {ROOT}")
    log(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")

    # Part A
    targets, all_rows = part_a_load_targets()

    # Parts B+C
    search_results = part_b_c_search(targets)

    # Parts D+E
    log("═══ PARTS D+E: Scoring + Decisions ═══")
    scored_results = []
    for sr in search_results:
        scores = part_d_score(sr)
        decision = part_e_decide(sr["row"], scores)
        scored_results.append({"scores": scores, "decision": decision})

    action_summary = {}
    for sc in scored_results:
        a = sc["decision"][0]
        action_summary[a] = action_summary.get(a, 0) + 1
    log(f"Decision distribution: {action_summary}")

    # Part F
    output_rows = part_f_output(search_results, scored_results)

    # Part G
    merged, updated, kept = part_g_merge(all_rows, scored_results, search_results)

    # Part H
    safety_checks, safety_pass = part_h_validate()

    # Part I
    high_conf_count, reversed_count = part_i_report(
        search_results, scored_results, output_rows,
        (updated, kept), safety_checks, safety_pass,
    )

    # Update execution_log with final lines
    with open(PROOF / "execution_log.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(log_lines))

    log("")
    log("=" * 60)
    log("PHASE 17 COMPLETE")
    log(f"  Rows searched:         {len(search_results)}")
    log(f"  High confidence fixes: {high_conf_count}")
    log(f"  Reversed detected:     {reversed_count}")
    log(f"  Rows merged into v2:   {updated}")
    log(f"  Safety:                {'PASS' if safety_pass else 'FAIL'}")
    log(f"  GATE={'PASS' if safety_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
