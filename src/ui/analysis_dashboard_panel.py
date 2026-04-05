"""
NGKsPlayerNative — Analysis Dashboard Panel (Instrument-Style)
Read-only visualization layer. Presents global + live analysis values
in a structured, confidence-aware, instrument-style dashboard.

No analyzer logic modified. No playback blocking. Pure presentation.
"""

from enum import Enum
from typing import Optional

# ═══════════════════════════════════════════════════════════
#  CONFIDENCE TIER SYSTEM
# ═══════════════════════════════════════════════════════════

class ConfidenceTier(Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


def classify_confidence(value: float) -> ConfidenceTier:
    if value >= 0.75:
        return ConfidenceTier.HIGH
    elif value >= 0.50:
        return ConfidenceTier.MEDIUM
    else:
        return ConfidenceTier.LOW


def confidence_bar(value: float) -> str:
    tier = classify_confidence(value)
    if tier == ConfidenceTier.HIGH:
        return "███"
    elif tier == ConfidenceTier.MEDIUM:
        return "██░"
    else:
        return "█░░"


def confidence_label(value: float) -> str:
    tier = classify_confidence(value)
    pct = f"{value:.0%}"
    if tier == ConfidenceTier.HIGH:
        return f"{pct} {confidence_bar(value)}"
    elif tier == ConfidenceTier.MEDIUM:
        return f"{pct} {confidence_bar(value)} (moderate)"
    else:
        return f"{pct} {confidence_bar(value)} ⚠ LOW"


# ═══════════════════════════════════════════════════════════
#  DASHBOARD STATE
# ═══════════════════════════════════════════════════════════

class DashboardState(Enum):
    NO_TRACK = "NO_TRACK"
    NO_ANALYSIS = "NO_ANALYSIS"
    ANALYSIS_QUEUED = "ANALYSIS_QUEUED"
    ANALYSIS_RUNNING = "ANALYSIS_RUNNING"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    ANALYSIS_FAILED = "ANALYSIS_FAILED"
    ANALYSIS_CANCELED = "ANALYSIS_CANCELED"


_STATE_DISPLAY = {
    DashboardState.NO_TRACK: ("No Track Loaded", "─"),
    DashboardState.NO_ANALYSIS: ("Awaiting Analysis", "◌"),
    DashboardState.ANALYSIS_QUEUED: ("Queued…", "◔"),
    DashboardState.ANALYSIS_RUNNING: ("Analyzing…", "◑"),
    DashboardState.ANALYSIS_COMPLETE: ("Ready", "●"),
    DashboardState.ANALYSIS_FAILED: ("FAILED", "✗"),
    DashboardState.ANALYSIS_CANCELED: ("Canceled", "○"),
}


# ═══════════════════════════════════════════════════════════
#  ANALYSIS DASHBOARD PANEL
# ═══════════════════════════════════════════════════════════

class AnalysisDashboardPanel:
    """Instrument-style dashboard that renders analysis data.

    Consumes AnalysisPanelModel snapshot dicts.
    Produces structured text dashboard frames.
    Thread-safe read-only rendering.
    """

    def __init__(self):
        self._last_snapshot: Optional[dict] = None
        self._frame_count: int = 0

    # ────────────────────────────────────────────
    #  UPDATE
    # ────────────────────────────────────────────

    def update(self, panel_snapshot: dict) -> None:
        """Accept a new panel model snapshot. Lightweight — no heavy work."""
        self._last_snapshot = panel_snapshot
        self._frame_count += 1

    # ────────────────────────────────────────────
    #  RENDER
    # ────────────────────────────────────────────

    def render(self) -> str:
        """Render the full dashboard as a text frame.

        Returns a multi-line string ready for display or logging.
        """
        snap = self._last_snapshot
        if snap is None:
            return self._render_empty()

        state = self._resolve_state(snap)

        if state == DashboardState.NO_TRACK:
            return self._render_no_track()
        elif state == DashboardState.NO_ANALYSIS:
            return self._render_no_analysis(snap)
        elif state in (DashboardState.ANALYSIS_QUEUED, DashboardState.ANALYSIS_RUNNING):
            return self._render_running(snap, state)
        elif state == DashboardState.ANALYSIS_FAILED:
            return self._render_failed(snap)
        elif state == DashboardState.ANALYSIS_CANCELED:
            return self._render_canceled(snap)
        else:
            return self._render_complete(snap)

    def render_zone(self, zone: str) -> str:
        """Render a single zone: 'header', 'left', 'center', 'right', 'bottom'."""
        snap = self._last_snapshot
        if snap is None:
            return ""

        if zone == "header":
            return self._build_header(snap)
        elif zone == "left":
            return self._build_left_panel(snap)
        elif zone == "center":
            return self._build_center(snap)
        elif zone == "right":
            return self._build_right_panel(snap)
        elif zone == "bottom":
            return self._build_bottom_strip(snap)
        return ""

    # ────────────────────────────────────────────
    #  STATE RESOLUTION
    # ────────────────────────────────────────────

    @staticmethod
    def _resolve_state(snap: dict) -> DashboardState:
        raw = snap.get("state", "NO_TRACK")
        try:
            return DashboardState(raw)
        except ValueError:
            return DashboardState.NO_TRACK

    # ────────────────────────────────────────────
    #  EMPTY / NO_TRACK
    # ────────────────────────────────────────────

    def _render_empty(self) -> str:
        w = 70
        lines = [
            "┌" + "─" * w + "┐",
            "│" + "NGKsPlayerNative — Analysis Dashboard".center(w) + "│",
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + "No Data".center(w) + "│",
            "│" + "".center(w) + "│",
            "└" + "─" * w + "┘",
        ]
        return "\n".join(lines)

    def _render_no_track(self) -> str:
        w = 70
        lines = [
            "┌" + "─" * w + "┐",
            "│" + " ANALYSIS DASHBOARD".ljust(w) + "│",
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + "─  No Track Loaded  ─".center(w) + "│",
            "│" + "".center(w) + "│",
            "│" + "Load a track to begin analysis".center(w) + "│",
            "│" + "".center(w) + "│",
            "└" + "─" * w + "┘",
        ]
        return "\n".join(lines)

    def _render_no_analysis(self, snap: dict) -> str:
        w = 70
        track = snap.get("track_id", "Unknown") or "Unknown"
        lines = [
            "┌" + "─" * w + "┐",
            self._hdr_line(track, "◌ Awaiting Analysis", "", w),
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + "Analysis not yet available".center(w) + "│",
            "│" + "".center(w) + "│",
            "└" + "─" * w + "┘",
        ]
        return "\n".join(lines)

    # ────────────────────────────────────────────
    #  RUNNING
    # ────────────────────────────────────────────

    def _render_running(self, snap: dict, state: DashboardState) -> str:
        w = 70
        track = snap.get("track_id", "Unknown") or "Unknown"
        progress = snap.get("progress", 0.0)
        ptext = snap.get("progress_text", "")
        state_label, state_icon = _STATE_DISPLAY.get(state, ("…", "◔"))

        bar_w = 40
        filled = int(bar_w * progress / 100.0)
        bar = "█" * filled + "░" * (bar_w - filled)

        lines = [
            "┌" + "─" * w + "┐",
            self._hdr_line(track, f"{state_icon} {state_label}", "", w),
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + f"  [{bar}]  {ptext}".ljust(w) + "│",
            "│" + "".center(w) + "│",
        ]

        # Show partial values if available
        bpm = snap.get("bpm_text", "")
        key = snap.get("key_text", "")
        if bpm or key:
            lines.append("│" + f"  Partial: {bpm}  {key}".ljust(w) + "│")
        else:
            lines.append("│" + "  Waiting for first data…".ljust(w) + "│")

        lines.append("│" + "".center(w) + "│")
        lines.append("└" + "─" * w + "┘")
        return "\n".join(lines)

    # ────────────────────────────────────────────
    #  FAILED / CANCELED
    # ────────────────────────────────────────────

    def _render_failed(self, snap: dict) -> str:
        w = 70
        track = snap.get("track_id", "Unknown") or "Unknown"
        error = snap.get("error_text", "Unknown error")
        lines = [
            "┌" + "─" * w + "┐",
            self._hdr_line(track, "✗ FAILED", "", w),
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + "  ANALYSIS FAILED".ljust(w) + "│",
            "│" + f"  Error: {error[:60]}".ljust(w) + "│",
            "│" + "".center(w) + "│",
            "└" + "─" * w + "┘",
        ]
        return "\n".join(lines)

    def _render_canceled(self, snap: dict) -> str:
        w = 70
        track = snap.get("track_id", "Unknown") or "Unknown"
        lines = [
            "┌" + "─" * w + "┐",
            self._hdr_line(track, "○ Canceled", "", w),
            "├" + "─" * w + "┤",
            "│" + "".center(w) + "│",
            "│" + "  Analysis was canceled".center(w) + "│",
            "│" + "".center(w) + "│",
            "└" + "─" * w + "┘",
        ]
        return "\n".join(lines)

    # ────────────────────────────────────────────
    #  COMPLETE — FULL DASHBOARD
    # ────────────────────────────────────────────

    def _render_complete(self, snap: dict) -> str:
        w = 70
        header = self._build_header(snap)
        left = self._build_left_panel(snap)
        center = self._build_center(snap)
        right = self._build_right_panel(snap)
        bottom = self._build_bottom_strip(snap)

        # Combine panels side by side
        left_lines = left.split("\n")
        center_lines = center.split("\n")
        right_lines = right.split("\n")

        lw, cw, rw = 20, 28, 20

        # Pad to equal height
        max_h = max(len(left_lines), len(center_lines), len(right_lines))
        while len(left_lines) < max_h:
            left_lines.append("")
        while len(center_lines) < max_h:
            center_lines.append("")
        while len(right_lines) < max_h:
            right_lines.append("")

        lines = []
        lines.append("┌" + "─" * w + "┐")
        lines.append(header)
        lines.append("├" + "─" * lw + "┬" + "─" * cw + "┬" + "─" * rw + "┤")

        for i in range(max_h):
            l = left_lines[i][:lw].ljust(lw)
            c = center_lines[i][:cw].ljust(cw)
            r = right_lines[i][:rw].ljust(rw)
            lines.append(f"│{l}│{c}│{r}│")

        lines.append("├" + "─" * lw + "┴" + "─" * cw + "┴" + "─" * rw + "┤")
        for bl in bottom.split("\n"):
            lines.append("│" + bl[:w].ljust(w) + "│")
        lines.append("└" + "─" * w + "┘")

        return "\n".join(lines)

    # ────────────────────────────────────────────
    #  ZONE BUILDERS
    # ────────────────────────────────────────────

    def _build_header(self, snap: dict) -> str:
        w = 70
        track = snap.get("track_id", "") or ""
        duration = snap.get("duration_text", "")
        state = self._resolve_state(snap)
        state_label, state_icon = _STATE_DISPLAY.get(state, ("?", "?"))

        review = ""
        if snap.get("review_required"):
            review = " [REVIEW]"

        return self._hdr_line(track, f"{state_icon} {state_label}{review}", duration, w)

    def _build_left_panel(self, snap: dict) -> str:
        bpm_text = snap.get("bpm_text", "—")
        bpm_conf_raw = self._extract_bpm_conf(snap)
        bpm_conf = confidence_label(bpm_conf_raw)
        section_count = snap.get("section_count", 0)
        cue_count = snap.get("cue_count", 0)
        proc_time = snap.get("processing_time_text", "")
        ready = "YES" if snap.get("analyzer_ready") else "NO"

        lines = [
            " GLOBAL",
            f" BPM: {bpm_text}",
            f" Conf: {bpm_conf}",
            "",
            f" Sections: {section_count}",
            f" Cues:     {cue_count}",
            "",
            f" Ready: {ready}",
            f" Time: {proc_time}",
        ]
        return "\n".join(lines)

    def _build_center(self, snap: dict) -> str:
        live_bpm = snap.get("live_bpm_text", "")
        live_bpm_conf = snap.get("live_bpm_confidence", 0.0)
        live_bpm_fb = snap.get("live_bpm_is_fallback", False)
        live_section = snap.get("live_section_label", "")
        live_section_range = snap.get("live_section_time_range", "")
        live_section_idx = snap.get("live_section_index", -1)

        readout_state = snap.get("live_readout_state", "NO_TRACK")

        # BPM gauge (text-based circular visualization)
        bpm_val = self._extract_live_bpm_value(snap)
        gauge = self._bpm_gauge(bpm_val, live_bpm_conf)

        fb_tag = " (global)" if live_bpm_fb else ""

        lines = [
            "",
            gauge,
            "",
            f"  ▶ {live_bpm or '— BPM'}{fb_tag}",
            f"  Conf: {confidence_bar(live_bpm_conf)} {live_bpm_conf:.0%}",
            "",
        ]

        if live_section:
            lines.append(f"  Section: {live_section}")
            if live_section_range:
                lines.append(f"  [{live_section_range}]")
        else:
            lines.append("  Section: —")

        return "\n".join(lines)

    def _build_right_panel(self, snap: dict) -> str:
        live_key = snap.get("live_key_text", "—") or "—"
        live_key_conf = snap.get("live_key_confidence", 0.0)
        live_key_fb = snap.get("live_key_is_fallback", False)
        global_key = snap.get("key_text", "—") or "—"
        key_change = snap.get("review_reason", "")

        fb_tag = " (global)" if live_key_fb else ""
        kc_flag = " ⚑" if "Key change" in key_change else ""

        key_conf_display = confidence_label(live_key_conf)

        lines = [
            " CURRENT KEY",
            f" {live_key}{fb_tag}",
            f" Conf: {key_conf_display}",
            "",
            " GLOBAL KEY",
            f" {global_key}{kc_flag}",
            "",
            f" Readout:",
            f" {snap.get('live_readout_state', '—')}",
        ]
        return "\n".join(lines)

    def _build_bottom_strip(self, snap: dict) -> str:
        live_section = snap.get("live_section_label", "—") or "—"
        live_range = snap.get("live_section_time_range", "")
        playback_t = snap.get("live_playback_time_s", 0.0)
        readout_state = snap.get("live_readout_state", "—")
        review = snap.get("review_reason", "")

        bpm_conf_raw = self._extract_bpm_conf(snap)
        key_conf_raw = snap.get("live_key_confidence", 0.0)

        pos_str = self._fmt_time(playback_t)

        strip1 = f" Section: {live_section} {live_range}  │  Pos: {pos_str}  │  {readout_state}"
        strip2 = f" BPM Conf: {confidence_bar(bpm_conf_raw)} {bpm_conf_raw:.0%}  Key Conf: {confidence_bar(key_conf_raw)} {key_conf_raw:.0%}"
        if review:
            strip2 += f"  │  Review: {review[:40]}"

        return f"{strip1}\n{strip2}"

    # ────────────────────────────────────────────
    #  HELPERS
    # ────────────────────────────────────────────

    @staticmethod
    def _hdr_line(track: str, state: str, duration: str, w: int) -> str:
        left = f" {track[:30]}"
        right = f"{state}  {duration} "
        pad = w - len(left) - len(right)
        if pad < 1:
            pad = 1
        return "│" + left + " " * pad + right + "│"

    @staticmethod
    def _extract_bpm_conf(snap: dict) -> float:
        conf_text = snap.get("confidence_text", "")
        if "BPM conf:" in conf_text:
            try:
                part = conf_text.split("BPM conf:")[1].split("|")[0].strip()
                return float(part.replace("%", "")) / 100.0
            except (ValueError, IndexError):
                pass
        return snap.get("live_bpm_confidence", 0.0)

    @staticmethod
    def _extract_live_bpm_value(snap: dict) -> float:
        text = snap.get("live_bpm_text", "")
        if text:
            try:
                return float(text.split()[0])
            except (ValueError, IndexError):
                pass
        return 0.0

    @staticmethod
    def _bpm_gauge(bpm: float, confidence: float) -> str:
        if bpm <= 0:
            return "       ┌─────────┐\n       │  — BPM  │\n       └─────────┘"

        tier = classify_confidence(confidence)
        if tier == ConfidenceTier.HIGH:
            border = "━"
            corner_tl, corner_tr, corner_bl, corner_br = "┏", "┓", "┗", "┛"
            side = "┃"
        elif tier == ConfidenceTier.MEDIUM:
            border = "─"
            corner_tl, corner_tr, corner_bl, corner_br = "┌", "┐", "└", "┘"
            side = "│"
        else:
            border = "╌"
            corner_tl, corner_tr, corner_bl, corner_br = "┌", "┐", "└", "┘"
            side = "╎"

        bpm_str = f"{bpm:.1f}"
        inner_w = max(len(bpm_str) + 4, 10)
        top = f"  {corner_tl}{border * inner_w}{corner_tr}"
        mid = f"  {side}{bpm_str.center(inner_w)}{side}"
        bot = f"  {corner_bl}{border * inner_w}{corner_br}"
        return f"{top}\n{mid}\n{bot}"

    @staticmethod
    def _fmt_time(seconds: float) -> str:
        if seconds <= 0:
            return "0:00"
        m = int(seconds // 60)
        s = seconds % 60
        return f"{m}:{s:04.1f}"

    # ────────────────────────────────────────────
    #  ACCESSORS
    # ────────────────────────────────────────────

    @property
    def frame_count(self) -> int:
        return self._frame_count

    @property
    def current_state(self) -> DashboardState:
        if self._last_snapshot is None:
            return DashboardState.NO_TRACK
        return self._resolve_state(self._last_snapshot)

    def get_snapshot(self) -> Optional[dict]:
        return self._last_snapshot
