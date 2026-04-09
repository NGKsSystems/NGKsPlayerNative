# NGKsPlayerNative — Analysis Dashboard Panel Layout

## Overview

Instrument-style dashboard that presents analysis data in a structured,
confidence-aware layout. Read-only. No analyzer logic modified.

## Layout Zones

```
┌─────────────────────────────────────────────────────────────────────┐
│                     HEADER (state bar)                              │
│  [Track ID]                            [Analyzer State] [Duration] │
├──────────────┬─────────────────────────────┬────────────────────────┤
│  LEFT PANEL  │       CENTER (PRIMARY)       │      RIGHT PANEL      │
│              │                              │                       │
│  Global BPM  │   ┌────────────────────┐     │   Current Key         │
│  BPM Conf ●  │   │                    │     │   Key Conf ●          │
│              │   │   CURRENT BPM      │     │                       │
│  Section Cnt │   │   (instrument)     │     │   Global Key          │
│  Cue Count   │   │                    │     │   Key Change ⚑        │
│              │   └────────────────────┘     │                       │
│  Processing  │                              │   Readout State       │
│  Time        │   Current Section            │                       │
│              │   [label] (range)            │                       │
├──────────────┴─────────────────────────────┴────────────────────────┤
│                      BOTTOM STRIP                                   │
│  Section: [label] [start–end]  │  Pos: [time]  │  State: [status]  │
│  BPM Conf ●●●  Key Conf ●●●   │  Review: [reason]                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Visual Hierarchy

| Level | Elements | Size | Emphasis |
|-------|----------|------|----------|
| L1 (largest) | Current BPM | 48pt equivalent | Full brightness |
| L2 | Current Key | 28pt equivalent | Full brightness |
| L3 | Current Section, Global BPM, Global Key | 18pt | Standard |
| L4 | Confidence, Section/Cue counts, State | 12pt | Subdued |

## Confidence Tiers

| Tier | Threshold | Display |
|------|-----------|---------|
| HIGH | >= 0.75 | `███` full bar, bright |
| MEDIUM | >= 0.50 | `██░` partial bar, muted |
| LOW | < 0.50 | `█░░` minimal bar, dim/flagged |

Applies to: BPM confidence, Key confidence, Live readout confidences.

## Panel States

| State | Display |
|-------|---------|
| NO_TRACK | Empty dashboard, "No Track Loaded" message |
| NO_ANALYSIS | Track name shown, "Awaiting Analysis" |
| ANALYSIS_QUEUED | Progress: "Queued…" |
| ANALYSIS_RUNNING | Progress bar, partial values if available |
| ANALYSIS_COMPLETE | Full dashboard with all values |
| ANALYSIS_FAILED | Error message displayed |
| ANALYSIS_CANCELED | "Canceled" status |

## Data Sources

- **Global values**: from `AnalysisPanelModel` (bpm_text, key_text, etc.)
- **Current values**: from live readout properties (live_bpm_text, live_key_text, etc.)
- **No computed fields** — display only what the model provides.

## Performance

- Text-based rendering only (no bitmap drawing)
- Update at model refresh rate (≤8 Hz)
- Zero allocation per frame (reuse string buffers)
- No blocking calls
