#pragma once

#include <cstdint>
#include <cstdio>
#include <cmath>
#include <thread>

// ═══════════════════════════════════════════════════════════════════
// WaveformState — Per-deck waveform behavior controller.
//
// Owns the waveform display state machine. Rendering reads from this;
// it does NOT invent behavior.
//
// States:  EMPTY → OVERVIEW → CUE_FOCUS → LIVE_SCROLL
// Mode:    Always LIVE — no STATIC full-track view.
// ═══════════════════════════════════════════════════════════════════

enum class WaveViewState : uint8_t {
    EMPTY,         // No track loaded
    OVERVIEW,      // Full-track overview (only on stop)
    CUE_FOCUS,     // Centered on cue/start/hot cue position (~15% of track)
    STATIC_PLAY,   // DEPRECATED — kept for compile compat, never entered
    LIVE_SCROLL    // Waveform scrolls, playhead fixed (~12s window)
};

enum class WaveUserMode : uint8_t {
    LIVE           // Play → LIVE_SCROLL (only mode)
};

inline const char* waveViewStateName(WaveViewState s) {
    switch (s) {
        case WaveViewState::EMPTY:        return "EMPTY";
        case WaveViewState::OVERVIEW:     return "OVERVIEW";
        case WaveViewState::CUE_FOCUS:    return "CUE_FOCUS";
        case WaveViewState::STATIC_PLAY:  return "STATIC_PLAY";
        case WaveViewState::LIVE_SCROLL:  return "LIVE_SCROLL";
    }
    return "UNKNOWN";
}

inline const char* waveUserModeName(WaveUserMode m) {
    switch (m) {
        case WaveUserMode::LIVE:   return "LIVE";
    }
    return "UNKNOWN";
}

// ═══════════════════════════════════════════════════════════════════
// WaveformStateController — one per deck
// ═══════════════════════════════════════════════════════════════════
class WaveformStateController {
public:
    explicit WaveformStateController(int deckIndex);

    // ── Current state queries ──
    WaveViewState state()    const { return state_; }
    WaveUserMode  userMode() const { return userMode_; }
    int           deckIndex()const { return deckIndex_; }

    bool trackLoaded()  const { return trackLoaded_; }
    bool trackPlaying() const { return trackPlaying_; }

    // Viewport anchor: fraction [0,1] of track that is the center of view
    double viewportAnchor() const { return viewportAnchor_; }

    // Cue focus target: fraction [0,1] of track
    double cueFocusTarget() const { return cueFocusTarget_; }

    // ── State transition events (called by DeckStrip) ──

    /// Track loaded into deck — enters CUE_FOCUS at start
    void onTrackLoaded(double durationSeconds);

    /// Track unloaded / deck emptied
    void onTrackUnloaded();

    /// Play pressed (or transport → Playing)
    void onPlay();

    /// Pause pressed (or transport → Paused)
    void onPause();

    /// Stop pressed (or transport → Stopped with track still loaded)
    void onStop();

    /// Cue button pressed — return to cue focus at start
    void onCuePressed();

    /// Hot cue selected — focus on specific position
    void onHotCueSelected(int hotCueIndex, double positionSeconds, double durationSeconds);

    /// User changed waveform mode preference
    void setUserMode(WaveUserMode mode);

    /// Called every poll to update viewport anchor during playback
    void updatePlayhead(double playheadSeconds, double durationSeconds);

private:
    void setState(WaveViewState newState);
    void log(const char* event, const char* extra = nullptr);

    int deckIndex_{0};
    WaveViewState state_{WaveViewState::EMPTY};
    WaveUserMode  userMode_{WaveUserMode::LIVE};

    bool trackLoaded_{false};
    bool trackPlaying_{false};

    double viewportAnchor_{0.0};   // [0,1] center of viewport
    double cueFocusTarget_{0.0};   // [0,1] cue position
    double trackDuration_{0.0};    // seconds
};
