#include "WaveformState.h"

// ═══════════════════════════════════════════════════════════════════
// WaveformStateController — implementation
// ═══════════════════════════════════════════════════════════════════

WaveformStateController::WaveformStateController(int deckIndex)
    : deckIndex_(deckIndex) {}

// ── Logging helper — immediate flush ──
void WaveformStateController::log(const char* event, const char* extra)
{
    auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    if (extra) {
        std::fprintf(stderr, "%s deck=%d state=%s mode=%s anchor=%.4f cue=%.4f tid=%zu %s\n",
            event, deckIndex_,
            waveViewStateName(state_), waveUserModeName(userMode_),
            viewportAnchor_, cueFocusTarget_,
            static_cast<size_t>(tid), extra);
    } else {
        std::fprintf(stderr, "%s deck=%d state=%s mode=%s anchor=%.4f cue=%.4f tid=%zu\n",
            event, deckIndex_,
            waveViewStateName(state_), waveUserModeName(userMode_),
            viewportAnchor_, cueFocusTarget_,
            static_cast<size_t>(tid));
    }
    std::fflush(stderr);
}

// ── State transition with logging ──
void WaveformStateController::setState(WaveViewState newState)
{
    if (newState == state_) return;
    const char* oldName = waveViewStateName(state_);
    state_ = newState;

    char buf[128];
    std::snprintf(buf, sizeof(buf), "old=%s new=%s", oldName, waveViewStateName(state_));
    log("WAVE_STATE_SET", buf);
}

// ═══════════════════════════════════════════════════════════════════
// State transition events
// ═══════════════════════════════════════════════════════════════════

void WaveformStateController::onTrackLoaded(double durationSeconds)
{
    trackLoaded_ = true;
    trackPlaying_ = false;
    trackDuration_ = durationSeconds;
    cueFocusTarget_ = 0.0;   // start of track
    viewportAnchor_ = 0.0;
    setState(WaveViewState::CUE_FOCUS);
    log("WAVE_CUE_FOCUS", "reason=track_loaded");
}

void WaveformStateController::onTrackUnloaded()
{
    trackLoaded_ = false;
    trackPlaying_ = false;
    trackDuration_ = 0.0;
    cueFocusTarget_ = 0.0;
    viewportAnchor_ = 0.0;
    setState(WaveViewState::EMPTY);
    log("WAVE_OVERVIEW", "reason=track_unloaded→EMPTY");
}

void WaveformStateController::onPlay()
{
    if (!trackLoaded_) return;
    trackPlaying_ = true;

    // Always LIVE — no STATIC full-track mode.
    setState(WaveViewState::LIVE_SCROLL);
    log("WAVE_PLAY_MODE_LIVE");
}

void WaveformStateController::onPause()
{
    if (!trackLoaded_) return;
    trackPlaying_ = false;
    // Hold current state — do NOT change waveform view on pause
    log("WAVE_PAUSE_HOLD");
}

void WaveformStateController::onStop()
{
    if (!trackLoaded_) return;
    trackPlaying_ = false;
    viewportAnchor_ = 0.0;
    // Return to overview when stopped (track still loaded)
    setState(WaveViewState::OVERVIEW);
    log("WAVE_OVERVIEW", "reason=stop");
}

void WaveformStateController::onCuePressed()
{
    if (!trackLoaded_) return;
    cueFocusTarget_ = 0.0;   // main cue = start of track
    viewportAnchor_ = 0.0;
    setState(WaveViewState::CUE_FOCUS);
    log("WAVE_CUE_FOCUS", "reason=cue_pressed");
}

void WaveformStateController::onHotCueSelected(int hotCueIndex, double positionSeconds,
                                                 double durationSeconds)
{
    if (!trackLoaded_ || durationSeconds <= 0.0) return;
    const double frac = positionSeconds / durationSeconds;
    cueFocusTarget_ = frac;
    viewportAnchor_ = frac;
    setState(WaveViewState::CUE_FOCUS);

    char buf[128];
    std::snprintf(buf, sizeof(buf), "reason=hotcue hotcue=%d pos=%.2f",
                  hotCueIndex, positionSeconds);
    log("WAVE_HOTCUE_FOCUS", buf);
}

void WaveformStateController::setUserMode(WaveUserMode mode)
{
    if (mode == userMode_) return;
    userMode_ = mode;
    log("WAVE_MODE_SET");

    // If currently playing, stay in LIVE — no STATIC fallback.
    if (trackPlaying_) {
        setState(WaveViewState::LIVE_SCROLL);
        log("WAVE_PLAY_MODE_LIVE", "reason=mode_switch");
    }
}

void WaveformStateController::updatePlayhead(double playheadSeconds, double durationSeconds)
{
    if (!trackLoaded_ || durationSeconds <= 0.0) return;
    viewportAnchor_ = playheadSeconds / durationSeconds;
}
