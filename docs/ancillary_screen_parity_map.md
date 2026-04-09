# Ancillary Screen Parity Map

## Scope

This document maps the donor NGKsPlayer ancillary screens into the NGKsPlayerNative Qt implementation added for phase 1.

Donor repository used for forensics:
- _artifacts/NGKsPlayer_donor

Native target surface:
- src/ui/AncillaryScreensWidget.h
- src/ui/main.cpp

## Screen Order

1. Hardware Integration
2. Crowd Requests
3. Streaming Music Services
4. Live Streaming Setup

## Donor To Native Mapping

| Requested Screen | Donor Source | Donor Notes | Native Port Surface | Phase 1 Status |
| --- | --- | --- | --- | --- |
| Hardware Integration | _artifacts/NGKsPlayer_donor/src/hardware/HardwareIntegration.jsx | Controller scan, DVS tab, MIDI tab, settings tab | src/ui/AncillaryScreensWidget.h | Ported as native Qt page with controller, DVS, MIDI, and settings sections |
| Crowd Requests | _artifacts/NGKsPlayer_donor/src/components/RequestQueue.jsx | Queue controls, payment handles, request policy, accept/reject flow | src/ui/AncillaryScreensWidget.h | Ported as native local operator queue |
| Crowd Requests backend | _artifacts/NGKsPlayer_donor/electron/request-server.cjs | HTTP/WebSocket request server and IPC | Not directly copied | Adapted for phase 1 as an in-app local queue shell; network server deferred |
| Streaming Music Services | _artifacts/NGKsPlayer_donor/src/streaming/StreamingInterface.jsx | Service cards, search, one-click deck loading | src/ui/AncillaryScreensWidget.h plus src/ui/main.cpp track loader wiring | Ported with native service cards and library-backed search proxy |
| Live Streaming Setup | _artifacts/NGKsPlayer_donor/src/components/OBSIntegration.jsx | Software selection, theme selection, resolution, open/close broadcast window | src/ui/AncillaryScreensWidget.h | Ported with native preview window and setup guide |
| Donor route integration | _artifacts/NGKsPlayer_donor/src/main.jsx | Separate React routes for streaming and hardware | src/ui/main.cpp | Ported into one native ancillary page reachable from landing page and menu |

## Native Navigation

Entry points added in NGKsPlayerNative:
- Landing page tool button: Ancillary Screens
- Menu bar action: Ancillary Screens

Native stack placement:
- Added as a new stacked page after Tag Editor
- Uses left-rail section navigation inside the ancillary page

## Functional Parity Notes

### Hardware Integration

Donor behavior:
- Used browser-side Web MIDI access
- Presented controllers, DVS, MIDI, and settings tabs

Native phase 1 behavior:
- Scans engine-visible audio devices through EngineBridge
- Allows active audio-device switching from the ancillary screen
- Preserves donor information architecture for Controllers, DVS, MIDI, and Settings

Intentional deviation:
- Web MIDI access is not available in the native Qt shell the same way it existed in Electron/React, so phase 1 anchors the screen to actual engine device state and output switching.

### Crowd Requests

Donor behavior:
- Used Electron IPC plus a local HTTP/WebSocket request server
- Included QR generation and live queue updates

Native phase 1 behavior:
- Provides a local operator queue with manual request creation
- Preserves request policy, payment handles, accept/reject/remove controls, and queue status
- Supports promoting items from Streaming Music Services into the request queue

Intentional deviation:
- The donor network server and QR generation were not copied directly because they depend on the Electron host process and IPC surface. Phase 1 ports the operator workflow first and keeps the queue entirely native.

### Streaming Music Services

Donor behavior:
- Presented connect/disconnect cards for multiple services
- Queried external service APIs through the donor streaming controller
- Allowed one-click deck loading

Native phase 1 behavior:
- Preserves multi-service cards and operator connection state toggles
- Uses the imported native library as a search proxy until external APIs are integrated
- Loads selected results directly to Deck A or Deck B through the native main window wiring

Intentional deviation:
- External OAuth and platform search APIs are not yet wired into NGKsPlayerNative. Phase 1 keeps the donor layout and load workflow while binding search to native library content.

### Live Streaming Setup

Donor behavior:
- Focused on OBS and similar software setup
- Allowed theme selection, resolution selection, and opening a capture window

Native phase 1 behavior:
- Preserves software selection, theme selection, resolution choice, and setup instructions
- Opens a real native broadcast preview window titled NGKs Player - Broadcast Output
- Refreshes now-playing text from the native player surface

Intentional deviation:
- This is a native preview window rather than the donor Electron broadcast window. The workflow remains the same for window capture.

## Integration Summary

Implemented files:
- src/ui/AncillaryScreensWidget.h
- src/ui/main.cpp

No donor source was copied verbatim. The port mirrors the donor layout, control groupings, and operator workflow while adapting Electron-specific dependencies into native Qt behavior.
