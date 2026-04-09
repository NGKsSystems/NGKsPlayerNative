-- ============================================================================
-- NGKsPlayerNative — Song Analysis Database Schema
-- Target: SQLite 3.x
-- Created: 2026-04-04
-- ============================================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. CORE TABLES
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tracks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path       TEXT    NOT NULL UNIQUE,
    title           TEXT,
    artist          TEXT,
    album           TEXT,
    album_artist    TEXT,
    composer        TEXT,
    year            INTEGER,
    track_number    INTEGER,
    disc_number     INTEGER,
    duration_sec    REAL,
    sample_rate     INTEGER,
    channels        INTEGER,
    bit_depth       INTEGER,
    file_format     TEXT,
    file_size_bytes INTEGER,
    file_hash_sha256 TEXT,
    imported_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS analyzer_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    analyzer_name   TEXT    NOT NULL,
    analyzer_version TEXT   NOT NULL,
    config_json     TEXT,
    started_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    finished_at     TEXT,
    status          TEXT    NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS analysis_summary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES analyzer_runs(id) ON DELETE CASCADE,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    bpm             REAL,
    bpm_confidence  REAL    CHECK (bpm_confidence IS NULL OR (bpm_confidence >= 0.0 AND bpm_confidence <= 1.0)),
    key_label       TEXT,
    key_confidence  REAL    CHECK (key_confidence IS NULL OR (key_confidence >= 0.0 AND key_confidence <= 1.0)),
    loudness_lufs   REAL,
    energy          REAL    CHECK (energy IS NULL OR (energy >= 0.0 AND energy <= 1.0)),
    danceability    REAL    CHECK (danceability IS NULL OR (danceability >= 0.0 AND danceability <= 1.0)),
    valence         REAL    CHECK (valence IS NULL OR (valence >= 0.0 AND valence <= 1.0)),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(run_id, track_id)
);

CREATE TABLE IF NOT EXISTS section_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES analyzer_runs(id) ON DELETE CASCADE,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    section_index   INTEGER NOT NULL,
    label           TEXT    NOT NULL,
    start_sec       REAL    NOT NULL,
    end_sec         REAL    NOT NULL,
    confidence      REAL    CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (end_sec > start_sec),
    UNIQUE(run_id, track_id, section_index)
);

CREATE TABLE IF NOT EXISTS cue_points (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    run_id          INTEGER REFERENCES analyzer_runs(id) ON DELETE SET NULL,
    cue_index       INTEGER NOT NULL,
    label           TEXT,
    position_sec    REAL    NOT NULL,
    type            TEXT    NOT NULL DEFAULT 'hot'
                    CHECK (type IN ('hot', 'memory', 'loop_in', 'loop_out', 'fade_in', 'fade_out')),
    color           TEXT,
    source          TEXT    NOT NULL DEFAULT 'analyzer'
                    CHECK (source IN ('analyzer', 'manual', 'import')),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ════════════════════════════════════════════════════════════════════════════
-- 2. GENRE SYSTEM
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS genres (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    description     TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS subgenres (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_id        INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL COLLATE NOCASE,
    description     TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(genre_id, name)
);

CREATE TABLE IF NOT EXISTS track_genre_labels (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    genre_id        INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    subgenre_id     INTEGER REFERENCES subgenres(id) ON DELETE SET NULL,
    role            TEXT    NOT NULL DEFAULT 'primary'
                    CHECK (role IN ('primary', 'secondary', 'candidate')),
    source          TEXT    NOT NULL DEFAULT 'manual'
                    CHECK (source IN ('manual', 'classifier', 'llm', 'rules')),
    confidence      REAL    CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)),
    applied_by      TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(track_id, genre_id, subgenre_id, role, source)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 3. TRUTH / CORRECTION LAYER
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS track_corrections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    field           TEXT    NOT NULL
                    CHECK (field IN ('bpm', 'key', 'genre', 'subgenre', 'section', 'cue', 'title', 'artist')),
    original_value  TEXT,
    corrected_value TEXT    NOT NULL,
    reason          TEXT,
    corrected_by    TEXT    NOT NULL DEFAULT 'user',
    superseded_by   INTEGER REFERENCES track_corrections(id) ON DELETE SET NULL,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ════════════════════════════════════════════════════════════════════════════
-- 4. FEATURE LAYER
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS track_features (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    run_id          INTEGER REFERENCES analyzer_runs(id) ON DELETE SET NULL,
    tempo           REAL,
    energy          REAL,
    spectral_centroid REAL,
    spectral_bandwidth REAL,
    spectral_rolloff REAL,
    zero_crossing_rate REAL,
    mfcc_mean_json  TEXT,
    chroma_mean_json TEXT,
    onset_rate      REAL,
    rms_mean        REAL,
    embedding_blob  BLOB,
    embedding_model TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(track_id, run_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 5. PREDICTION LAYER
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS genre_predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    run_id          INTEGER REFERENCES analyzer_runs(id) ON DELETE SET NULL,
    classifier_name TEXT    NOT NULL,
    classifier_version TEXT NOT NULL,
    genre_id        INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    subgenre_id     INTEGER REFERENCES subgenres(id) ON DELETE SET NULL,
    rank            INTEGER NOT NULL DEFAULT 1 CHECK (rank >= 1),
    confidence      REAL    NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(track_id, classifier_name, classifier_version, rank)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 6. LLM LAYER (ISOLATED — READ-ONLY ADVISORY)
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS llm_reviews (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    model_name      TEXT    NOT NULL,
    model_version   TEXT,
    prompt_hash     TEXT,
    request_json    TEXT,
    response_json   TEXT    NOT NULL,
    suggested_genre TEXT,
    suggested_subgenre TEXT,
    suggested_bpm   REAL,
    suggested_key   TEXT,
    notes           TEXT,
    reviewed_by     TEXT,
    review_status   TEXT    NOT NULL DEFAULT 'pending'
                    CHECK (review_status IN ('pending', 'accepted', 'rejected', 'partial')),
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ════════════════════════════════════════════════════════════════════════════
-- 7. BENCHMARKING
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS benchmark_sets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    description     TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS benchmark_set_tracks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_set_id INTEGER NOT NULL REFERENCES benchmark_sets(id) ON DELETE CASCADE,
    track_id        INTEGER NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    expected_bpm    REAL,
    expected_key    TEXT,
    expected_genre  TEXT,
    expected_sections_json TEXT,
    notes           TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(benchmark_set_id, track_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 8. VIEWS
-- ════════════════════════════════════════════════════════════════════════════

-- Effective labels: corrections override analyzer values, genre labels merged
CREATE VIEW IF NOT EXISTS v_track_effective_labels AS
SELECT
    t.id                AS track_id,
    t.file_path,
    t.title,
    t.artist,
    -- BPM: correction wins, else latest analysis
    COALESCE(
        (SELECT tc.corrected_value FROM track_corrections tc
         WHERE tc.track_id = t.id AND tc.field = 'bpm' AND tc.superseded_by IS NULL
         ORDER BY tc.created_at DESC LIMIT 1),
        CAST((SELECT a.bpm FROM analysis_summary a
              JOIN analyzer_runs r ON r.id = a.run_id
              WHERE a.track_id = t.id AND r.status = 'completed'
              ORDER BY r.finished_at DESC LIMIT 1) AS TEXT)
    ) AS effective_bpm,
    -- Key: correction wins, else latest analysis
    COALESCE(
        (SELECT tc.corrected_value FROM track_corrections tc
         WHERE tc.track_id = t.id AND tc.field = 'key' AND tc.superseded_by IS NULL
         ORDER BY tc.created_at DESC LIMIT 1),
        (SELECT a.key_label FROM analysis_summary a
         JOIN analyzer_runs r ON r.id = a.run_id
         WHERE a.track_id = t.id AND r.status = 'completed'
         ORDER BY r.finished_at DESC LIMIT 1)
    ) AS effective_key,
    -- Primary genre label
    (SELECT g.name FROM track_genre_labels tgl
     JOIN genres g ON g.id = tgl.genre_id
     WHERE tgl.track_id = t.id AND tgl.role = 'primary'
     ORDER BY tgl.created_at DESC LIMIT 1
    ) AS primary_genre,
    -- Primary subgenre label
    (SELECT sg.name FROM track_genre_labels tgl
     JOIN subgenres sg ON sg.id = tgl.subgenre_id
     WHERE tgl.track_id = t.id AND tgl.role = 'primary' AND tgl.subgenre_id IS NOT NULL
     ORDER BY tgl.created_at DESC LIMIT 1
    ) AS primary_subgenre,
    -- Genre source
    (SELECT tgl.source FROM track_genre_labels tgl
     WHERE tgl.track_id = t.id AND tgl.role = 'primary'
     ORDER BY tgl.created_at DESC LIMIT 1
    ) AS genre_source
FROM tracks t;

-- ════════════════════════════════════════════════════════════════════════════
-- 9. INDEXES
-- ════════════════════════════════════════════════════════════════════════════

-- Core lookups
CREATE INDEX IF NOT EXISTS idx_tracks_artist       ON tracks(artist);
CREATE INDEX IF NOT EXISTS idx_tracks_title         ON tracks(title);
CREATE INDEX IF NOT EXISTS idx_tracks_hash          ON tracks(file_hash_sha256);

-- Analyzer runs
CREATE INDEX IF NOT EXISTS idx_analyzer_runs_track  ON analyzer_runs(track_id);
CREATE INDEX IF NOT EXISTS idx_analyzer_runs_status ON analyzer_runs(status);
CREATE INDEX IF NOT EXISTS idx_analyzer_runs_name   ON analyzer_runs(analyzer_name, analyzer_version);

-- Analysis summary
CREATE INDEX IF NOT EXISTS idx_analysis_summary_track ON analysis_summary(track_id);
CREATE INDEX IF NOT EXISTS idx_analysis_summary_run   ON analysis_summary(run_id);

-- Sections
CREATE INDEX IF NOT EXISTS idx_section_events_track ON section_events(track_id);
CREATE INDEX IF NOT EXISTS idx_section_events_run   ON section_events(run_id);

-- Cue points
CREATE INDEX IF NOT EXISTS idx_cue_points_track     ON cue_points(track_id);

-- Genre system
CREATE INDEX IF NOT EXISTS idx_subgenres_genre      ON subgenres(genre_id);
CREATE INDEX IF NOT EXISTS idx_track_genre_labels_track ON track_genre_labels(track_id);
CREATE INDEX IF NOT EXISTS idx_track_genre_labels_genre ON track_genre_labels(genre_id);
CREATE INDEX IF NOT EXISTS idx_track_genre_labels_role  ON track_genre_labels(role, source);

-- Corrections
CREATE INDEX IF NOT EXISTS idx_corrections_track    ON track_corrections(track_id);
CREATE INDEX IF NOT EXISTS idx_corrections_field    ON track_corrections(field);

-- Features
CREATE INDEX IF NOT EXISTS idx_features_track       ON track_features(track_id);

-- Predictions
CREATE INDEX IF NOT EXISTS idx_predictions_track    ON genre_predictions(track_id);
CREATE INDEX IF NOT EXISTS idx_predictions_genre    ON genre_predictions(genre_id);
CREATE INDEX IF NOT EXISTS idx_predictions_classifier ON genre_predictions(classifier_name, classifier_version);

-- LLM
CREATE INDEX IF NOT EXISTS idx_llm_reviews_track    ON llm_reviews(track_id);
CREATE INDEX IF NOT EXISTS idx_llm_reviews_status   ON llm_reviews(review_status);

-- Benchmark
CREATE INDEX IF NOT EXISTS idx_benchmark_tracks_set ON benchmark_set_tracks(benchmark_set_id);
CREATE INDEX IF NOT EXISTS idx_benchmark_tracks_track ON benchmark_set_tracks(track_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 10. SEED DATA — GENRES
-- ════════════════════════════════════════════════════════════════════════════

INSERT OR IGNORE INTO genres (name, description) VALUES
    ('Electronic',      'Broad electronic music category'),
    ('Hip-Hop',         'Hip-hop and rap music'),
    ('Rock',            'Rock and guitar-driven music'),
    ('Pop',             'Pop and mainstream music'),
    ('R&B',             'Rhythm and blues, soul'),
    ('Jazz',            'Jazz and improvisation-based music'),
    ('Classical',       'Western art music tradition'),
    ('Country',         'Country and Americana'),
    ('Metal',           'Heavy metal and subgenres'),
    ('Reggae',          'Reggae, ska, and Caribbean-influenced music'),
    ('Latin',           'Latin-influenced genres'),
    ('Blues',           'Blues and blues-derived styles'),
    ('Folk',            'Folk and traditional acoustic music'),
    ('Funk',            'Funk groove-based music'),
    ('World',           'Non-Western traditional and fusion'),
    ('Ambient',         'Atmospheric and ambient soundscapes'),
    ('Soundtrack',      'Film, game, and media scores');

-- ════════════════════════════════════════════════════════════════════════════
-- 10b. SEED DATA — SUBGENRES
-- ════════════════════════════════════════════════════════════════════════════

-- Electronic
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Electronic'), 'House',            'Four-on-the-floor house music'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Techno',           'Detroit techno and derivatives'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Drum & Bass',     'High-tempo breakbeat bass music'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Dubstep',         'Half-time wobble bass'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Trance',          'Uplifting and progressive trance'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Electro',         'Electro and electro-funk'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Downtempo',       'Chill and downtempo electronic'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Garage',          'UK garage and 2-step'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'Breakbeat',       'Breakbeat and big beat'),
    ((SELECT id FROM genres WHERE name='Electronic'), 'IDM',             'Intelligent dance music');

-- Hip-Hop
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Hip-Hop'), 'Trap',              'Trap beats and 808s'),
    ((SELECT id FROM genres WHERE name='Hip-Hop'), 'Boom Bap',          'Classic East Coast hip-hop'),
    ((SELECT id FROM genres WHERE name='Hip-Hop'), 'Lo-Fi Hip-Hop',     'Chilled, lo-fi beats'),
    ((SELECT id FROM genres WHERE name='Hip-Hop'), 'Drill',             'UK/Chicago drill'),
    ((SELECT id FROM genres WHERE name='Hip-Hop'), 'G-Funk',            'West Coast funk-rap');

-- Rock
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Rock'), 'Alternative',          'Alternative and indie rock'),
    ((SELECT id FROM genres WHERE name='Rock'), 'Prog Rock',            'Progressive rock'),
    ((SELECT id FROM genres WHERE name='Rock'), 'Punk',                 'Punk rock and post-punk'),
    ((SELECT id FROM genres WHERE name='Rock'), 'Classic Rock',         '60s–80s rock canon'),
    ((SELECT id FROM genres WHERE name='Rock'), 'Grunge',               '90s Seattle grunge'),
    ((SELECT id FROM genres WHERE name='Rock'), 'Shoegaze',             'Wall-of-sound guitar textures');

-- Pop
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Pop'), 'Synth-Pop',             'Synth-driven pop'),
    ((SELECT id FROM genres WHERE name='Pop'), 'Indie Pop',             'Independent pop aesthetics'),
    ((SELECT id FROM genres WHERE name='Pop'), 'K-Pop',                 'Korean pop music'),
    ((SELECT id FROM genres WHERE name='Pop'), 'Dance-Pop',             'Pop with dance beats'),
    ((SELECT id FROM genres WHERE name='Pop'), 'Electropop',            'Electronic-infused pop');

-- R&B
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='R&B'), 'Neo-Soul',              'Modern soul and R&B'),
    ((SELECT id FROM genres WHERE name='R&B'), 'Contemporary R&B',      'Modern R&B production'),
    ((SELECT id FROM genres WHERE name='R&B'), 'Quiet Storm',           'Smooth, romantic R&B');

-- Jazz
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Jazz'), 'Bebop',                'Fast, complex jazz improvisation'),
    ((SELECT id FROM genres WHERE name='Jazz'), 'Smooth Jazz',          'Accessible, polished jazz'),
    ((SELECT id FROM genres WHERE name='Jazz'), 'Acid Jazz',            'Jazz-funk-dance fusion'),
    ((SELECT id FROM genres WHERE name='Jazz'), 'Jazz Fusion',          'Jazz-rock crossover');

-- Metal
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Metal'), 'Death Metal',         'Extreme metal with growls'),
    ((SELECT id FROM genres WHERE name='Metal'), 'Black Metal',         'Tremolo-picked extreme metal'),
    ((SELECT id FROM genres WHERE name='Metal'), 'Thrash Metal',        'Fast aggressive metal'),
    ((SELECT id FROM genres WHERE name='Metal'), 'Doom Metal',          'Slow, heavy, dark metal'),
    ((SELECT id FROM genres WHERE name='Metal'), 'Progressive Metal',   'Complex, technical metal');

-- Latin
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Latin'), 'Reggaeton',           'Latin urban beats'),
    ((SELECT id FROM genres WHERE name='Latin'), 'Salsa',               'Salsa and salsa dura'),
    ((SELECT id FROM genres WHERE name='Latin'), 'Bachata',             'Dominican romantic style'),
    ((SELECT id FROM genres WHERE name='Latin'), 'Cumbia',              'Colombian-origin dance music');

-- Reggae
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Reggae'), 'Dub',                'Echo-heavy dub production'),
    ((SELECT id FROM genres WHERE name='Reggae'), 'Dancehall',          'Dancehall reggae'),
    ((SELECT id FROM genres WHERE name='Reggae'), 'Ska',                'Uptempo Jamaican ska');

-- Country
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Country'), 'Outlaw Country',    'Outlaw and alt-country'),
    ((SELECT id FROM genres WHERE name='Country'), 'Bluegrass',         'Acoustic string-band music'),
    ((SELECT id FROM genres WHERE name='Country'), 'Country Pop',       'Pop-influenced country');

-- Ambient
INSERT OR IGNORE INTO subgenres (genre_id, name, description) VALUES
    ((SELECT id FROM genres WHERE name='Ambient'), 'Dark Ambient',      'Ominous atmospheric textures'),
    ((SELECT id FROM genres WHERE name='Ambient'), 'Space Ambient',     'Cosmic, spacious drones'),
    ((SELECT id FROM genres WHERE name='Ambient'), 'Drone',             'Sustained-tone compositions');

-- ════════════════════════════════════════════════════════════════════════════
-- END OF SCHEMA
-- ════════════════════════════════════════════════════════════════════════════
