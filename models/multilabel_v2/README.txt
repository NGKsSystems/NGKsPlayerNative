============================================================
MULTI-LABEL CLASSIFIER V2 — MODEL ARTIFACTS
Created: 2026-04-04 23:39:01
============================================================

STATUS: EXPERIMENTAL — NON-PRODUCTION — BENCHMARK-ONLY

These model artifacts are from a benchmark-only evaluation.
They must NOT be used for production predictions.
They must NOT write predictions into production DB tables.

Files:
  ovr_random_forest_v2.pkl (3,969,172 bytes)
  imputer_v2.pkl (466 bytes)
  label_mapping_v2.json

Model: OneVsRestClassifier(RandomForestClassifier(n_estimators=200))
Labels: ['Country', 'Hip-Hop', 'Metal', 'Other', 'Pop', 'Rock']
Features: ['harmonic_stability', 'loudness_lufs', 'avg_section_duration', 'tempo_stability', 'energy', 'danceability', 'section_count']