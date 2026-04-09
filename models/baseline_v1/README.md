# Baseline Genre Classifier V1 — Model Artifacts

## STATUS: EXPERIMENTAL / NON-PRODUCTION / GENRE-ONLY BASELINE

These model files are experimental artifacts from the baseline genre classifier training.

### Files
- `random_forest_v1.pkl` — Primary Random Forest classifier (sklearn Pipeline)
- `logistic_regression_v1.pkl` — Comparison Logistic Regression classifier
- `label_encoder_v1.pkl` — LabelEncoder for genre target mapping

### Usage Warning
- These models are trained on only 200 benchmark tracks
- They are NOT suitable for production use
- They are genre-only (no subgenre)
- Performance numbers are cross-validation estimates only
- Do NOT use predictions from these models to update any database tables

### Trained on
- Dataset: data/classifier_dataset_v1.csv
- Features: harmonic_stability, loudness_lufs, avg_section_duration, tempo_stability, energy, danceability, section_count
- Target: genre (primary label)
