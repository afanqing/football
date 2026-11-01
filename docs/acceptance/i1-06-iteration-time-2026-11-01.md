# I1-06 Feature Iteration Timing Pilot

Run date: 2026-11-01 (UTC)
Repository base: `abe36aa`, with uncommitted worktree changes
Status: Modeling Lead accepted the three warm Gold-to-report ablation samples as sufficient current pilot timing evidence on 2026-11-01. Re-measure after future feature-code changes.

## Protocol

Using the existing 380-row Gold Parquet and `WalkForwardConfig::default()`, run each single-feature-group ablation sequentially through all 15 walk-forward folds (300 OOS samples). Each timed interval starts before the temporary runner reads Gold and ends after the report is written. Warm times include the Gold read, variant write, model fit/calibration, and report write. They exclude editing/implementing feature code and compiling/linking the runner.

The three hypotheses remove one existing feature group at a time by setting its values to zero: recent-five scored/conceded averages (`form`), rest-days difference (`rest`), and Elo difference (`elo`). This is a pilot for data-to-report iteration cost, not yet a representative measurement of full hypothesis-to-report work.

## Results

| Hypothesis | Start (UTC) | End (UTC) | Warm elapsed | Folds / OOS samples | RPS | Brier | Log loss | Report SHA-256 |
|---|---|---|---:|---:|---:|---:|---:|---|
| Remove recent form | 2026-11-01T18:36:16.783662608Z | 2026-11-01T18:36:17.520846559Z | 0.737 s | 15 / 300 | 0.2407445 | 0.7100467 | 3.7852825 | `757e8022c1b736fecdb64d1f7b9736087732d23fb100663436c9cfa843318c32` |
| Remove rest-days difference | 2026-11-01T18:36:17.523062435Z | 2026-11-01T18:36:18.424584753Z | 0.901 s | 15 / 300 | 0.2279265 | 0.6699222 | 3.1985565 | `526f4a363eaf79d16b41701b7d4937165bb89105da418a37169b83b8dfc209ba` |
| Remove Elo difference | 2026-11-01T18:36:18.426081219Z | 2026-11-01T18:36:19.086460052Z | 0.660 s | 15 / 300 | 0.2495575 | 0.7277774 | 3.2929972 | `03128a3aaf6bd5a14ac318c3e35c541f506c6483137cf4ccd5f1b1f628be6280` |

Median warm data-to-report time: **0.737 s**.
Baseline: 15 folds / 300 samples; RPS `0.2384652`, Brier `0.6930116`, log loss `3.2407042`.

## Build Cost and Limitations

The temporary example required a one-time `CARGO_BUILD_JOBS=2 cargo build -p football-backtest --example i1_06_ablate`, which took 7m37s for crate compilation/linking in this container. This is a cold example-link cost after the earlier workspace build, not a clean-target build of the full workspace. Do not mix it into the warm ablation median.

Reports are outside Git under `/tmp/football-validation-2026-11-01/i1-06/`: `form-report.json`, `rest-report.json`, and `elo-report.json`. The temporary runner source was removed after measurement. No production feature code was changed. Modeling Lead Cao Yuheng accepted these three ablation samples as sufficient current pilot timing evidence on 2026-11-01, closing I1-06 for this checkpoint. Re-measure after future feature-code changes, keeping code/build time separate from warm report execution.

Analyst: GitHub Copilot, 2026-11-01.
Modeling-lead approval: Cao Yuheng (曹宇恒), 2026-11-01; the three-ablation pilot is acceptable as current timing evidence. Future feature-code changes require a new timing measurement, with code/build time kept separate from warm report execution.