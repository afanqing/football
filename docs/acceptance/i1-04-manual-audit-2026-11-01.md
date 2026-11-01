# I1-04 Manual Gold Audit

Run date: 2026-11-01 (UTC)
Repository base: `abe36aa`, with uncommitted worktree changes
Status: 20/20 source-to-Gold checks passed; independent reviewer sign-off and I1-03 publication-time validation remain pending.

## Method

Read the 380-match Bronze payload, Silver Parquet, and Gold Parquet. Select 20 rows evenly by chronological Gold row position, indices `0, 19, ..., 361`. For every selected row, verify source match ID, home/away names, kickoff, scores, derived outcome, and that Gold decision time precedes the Silver/Gold `result_available_at`. The implemented availability time is `kickoff + 4 hours`.

This checks source identity and feature-label consistency. It does **not** show when the provider first published each result. The four-hour delay is still an assumption; do not treat these 20 checks as closing I1-03 or proving real-world point-in-time availability. Reviewer sign-off is pending.

## Inputs

| Artifact | SHA-256 |
|---|---|
| Bronze payload | `1b046719d88b2ec5340c4a311822c1f7abea8a03c4021eb7cad0dbc67defd913` |
| Bronze manifest | `96b67ed169a091c97e0824c13d565db0d15a54e4c71210834924111f659583a4` |
| Silver Parquet | `6dde28057149bfe7e78733297b6a01c093597f09c5e053fcbef65a660005dfdd` |
| Gold Parquet | `d38cd0f97b89443d9f9d865531ba89b242e2feab2a69f387c5a376751ccd898b` |

Files are preserved outside Git under `/tmp/football-validation-2026-11-01/`.

## Sample Results

All times are UTC. Each row passed source identity, kickoff, score/outcome, and decision-before-assumed-availability checks. Every delay shown is the configured four-hour assumption, not an observed publication delay.

| # | Match ID | Kickoff | Home | Score | Away | Gold outcome |
|---:|---:|---|---|---:|---|---|
| 1 | 537785 | 2025-08-15 19:00 | Liverpool FC | 4-2 | AFC Bournemouth | home_win |
| 2 | 537803 | 2025-08-25 19:00 | Newcastle United FC | 2-3 | Liverpool FC | away_win |
| 3 | 537819 | 2025-09-14 13:00 | Burnley FC | 0-1 | Liverpool FC | away_win |
| 4 | 537835 | 2025-09-28 13:00 | Aston Villa FC | 3-1 | Fulham FC | home_win |
| 5 | 537859 | 2025-10-18 16:30 | Fulham FC | 0-1 | Arsenal FC | away_win |
| 6 | 537883 | 2025-11-01 17:30 | Tottenham Hotspur FC | 0-1 | Chelsea FC | away_win |
| 7 | 537900 | 2025-11-22 15:00 | Liverpool FC | 0-3 | Nottingham Forest FC | away_win |
| 8 | 537916 | 2025-12-03 19:30 | Brighton & Hove Albion FC | 3-4 | Aston Villa FC | away_win |
| 9 | 537939 | 2025-12-13 17:30 | Burnley FC | 2-3 | Fulham FC | away_win |
| 10 | 537963 | 2025-12-27 12:30 | Nottingham Forest FC | 1-2 | Manchester City FC | away_win |
| 11 | 537976 | 2026-01-03 12:30 | Aston Villa FC | 3-1 | Nottingham Forest FC | home_win |
| 12 | 537987 | 2026-01-08 20:00 | Arsenal FC | 0-0 | Liverpool FC | draw |
| 13 | 538007 | 2026-01-25 16:30 | Arsenal FC | 2-3 | Manchester United FC | away_win |
| 14 | 538033 | 2026-02-07 17:30 | Newcastle United FC | 2-3 | Brentford FC | away_win |
| 15 | 538045 | 2026-02-22 14:00 | Sunderland AFC | 1-3 | Fulham FC | away_win |
| 16 | 538066 | 2026-03-04 19:30 | Aston Villa FC | 1-4 | Chelsea FC | away_win |
| 17 | 538088 | 2026-03-21 17:30 | Everton FC | 3-0 | Chelsea FC | home_win |
| 18 | 538108 | 2026-04-18 19:00 | Chelsea FC | 0-1 | Manchester United FC | away_win |
| 19 | 538134 | 2026-05-02 14:00 | Wolverhampton Wanderers FC | 1-1 | Sunderland AFC | draw |
| 20 | 538152 | 2026-05-17 11:30 | Manchester United FC | 3-2 | Nottingham Forest FC | home_win |

## Review

Automated consistency check: 20 selected rows passed; 0 mismatches.
Observed first-publication time: unavailable from this historical snapshot.
Analyst: GitHub Copilot, 2026-11-01.
Independent reviewer: pending.