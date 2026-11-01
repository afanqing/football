# I1-02 Daily Fetch Observation

## Window and denominator

- Observation window: 2026-11-02 through 2026-11-06, Asia/Shanghai calendar dates.
- Schedule: one Premier League football-data.org fetch at 08:00 Asia/Shanghai per date, covering that date and the prior 7 days.
- Denominator: exactly five scheduled attempts. Provider outages, command failures, missed dates, and interrupted runs count as failures; no same-day retry is performed.
- This is a user-container process, not a system service. It runs only while the current container remains alive and does not automatically recover after a container restart. A missed attempt must be recorded as such; do not silently restart and count it as a success.

## Deployment

The observer script is stored outside the Git worktree at `/home/coder/.local/bin/football-daily-fetch-observer`. It reads `FOOTBALL_DATA_TOKEN` from the repository's ignored `.env` file and requires mode `0600`; it never prints the token. Fetch artifacts and logs are stored outside Git under `/home/coder/football-validation/daily-fetch/`, with directory mode `0700` and file mode `0600`.

The observer was started on 2026-11-02 at 03:12 Asia/Shanghai as PID `228095` in a detached session. Its PID is recorded at `/home/coder/football-validation/daily-fetch/observer.pid`; `scheduler.lock` prevents duplicate observers. The observer writes `STARTED` and `FINISHED` events with UTC timestamps, status, exit code, payload SHA-256, and per-attempt log path to `attempts.csv`. CLI exit code 75 is recorded as `skipped_locked`; it is not retried and counts as a failed scheduled attempt. Each daily command's stdout/stderr is captured in `logs/YYYY-MM-DD.log`. Successful immutable payloads and manifests are written below `bronze/football-data.org/captured_date=.../`.

These once-daily snapshots are for I1-02 scheduled reliability only. They do not prove when a match first became `FINISHED` or validate the `kickoff + 4 hours` assumption. A separate I1-03 plan for 15-minute polling around relevant result windows is proposed but not deployed; confirm provider/request budget and approve its observation window before starting it.

## Review

After the final scheduled date, verify all five dates have exactly one terminal outcome (`FINISHED` or `MISSED`). Calculate the success rate as successful `FINISHED` events with status `success` divided by five; `skipped_locked` is a failure. Do not infer acceptance from the schedule declaration or a single successful fetch. Preserve the CSV, per-day logs, manifests, payload hashes, and the final container/process status as evidence. Review logs before sharing; never copy credentials or credential-bearing environment output into Git or chat.