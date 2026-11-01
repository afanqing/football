# I1-07 Single Live Fetch Runbook

## Policy

All `football-ingest` CLI commands acquire the same nonblocking OS file lock before performing a live fetch:

- Lock path: `/tmp/football-live-fetch.lock`
- Contention behavior: print a generic message, make no request, and exit with status `75` (`EX_TEMPFAIL`). Do not queue or automatically retry a competing process.
- Lock lifetime: held until the CLI process exits; the kernel releases it on normal exit, error, or process termination.
- Keep the lock file in place. Do not unlink it while a process may hold the lock.
- Dagster uses the checked-in `dagster/dagster.yaml` instance configuration with `max_concurrent_runs: 1`. Configure it under the active `DAGSTER_HOME` as described in `dagster/README.md`.

The Rust lock protects direct CLI invocations and Dagster child processes alike. Dagster serialization is an additional scheduler guard; it does not replace the OS lock.

## Test Cases

Use the built binary (`target/debug/football-ingest`) so tests do not require a live provider or API token.

### TC-01: football-data.org command is rejected while locked

Hold the shared lock in another process, then run:

```sh
FOOTBALL_DATA_TOKEN=tc-dummy target/debug/football-ingest matches --competition PL
```

Pass: immediate exit status `75`; no HTTP request is made.

### TC-02: Sporttery command is rejected while locked

While the same lock is held, run:

```sh
SPORTTERY_ODDS_URL='http://127.0.0.1:1/odds' \
  target/debug/football-ingest sporttery --resource tc
```

Pass: immediate exit status `75`; no HTTP request is made.

### TC-03: lock is exclusive and released

While the first holder owns the lock, a second `flock -n /tmp/football-live-fetch.lock true` must fail immediately. After the holder exits, that command must succeed and the ingest CLI must proceed past lock acquisition. Never remove the lock file as a way to release it.

### TC-04: failure log does not disclose credentials

Set a dummy `FOOTBALL_DATA_TOKEN` and a `SPORTTERY_ODDS_URL` containing dummy userinfo and a dummy query token, pointing at a closed loopback port. Capture combined stdout/stderr. Pass: the command fails without logging the URL, dummy token, userinfo, or an `Authorization` header/value. Do not use real credentials for this test.

## Evidence Record

Run date: 2026-11-01
Implementation revision: working tree based on `abe36aa`
Commands/checks: `cargo test -p football-ingest`; `cargo build -p football-ingest`; TC-01 through TC-04 against the built binary. The contention holder was an external `flock` process holding the production lock path; both CLI commands exited `75`. A second nonblocking lock was rejected. After terminating the holder, a nonblocking lock succeeded and the CLI reached token validation. The simulated closed-port request exited `1`; its log contained the generic `URL omitted` message and none of the dummy credential markers or `Authorization` string.
Dagster serialization: configuration template added; runtime loading still requires verification on the Dagster host under I2-06.
Reviewer: GitHub Copilot, 2026-11-01. Household-host Dagster config loading remains an I2-06 check.