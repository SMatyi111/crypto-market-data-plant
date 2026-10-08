# Bybit source-trial sizing (proposal, 2026-10-08)

**Status: reviewable proposal only.** This document sizes a future bounded
source trial of the optional Bybit BTCUSDT linear depth evidence (STANDARDS
4.12-4.16). It creates no lease, flag, config change or trial, and it does not
infer authorization from the October 7/8 trials, which stay 0/2 admitted.

## Fixed facts (contract, not tunable here)

| Item | Value | Source |
| --- | --- | --- |
| Lease length, from the first committed claim | 3600 s, immutable, UTC and monotonic | STANDARDS 4.16 |
| Slots per lease | 2, nonrefundable; every committed claim is spent | STANDARDS 4.16 |
| Logical byte allowance | 2 x (512 MiB journal + 8 MiB metadata) = 1040 MiB | STANDARDS 4.16 |
| Free-space floor at each admitted start | configured floor (default 100 GiB) + 1040 MiB | STANDARDS 4.14/4.16 |
| Terminal publication wait | 2 s on the writer; stage-named since v19 | STANDARDS 4.12/4.16 |
| Prepare wait before the segment socket | 2 s | STANDARDS 4.16 |
| Worker-exit reference drain ceiling | 33 s (3 x 10 s helper + 3 s) | STANDARDS 4.13 |
| Reference helper capture lifetime | 1860 s | STANDARDS 4.13 |

## Observed overheads (October 8, run 1 and run 2)

| Interval | Observed | Note |
| --- | --- | --- |
| Claim to first raw row | ~1.4 s | writer claim, lease checkpoint, socket open, snapshot |
| Segment capture | 1800 s | the lane's `max_segment_seconds` |
| Last raw row to terminal record | ~3.1 s | sink close + two sequential public GETs via helper subprocesses |
| Terminal record to job end | ~2.6 s | 2 s wait + process teardown |
| Job end to next claim | ~9 s | ops interval 5 s + child process spawn |
| Run 1 last raw row to run 2 first raw row | 15.4 s | the raw-binding gap in the inventory |

Two full segments therefore need about `1.4 + 1800 + 3.1 + 2.6 + 9 + 1.4 + 1800`
= **3617 s before run 2's own terminal work**, already past 3600 s. Run 2 expired
with 539 ordinary rows unbound, exactly as the contract says it must.

Worst cases that must fit inside the margin, not the average: each terminal
phase can take up to the 33 s drain ceiling if a public GET hangs; the prepare
wait is 2 s; a cold start of the child process has been slower than 9 s under
load. Budget **~45 s per terminal phase** and **~20 s per gap** for sizing.

## Options

### A. Trial-scoped shorter segment on the Bybit depth lane

Set `max_segment_seconds` for the trial only. Two admitted segments need
`2 x S + gap + prepare + 2 x terminal <= 3600`:

| Segment S | Fixed need (2S) | Margin for gap + prepare + 2 terminals | Verdict |
| --- | --- | --- | --- |
| 1800 s | 3600 s | 0 s | impossible (observed) |
| 1740 s | 3480 s | 120 s (worst-case need ~112 s) | fits, thin |
| 1680 s | 3360 s | 240 s | fits with a hung-GET phase to spare |
| 1500 s | 3000 s | 600 s | comfortable; also absorbs one slow child start |

Tradeoffs: the lane's ordinary runs are shorter for the trial's duration (each
segment is an independent replay unit, so curation is unaffected, but per-run
promotion cadence and file counts change); the segment length is a lane config
read by the segmented worker at job start, so it still needs the reviewed
config path and a runner restart under the owner's restart rules; the journal
cap and 1040 MiB allowance are unchanged (an 1800 s segment used ~80 MB of the
512 MiB journal cap, so byte headroom is not the constraint).

### B. One admitted segment per trial

Keep 1800 s. The trial admits at most one segment; slot 2 is the spare for a
claim rejected before capture (prepare timeout, headroom, writer-lock
contention), never a second 1800 s capture. Tradeoff: half the evidence per
lease, and a successful first slot leaves slot 2 unusable in practice.

### C. Contract change: lease length derived from slots and segment

STANDARDS 4.16 would define the lease as `slots x segment + overhead` (for
example 2 x 1800 + 600 = 4200 s) instead of a flat 3600 s. This is an owner
decision on the contract (bump, new `SECONDS` derivation in `evidence_lease.py`,
verifier bound on journal timestamps, inventory tooling). It is not implemented
and nothing in v19 depends on it.

**Recommendation for review:** A with S = 1500 s, or B if changing the lane's
run length is unacceptable. C only if two full-length segments per lease are a
stated requirement.

## Rejected runs and spent slots

Every committed claim is spent. Claims that spend a slot without any admissible
capture, all seen or possible on this host:

- prepare timeout (2 s) or headroom refusal before the socket opens;
- cross-process writer-lock contention: the previous segment's process can hold
  the lock for up to its 33 s drain, while the next claim arrives ~9 s after job
  end. With `max_segments: 1` (one process per segment) this is a real race; a
  trial-scoped ops interval or cooldown of at least 35 s between the two
  evidence segments removes it, at the cost of a longer gap (see table above);
- terminal failure after capture (`writer_close_timeout` with its stage token,
  `LeaseRefused` after expiry, `metadata_cap`);
- an unadmitted but published manifest (any issue, a connection boundary, a
  decode error, an unanchored row).

Inventory every claim, including rejected ones, as the trial's denominator.

## Cleanup and retention

- The control directory (`lease.sqlite3`, `writer.lock`) is permanent
  spent-budget evidence outside run and offload trees; it is never reset,
  cloned or repointed.
- Trial runs live in the lane's ordinary raw tree, so promotion, quarantine and
  `archive-offload-cold` treat them like any other run; the sidecar travels with
  the run in the offload file manifest, and readers re-verify the sidecar's
  hashes after offload.
- Observed footprint per 1800 s segment: ~34 MB raw, ~80 MB journal, <10 KB
  manifest; reserved: 520 MiB per slot. Disk headroom at start: floor + 1040 MiB.
- Nothing here grants retention deletion.

## Readiness checklist before any activation scope

1. v19 merged and the live checkout pulled (collector subprocesses pick the
   code up at their next segment; the live flags currently point at the spent
   v2 lease and are refused each segment; option A's config change and any new
   lease path need the reviewed config path and a runner restart).
2. Fresh plant health OK, free space above floor + 1040 MiB.
3. A new control directory with a new trial ID, prepared by the operator
   command; the October 7 and 8 directories untouched.
4. Chosen option (A/B/C) and segment length written into the activation scope
   with the arithmetic above; the ops interval between evidence segments set
   so the previous process has released the writer lock.
5. After the lease: inventory all claims, verify any manifest offline
   (`verify_session_evidence`, `verify_bybit_references`), read
   `terminal_timing_ms` and any `terminal_stage_*` token, and record the
   verdict. No economic result scan is part of source admission.
