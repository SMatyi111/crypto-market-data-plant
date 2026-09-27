# Bybit reference capacity and lifecycle preflight

Decision: **NO-GO for activating the current 30-minute / 64 MiB combination.**
September 25 synthetic measurements, recorded September 27 against PR95 source
`539d2bfbe61349d4971a26bdbb3f2bc92e0ed289`. Existing collector operation is unchanged.

## Method and limits

Two fixed three-second profiles used the actual optional journal, generic
collector, raw sink, reference lifecycle and independent validators. Socket and
HTTP responses were synthetic, with real network/DNS/children denied. Windows'
local event-loop self-pipe was initialized before that guard. Scratch I/O used
926,084 bytes in C: temporary storage, not the production archive.

Nominal scenario: 50 depth messages/s and 10 ticker messages/s, full synthetic
ticker fields on every update. Stress: 100/20 per second plus 4 KiB synthetic
ticker padding. Bybit documents [depth-50 publication at 20 ms](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
and [derivative ticker publication at 100 ms](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker).
These scenarios are not observations or venue guarantees. Sparse ticker fields
and normal book deltas usually cost less than these sizing assumptions.

Per-frame canonical bytes were widened to six-digit journal/receipt/ordinal IDs
and 19-digit monotonic clocks. Every book was priced as a snapshot, conservatively:
1,056 bytes versus 908 for the measured delta. Tickers cost 1,539 bytes nominal
and 5,658 with padding. Projections add 4 MiB HTTP and 64 KiB control reserves;
the criterion is 20% headroom under the total cap.

| Duration | Nominal projection | Stress projection |
| --- | ---: | ---: |
| 5 minutes | 23.57 MiB | 66.65 MiB |
| 10 minutes | 43.08 MiB | 129.24 MiB |
| 30 minutes | 121.12 MiB | 379.59 MiB |

Both short runs passed session/reference integrity: 150/300 books and 30/60
tickers, no journal errors, economic admission false. Fake immediate HTTP yielded
32/31 ms post-close draining. Synthetic schedule lateness peaked at 23/27 ms.
This is not endurance evidence or a G: throughput benchmark; normalization,
replay, scheduler load and actual network delay are excluded.

The 64 MiB cap with headroom accommodates about 725 seconds nominal or 226
seconds stressed under this model. Shortening the production lane solely to fit
the journal adds rotations and is not the selected remedy. For 30 minutes, caps
of at least 151.4/474.5 MiB meet the model criterion. An explicit 512 MiB opt-in
ceiling is a candidate for offline testing, with 64 MiB retained by default.
Sidecar projections at 48 full runs/day are 5.68/17.79 GiB per day; the cap envelope
is 24 GiB/day for this one lane. Disk headroom/retention needs a separate check.

## Lifecycle and queue boundaries

A held prior finalizer caused the adjacent journal to refuse with
`previous_writer_pending`; after draining the slot was available. The current
BTC perp lane uses one 1,800-second segment per worker job, a 100,000-frame limit
and a five-second job interval. Terminal finalization can add up to its 33-second
wait ceiling (1.83% of segment duration). Replay, scheduling, connection and other
rotation costs are additional: 33 seconds is not a total intersegment-gap bound.
Switching to an endless worker without fixing handoff would lose whole evidence
segments while the previous writer owns the slot. Do not concatenate across gaps
and present the result as continuous evidence.

A boundary control also confirmed that a permitted 1 MiB HTTP body expands beyond
the 1 MiB journal queue as one base64 event. Capture refuses with `queue_overflow`
and no manifest. A larger total cap does not fix this single-record limit; preserve
fail-closed behavior or bound HTTP bodies to fit, never silently truncate them.

Three focused controls pass: sizing arithmetic, immutability of sampled evidence,
and HTTP/queue refusal. The private research-factory report retains the fixed
harness, source hashes and first machine-readable result. No historical outcome,
new market recording, live option, runner restart or transfer was involved.
The last full ops-audit stamp is unchanged by this engineering preflight.
