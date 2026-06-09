# MEXC spot v3 protobuf schema (vendored)

MEXC retired its JSON websocket (`wss://wbs.mexc.com/ws`) on **2025-08-04**. The
only public market-data endpoint now is `wss://wbs-api.mexc.com/ws`, which pushes
**binary Protocol Buffers** frames (the subscription ack and PING/PONG control
frames stay JSON text). These `.proto` files are vendored so the plant can decode
those frames through the official `protobuf` runtime instead of hand-parsing the
wire format.

## Provenance

- Upstream: <https://github.com/mexcdevelop/websocket-proto>
- `PublicAggreDealsV3Api.proto` - vendored verbatim (trades / aggregated deals).
- `PublicLimitDepthsV3Api.proto` - vendored verbatim (limit / partial-book depth).
- `PushDataV3ApiWrapper.proto` - the upstream envelope's `oneof body` lists all 15
  public+private payload types; this copy is **scoped to the two public
  market-data bodies we subscribe to** (`publicLimitDepths = 303`,
  `publicAggreDeals = 314`), with the upstream field numbers preserved verbatim.
  Decoding those two branches is byte-for-byte identical to decoding against the
  full upstream wrapper; we only ever receive these two bodies because we only
  subscribe to their channels. To add another stream, vendor its body `.proto`
  here and add its branch with the upstream field number - never renumber.

## Regenerating the Python bindings

The generated `*_pb2.py` live in `crypto_collector/collectors/mexc_pb/` and are
**committed**, so runtime needs only the `protobuf` runtime, never `protoc`.
Regenerate (dev/build-time only) after editing a `.proto`:

```powershell
pip install grpcio-tools          # provides protoc; dev-only, not a project dep
python scripts/generate_mexc_protobuf.py
```

The script runs `protoc` and rewrites the wrapper's cross-module imports to be
package-relative (`from . import ..._pb2`) so the bindings resolve inside the
`mexc_pb` package without putting the proto dir on `sys.path`.

## Pre-rollout verification gate — CLEARED (2026-06-09)

**Status:** the MEXC lanes are now enabled live in `ops.live.local.json` and are
collecting + promoting replay-clean curated data (trades + depth), so the schema is
validated end-to-end in production. The steps below are retained as the procedure to
re-run if the schema or a subscribed channel changes.

The vendored schema + the gap-detection classification were originally built from MEXC's
published docs and proto repo, **not** a live capture (the plant's dry-run/offline
constraint at the time). Before enabling a live MEXC lane in `ops.live.local.json`:

1. Capture a few real frames from `wss://wbs-api.mexc.com/ws` for
   `spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT` and
   `spot@public.limit.depth.v3.api.pb@BTCUSDT@20`.
2. Confirm `decode_mexc_frame` reproduces them (field names/numbers match) and that
   the normalized side/price/size/levels are correct.
3. Confirm `tradeType` maps `1 -> buy`, `2 -> sell` (taker/aggressor side).

If the schema can't be verified against live frames, **do not enable the live MEXC
lane** - keep it disabled and treat it as blocked on schema/live-frame verification.
