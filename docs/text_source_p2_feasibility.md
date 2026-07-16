# Text-capture P2 source feasibility & probe plan (2026-07-16)

**Status: feasibility only.** This document evaluates the deferred P2 candidate
sources for the text-capture program (ROADMAP item 15; STANDARDS §4.6) from
primary sources as of **2026-07-16**. Nothing here builds, enables, signs up
for, or pays for anything: no collectors, no config changes, no accounts, no
API keys, no probes executed. Every probe below is a **proposed design** that
starts only on an explicit owner OK (see the ROADMAP Decision queue). Capture
rationale stays in the gitignored local request doc, per the public-safe
contract; this file covers only the capture surface itself. The X access
summary in that local doc (2026-07-12) is superseded by §6 below.

**Method.** Four independent primary-source research passes (one per source
family) on 2026-07-16, official documentation/terms/support pages only — no
third-party blogs, no news, no aggregator sites. The three most load-bearing
terms clauses (YouTube Developer Policies III.E.4(c)/(d) and III.E.1(a); X
pay-per-use pricing; the X 24-hour deletion-compliance duty) were re-verified
verbatim in a second pass. Live keyless HTTP probes were limited to confirming
that documented public endpoints exist (status code + shape only). Citations
are inline as `URL (accessed 2026-07-16[, page-dated ...])`; anything that
could not be confirmed from a primary source is listed per source under *Open
questions* rather than silently assumed.

---

## 1. Summary and recommended source order

The pre-study hypothesis was *Farcaster + official sources first, then
YouTube, then X*. Primary evidence **revises that order** — it splits the
first pair and demotes YouTube for reasons that are terms-driven, not
volume-driven:

| Order | Source family | One-line verdict |
| --- | --- | --- |
| 1 | **Official project sources** (GitHub releases, Discourse governance forums, Snapshot, project blogs) | GO to probe now: keyless, $0, terms explicitly permit API collection + archival, edits/deletes visible, no retention obligations. Zero owner prerequisites. |
| 2 | **Farcaster** | Protocol data is the cleanest of all four (no ToS, immutable casts, network-asserted arrival order) **but the assumed free public read path does not exist** — no documented keyless public Snapchain/hub endpoint. Probe needs an owner unlock first: free hosted-API account+key, or ~2 TB of dedicated node disk. |
| 3 | **YouTube** | Metadata + caption-*availability* signals only — transcript text of third-party videos is not obtainable by any permitted path. The API Developer Policies' 30-day refresh-or-delete storage rule conflicts with indefinite accrual. Parked pending an owner posture on that conflict; probe is cheap once unlocked (free API key). |
| 4 | **X** | NO-GO at current terms. Cheaper entry than previously recorded (pay-per-use $0.005/post read since 2026-02-06; full-archive search and filtered stream now available on pay-per-use) — but no free read tier, a 24-hour deletion/edit-propagation duty for offline-stored content, and redistribution caps make an append-only accruing archive non-compliant by design. Revisit only on owner initiative. |

**Why the revision.** (a) Farcaster's "free public hub HTTP reads" premise is
falsified: official docs list no public keyless endpoint — read access means
your own node (hardware the plant box does not have) or a hosted vendor free
tier (account+key = owner sign-up). (b) YouTube's usable surface shrank on
inspection: captions.download is owner-video-only, audio download and
timedtext/watch-page scraping are prohibited, and stored API data must be
deleted or refreshed after 30 days — the opposite of the plant's accrual
model. (c) The official-sources family has zero prerequisites and the
strongest terms position, so it moves first. (d) X's pricing model changed
materially in 2026 (see §6) but its compliance obligations, not its price
floor, remain the blocker.

---

## 2. Decision matrix

Detail and citations in §3–§6. `ingestion_ts` is always the plant clock and
authoritative (STANDARDS §4.6); `availability_ts` is defined in §2.1.

| Dimension | 1. Official sources | 2. Farcaster | 3. YouTube | 4. X |
| --- | --- | --- | --- | --- |
| **Authentication** | None (Atom feeds, Discourse JSON, Snapshot GraphQL, blog RSS). Optional free GitHub PAT lifts API 60→5,000 req/h | Own Snapchain node: none. Hosted (Neynar) free tier: account + API key | Free API key (Google account + Cloud project). Channel Atom feed: keyless | Developer account + prepaid credits; no keyless surface |
| **Current cost** | $0 | $0 (node hardware ≈ 2 TB disk, or hosted free tier) | $0 (quota-allocated, not billed) | $0.005/post read, $0.010/user read, $0.001/like; no free read tier |
| **Rate limits** | GitHub API 60/h unauth, 5,000/h PAT; Discourse default 200 req/min/IP; Snapshot ~60 req/min keyless | Own node: local. Neynar free: 600 RPM/endpoint, 10M credits/mo | 10,000 units/day default (+ separate search bucket 100 calls/day); captions.list costs 50 units | Per-endpoint 15-min windows (e.g. recent search 450/15 min/app); ~2M posts/mo pay-per-use cap |
| **Licensing / terms** | GitHub ToS/AUP explicitly allow API collection, research, archival. Discourse: per-forum operator terms. Snapshot: none stated | No ToS on protocol data (self-hosted read). Node software GPL-3.0. Hosted read = vendor ToS | API ToS + Developer Policies: 30-day storage rule, no audiovisual copies, no scraping, aggregation restrictions. Main ToS automation clause (regional ToS variants differ — §5.4) | Developer Agreement + Policy: 24 h deletion/edit propagation, no foundation/frontier-model training, ID-only redistribution ≤1.5M/30 d |
| **Retention / deletion / edit semantics** | Releases editable/deletable (`created_at`/`published_at`/`updated_at`, opt-in `immutable`). Discourse edits versioned, revisions public by default, 300 s silent-edit window; `deleted_at` exposed. No deletion duties on stored copies | Casts immutable (no edit op). Delete = CastRemove tombstone (content obscured on-network). Hub events prune after ~3 days; storage-unit overflow prunes oldest. No contractual duty on stored copies | Our stored API data must be **deleted or refreshed every ≤30 days** (Dev Policies III.E.4(d)) — conflicts with indefinite accrual | Stored content must track platform state; deletions/edits propagated within **24 h**; batch-compliance jobs exist. Append-only archive non-compliant |
| **Timestamps** | `source_ts` = platform-asserted `created_at`/`published_at`; edits carry `updated_at`/`version`; `availability_ts` = first observation | `source_ts` = author-claimed (backdatable; future-capped ~10–15 min — two official figures, §4.6); arrival order = hub event id (network-asserted); `availability_ts` = first event observation | `snippet.publishedAt` (semantics caveats); **caption availability_ts** = first poll where an `ASR` track appears (`captions.list`, `lastUpdated`) vs publish/stream-end ts | `created_at`; edit/delete state must be re-checked continuously (compliance duty) |
| **Expected useful volume** | Low, high-signal: ~10s/day releases+proposals, ~100s/day forum posts (no primary source — probe measures) | No primary source (whitepaper design target 9,000+ TPS network-wide); per-channel volume unknown — probe measures | ~tens of videos/day metadata for a fixed ~25-channel list (probe measures) | Keyword-dependent, 10³–10⁵ posts/day ⇒ $5–$500+/day at $0.005/read |
| **Bounded P0 probe** | 72 h keyless scratch script, conditional GET, ≤documented QPS (§3.7) | 72 h via hosted free tier (after owner unlock) or own node; events + per-channel reads (§4.7) | Two-phase: keyless Atom feed; then keyed, quota-bounded ≈8.5k units/day publish→caption-availability lag (§5.7) | None possible without spend — decision, not probe (§6.7) |
| **Go/no-go criteria** | Sustained keyless polling clean; publish-ts honesty; edit/delete visibility as documented; any useful volume (§3.8) | Read path unlocked AND channel volume above floor AND event-resume works (§4.8) | Owner accepts a 30-day-rule-compliant design first; captions.list works key-only; lag measurable within quota (§5.8) | NO-GO standing; revisit = owner budget + deletion-compliance subsystem + mutable-archive acceptance (§6.8) |

### 2.1 Timestamp vocabulary (extends STANDARDS §4.6 for P2)

- **`source_ts`** — the platform-claimed creation/publish time, preserved
  verbatim, never trusted as a clock (STANDARDS §4.6; the P1 probe's ~16 h
  stale publish-ts outlier is the standing example).
- **`ingestion_ts`** — the plant clock at capture. Authoritative time axis;
  drives `event_date` partitioning.
- **`availability_ts`** — *new for P2*: the plant-observed first-availability
  time of an item or derived artifact on the permitted read path — defined as
  the `ingestion_ts` of the poll that first observed it. Always
  plant-observed, never platform-claimed. It matters wherever an artifact
  appears materially later than its content's nominal time: a YouTube
  auto-caption track appearing hours after `publishedAt`, a Farcaster cast
  observed via the event stream, an edited forum post. For sources polled at
  interval `T`, `availability_ts` is right-censored by up to `T` — probe
  readouts must report poll cadence next to any latency percentile. For
  envelope-row sources (official sources, Farcaster casts) it is
  **derivable at read time** as min(`ingestion_ts`) per dedup key — no new
  stored field; it needs to be first-class only where the observed artifact
  has no envelope row of its own (a YouTube caption track observed while
  polling a video's metadata) or where a protocol-asserted arrival id should
  be preserved beside it (Farcaster hub event ids).
- Probes need no STANDARDS change (scratch scripts, per shop rule). Building
  any P2 lane later WOULD touch the contract: §4.6 currently defines
  `event_type` `new`/`edit` only, so Farcaster delete-tombstone events and an
  `availability_ts` envelope field are `STANDARDS_VERSION`-bump territory —
  flagged now so it is not discovered mid-build.

---

## 3. Source family 1 — official project sources (GitHub releases, Discourse governance, Snapshot, project blogs)

### 3.1 Access & authentication

- GitHub REST releases: `GET /repos/{owner}/{repo}/releases` (+ `/latest`,
  `/tags/{tag}`) — https://docs.github.com/en/rest/releases/releases
  (accessed 2026-07-16). Unauthenticated works; a free personal access token
  lifts limits (below).
- GitHub Atom feeds `releases.atom` / `tags.atom` / `commits/{branch}.atom`
  exist and serve anonymously — live probes 2026-07-16:
  `https://github.com/bitcoin/bitcoin/releases.atom` → 200
  `application/atom+xml` (same for tags/commits). They are **not documented**
  on the current REST feeds page
  (https://docs.github.com/en/rest/activity/feeds documents only `GET /feeds`)
  — functional but with no stability contract.
- Discourse: appending `.json` to public forum URLs is the supported API —
  the official OpenAPI spec states "the URL `/categories` serves a list of
  categories, the `/categories.json` API provides the same information in
  JSON format" and that some endpoints need no authentication
  (https://docs.discourse.org/openapi.json, `info.description`, accessed
  2026-07-16). Live probe: `https://meta.discourse.org/t/22706.json` → 200.
- Snapshot: GraphQL at `https://hub.snapshot.org/graphql`; API key
  **optional** (https://docs.snapshot.box/tools/api, accessed 2026-07-16).
  Live keyless probe returned 200 with proposal fields.
- Project blogs: RSS/Atom is common but not universal — live probes
  2026-07-16: `blog.ethereum.org/en/feed.xml` → 200, `solana.com/news/rss.xml`
  → 200, `medium.com/feed/offchainlabs` → 200; but `blog.uniswap.org/rss.xml`
  → 404, `blog.oplabs.co/rss/` → 404. Per-project feed discovery is a probe
  task; capture itself reuses the proven `text-rss` machinery.

### 3.2 Current cost

$0 across the family. GitHub's paid high-throughput API tier exists ("GitHub
may offer subscription-based access to our API for those Users who require
high-throughput access", ToS §H) but is irrelevant at plant polling rates.
Snapshot API keys are free by application ("wait 72 hours", key by email; no
fees mentioned — https://docs.snapshot.box/tools/api/api-keys, accessed
2026-07-16).

### 3.3 Rate limits

- GitHub API: "The primary rate limit for unauthenticated requests is 60
  requests per hour" (per originating IP); PAT-authenticated "5,000 requests
  per hour"; secondary limits include 900 points/min/endpoint
  (https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api,
  accessed 2026-07-16; confirmed live: unauthenticated `GET /rate_limit`
  returned `"core": {"limit": 60}`).
- Conditional-request caveat: "Making a conditional request does not count
  against your primary rate limit if a `304` response is returned and the
  request was made while correctly authorized with an `Authorization` header"
  (https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api,
  accessed 2026-07-16) — i.e. the 304 exemption is **authenticated-only**;
  unauthenticated conditional polling still spends the 60/h budget.
- Discourse defaults (per-forum operators can override):
  `max_reqs_per_ip_per_minute = 200`, `max_reqs_per_ip_per_10_seconds = 50`,
  mode `block`
  (https://github.com/discourse/discourse/blob/main/config/discourse_defaults.conf,
  accessed 2026-07-16).
- Snapshot: "There is a limit of 60 requests per minute" keyless; with a free
  key "2 million requests per month" (docs.snapshot.box pages above; note the
  api-keys page says 100/min keyless — a documented-figure discrepancy, plan
  on the lower 60).

### 3.4 Licensing / terms

- GitHub Acceptable Use Policies (accessed 2026-07-16): "Scraping does not
  refer to the collection of information through our API"; "Researchers may
  use public, non-personal information from the Service for research
  purposes, only if any publications resulting from that research are open
  access"; "Archivists may use public information from the Service for
  archival purposes"; prohibition is on spamming/personal-info resale and
  "excessive automated bulk activity"
  (https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies).
- GitHub ToS §H API Terms (page effective date 2026-04-27, accessed
  2026-07-16): abuse/excessive use suspendable at GitHub's discretion with
  attempted email warning; no token-sharing to exceed limits
  (https://docs.github.com/en/site-policy/github-terms/github-terms-of-service).
- **No retention/deletion obligation found** for stored public, non-personal
  project data (releases, tags) in the AUP or ToS §H.
- Discourse forums are separately operated — each instance's own terms apply;
  the platform defaults expose content anonymously by design. Snapshot docs
  state no use restrictions beyond rate limits.

### 3.5 Retention / deletion / edit semantics (upstream)

- GitHub releases: editable (`PATCH`) and deletable (`DELETE`); response
  carries `created_at`, `published_at` (nullable), `updated_at` (nullable)
  and an `immutable` boolean; immutable releases are opt-in and then only
  title/notes remain editable
  (https://docs.github.com/en/rest/releases/releases;
  https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository,
  accessed 2026-07-16). Live example (bitcoin/bitcoin latest, 2026-07-16):
  `created_at 2026-07-06T13:13:21Z`, `published_at 2026-07-08T09:14:15Z`.
- GitHub feed lookback: events-backed feeds were reduced to a **30-day
  queryable window** effective 2025-01-30
  (https://github.blog/changelog/2024-11-08-upcoming-changes-to-data-retention-for-events-api-atom-feed-timeline-and-dashboard-feed-features/,
  page-dated 2024-11-08). Irrelevant at minutes-level polling; fatal for
  backfill — consistent with the program's no-backfill posture anyway.
- Discourse posts: `created_at`, `updated_at`, `version` per post; revision
  history anonymously readable by default (`edit_history_visible_to_public:
  default: true`; live probe of a `/posts/{id}/revisions/latest.json` → 200);
  **ninja-edit window**: `editing_grace_period: default: 300` — "For (n)
  seconds after posting, editing will not create a new version" (Discourse
  `config/site_settings.yml` + server locale text, accessed 2026-07-16). So
  content can change with **no version bump** — exactly the case STANDARDS
  §4.6's semantic `content_hash` dedup already handles (a changed hash emits an
  `edit` row regardless of claimed version). Deleted posts expose
  `deleted_at`/`user_deleted` fields in payloads; anonymous-reader behavior
  for fully deleted topics is an open question (§3.6).
- Snapshot proposals: `created`/`start`/`end` (unix) plus nullable `updated`
  — mutability implied, immutability not documented either way (§3.6).

### 3.6 Open questions (probe measures / manual follow-up)

1. Whether repo `releases.atom` is covered by the 30-day events-feed window
   (the changelog names "Atom feed" without enumerating repo feeds), its
   entry-count depth, and whether the feeds share the REST API's
   unauthenticated per-IP budget (undocumented — the probe starts at
   ≤30 req/h and watches for 403/429 before assuming independence).
2. Discourse deleted-topic behavior for anonymous readers (404 vs tombstone).
3. Snapshot keyless limit 60 vs 100 req/min (two official pages disagree) and
   proposal edit rules.
4. GitHub Privacy Statement implications for personal data in feeds (commit
   author emails) — release/tag capture avoids this surface; a lane build
   should still scope fields deliberately.

### 3.7 Bounded P0 probe design (keyless, $0, no accounts)

72 h scratch script (per shop rule; not committed as a lane), all conditional
GET where supported, per-endpoint budgets far below documented limits:

- **GitHub**: poll `releases.atom` for a fixed list of ~15 major
  crypto-project repos every 30 min with `If-None-Match` (≤30 req/h — kept
  under the REST API's documented 60/h unauthenticated budget even though
  the Atom feeds are served off `github.com`, not `api.github.com`, and
  their own rate-limit regime is **undocumented** (§3.6); the probe's first
  hours confirm the effective budget before widening the repo list); 3 repos
  additionally via unauthenticated API hourly (≤3 req/h of the documented
  60/h budget) to compare Atom `updated` vs API
  `created_at`/`published_at`/`updated_at` and observe an edit if one occurs.
- **Discourse**: 3–5 major protocol governance forums (final list local, at
  enable time): `/latest.json` every 60 s + `/t/{id}.json` fetch on change;
  ≤5 req/min/forum vs the 200/min default.
- **Snapshot**: one keyless GraphQL poll/min for new/updated proposals across
  the top spaces (≤1/60th of the documented limit).
- **Blogs**: feed-discovery pass over ~10 official project blogs, then fold
  the ones that exist into the standard P1 RSS probe harness at 1–5 min.
- **Measured**: item rates per surface; `source_ts` → first-observation lag
  (this bounds `availability_ts` honesty); edit visibility (version bumps,
  revision diffs, Atom `updated` churn, semantic-hash-only changes inside the
  300 s ninja window); delete visibility; duplicate/id stability; HTTP error
  rates.

### 3.8 Go/no-go

**GO** to lane design if all hold over 72 h: no sustained 403/429 (transients
tolerated); `source_ts` honesty within poll-interval bounds (p95 lag ≤ poll
interval + 5 min, outliers diagnosed not gating — the P1 posture); edit and
delete signals observable as documented; combined volume ≥ ~20 items/day
(deliberately low bar — this family is high-signal/low-volume; below it,
fold blogs into the existing `text-rss` lane and skip dedicated
GitHub/Discourse lanes). **NO-GO** triggers: keyless paths turn out gated in
practice, or publish timestamps prove dishonest beyond diagnosis.

---

## 4. Source family 2 — Farcaster

### 4.1 Access & authentication

- Snapchain is the canonical data layer: "a peer-to-peer network of servers
  called Snapchain" (https://docs.farcaster.xyz/learn/architecture/overview,
  accessed 2026-07-16); the repo is "The open-source, canonical
  implementation of Farcaster's Snapchain network"
  (https://github.com/farcasterxyz/snapchain README, accessed 2026-07-16).
  Hubble is effectively retired (docs pages 404; app removed from the
  monorepo; last release 1.19.3 on 2025-04-28; Snapchain is "designed to be a
  drop-in replacement for Hubble" —
  https://snapchain.farcaster.xyz/guides/migrating-to-snapchain). Latest
  snapchain release v0.13.3, 2026-06-24 (github releases API).
- **Own node (keyless)**: README requires "16 GB of RAM, 4 CPU cores or
  vCPUs, 1.5TB of free storage, A public IP address, Ports 3381 - 3383
  exposed on both TCP and UDP"; the getting-started page says "2 TB of free
  storage" (plan on 2 TB) and "Snapshots are about 200 GB in size and may
  take a few hours to sync and decompress"
  (https://snapchain.farcaster.xyz/getting-started, accessed 2026-07-16). No
  API key or account anywhere in run/read instructions. **The plant box
  cannot host this** (G: is a shared 1.9 TB with ~437 GB free, and exposing
  inbound ports on the collection box is its own security decision) — a node
  means dedicated hardware.
- **No public keyless endpoint**: official docs list none — the cast-query
  guide uses only `http://localhost:3381`
  (https://docs.farcaster.xyz/developers/guides/querying/fetch-casts,
  accessed 2026-07-16). The hosted alternative (Neynar) requires an account:
  "All API endpoints require an API key"
  (https://docs.neynar.com/reference/quickstart, accessed 2026-07-16).
- Read surface (HTTP port 3381, gRPC 3383): `castsByFid`, `castsByParent`
  (by `url=` for channels), `castsByMention`, `castById`; `GET /v1/events`
  paginated from `from_event_id` and gRPC `Subscribe` with resume `from_id`
  (https://snapchain.farcaster.xyz/reference/httpapi/casts, .../httpapi/events,
  .../grpcapi/events, accessed 2026-07-16).

### 4.2 Current cost

$0 for protocol data. Own node: hardware only (≈2 TB disk + a public IP).
Neynar free tier: "10M credits/month, 10 webhooks, 5 apps, and a 600 RPM
per-API-endpoint rate limit" including "Hub endpoint" access; cheapest new
paid tier Scale $249/mo; legacy Starter $9/Growth $49 are grandfathered-only
(https://docs.neynar.com/reference/what-are-the-rate-limits-on-neynar-apis,
plan restructure dated June 2026, accessed 2026-07-16).

### 4.3 Rate limits

Own node: local, none. Neynar free tier: 600 RPM per endpoint / 10M
credits/mo (ample for a probe and a modest lane).

### 4.4 Licensing / terms

- **No ToS attaches to reading protocol data from your own node** — neither
  docs.farcaster.xyz, snapchain.farcaster.xyz, nor the protocol spec carries
  any terms document (checked 2026-07-16). Node software is GPL-3.0
  (https://raw.githubusercontent.com/farcasterxyz/snapchain/main/LICENSE);
  the protocol spec repo declares no license.
- Hosted read via Neynar = Neynar's commercial terms + key management.
- Governance note: Neynar acquired Farcaster (announced 2026-01-21: "the
  network will remain open", "no immediate product changes" —
  https://neynar.com/blog/neynar-is-acquiring-farcaster); third-party
  validators were added through 2026 (snapchain v0.11.5 2026-02-13, v0.13.3
  2026-06-24 release notes). Single-vendor concentration is a durability
  consideration, not a current blocker.

### 4.5 Retention / deletion / edit semantics (upstream)

- **No edit operation exists** — casts are add/remove only
  (CAST_ADD/CAST_REMOVE; protocol spec, accessed 2026-07-16).
- **Delete tombstones content**: "A CastRemove message ... ensures the
  message cannot be re-added while obscuring the original message's contents"
  (spec); docs: delete "removes content but leaves a tombstone placeholder"
  (https://docs.farcaster.xyz/learn/what-is-farcaster/messages). Post-delete,
  node reads no longer return the text — **capture-at-arrival is the only way
  to retain it**, and a stored copy of a later-deleted cast is a property the
  owner should sign off on explicitly (no contractual duty, but a
  privacy-norm decision).
- **Prunes**: hubs "prune events older than 3 days" (events API) — a
  collector down >3 days has an unfillable event gap (state endpoints still
  serve current casts). Storage-unit overflow prunes a user's oldest
  messages ("the message in the CRDT with the lowest timestamp-hash order is
  pruned" — spec; one unit = 5,000 casts, "$7 today, lasts for one year" —
  docs). PRUNE_MESSAGE/REVOKE_MESSAGE hub events signal this.
- Channels are `parentUrl`-keyed (`https://farcaster.xyz/~/channel/<name>`)
  and **experimental**: metadata/membership live in the client, "may be
  ported to the protocol in the future ... or they may be removed entirely"
  (https://docs.farcaster.xyz/learn/what-is-farcaster/channels); an active
  July-2026 FIP discusses moving channels on-protocol. Replies nest under
  casts (not the channel URL), so full channel capture walks reply trees —
  or filters the event firehose instead.

### 4.6 Timestamps

`data.timestamp` is **author-claimed**, "seconds since the Farcaster epoch"
(2021-01-01 00:00:00 UTC), validated only against the future ("not more than
600 seconds ahead" per spec; docs: "can be backdated by users" but not "more
than 15 minutes into the future" — **two official figures that disagree, 10
vs 15 min; plan on the spec's tighter 600 s and let the probe measure the
observed bound**). Hub event ids are network-asserted
arrival order (block height + intra-block sequence —
https://snapchain.farcaster.xyz/reference/datatypes/events). Mapping:
`source_ts` = claimed cast timestamp; `availability_ts` = first event
observation; `ingestion_ts` = plant clock. This is the same
claimed-vs-authoritative split STANDARDS §4.6 already enforces — Farcaster
just makes the claim *provably* unreliable, which the envelope tolerates by
design.

### 4.7 Bounded P0 probe design (needs one owner unlock first)

Precondition — pick a read path (Decision queue): **(a) recommended:** owner
creates a free Neynar account + API key (reversible, $0, key kept outside the
repo like `reddit_app.json`); or **(b)** dedicated node hardware (~2 TB disk,
public IP) — a bigger decision, not needed to answer the probe question; or
**(c)** defer Farcaster entirely.

Then a 72 h scratch probe (well inside 600 RPM): poll `/v1/events` (or
`castsByParent?url=` per tracked channel every 60 s) for a fixed channel
list (finalized at enable time, kept local); persist the event-id cursor;
restart the script at least twice mid-probe. **Measured**: casts/day per
channel and firehose-wide; claimed-`source_ts` vs event-arrival skew
distribution (expect backdating tails); CastRemove rate and
observed-then-deleted coverage; PRUNE/REVOKE event rates; resume-from-`from_id`
correctness across restarts; duplicate/ordering behavior.

### 4.8 Go/no-go

**GO** to lane design if: the unlocked read path sustains 72 h clean; tracked
channels carry ≥ ~500 casts/day combined (below that the lane is not worth
its ops surface — tune with the owner at readout); event-resume proves
gapless across restarts within the 3-day window. **NO-GO / defer** if the
owner declines both unlock options, or channel volume is dust, or the
channels-on-protocol FIP lands mid-flight in a way that invalidates
`parentUrl` capture (re-probe then).

---

## 5. Source family 3 — YouTube

### 5.1 Access & authentication

Data API v3 needs a Google account + Cloud project + API key ("You need a
Google Account to access the Google API Console, request an API key, and
register your application" —
https://developers.google.com/youtube/v3/getting-started, page-dated
2026-06-01, accessed 2026-07-16). OAuth is required only for
authorization-gated methods — which includes everything caption-download
related (§5.5). Keyless official surfaces: the channel Atom feed
`https://www.youtube.com/feeds/videos.xml?channel_id=...` (live probe
2026-07-16: 200, 15 entries, per-entry `published` + `updated`), which the
Data API's own push-notifications guide designates as the WebSub topic URL
(https://developers.google.com/youtube/v3/guides/push_notifications,
page-dated 2026-06-01) — an officially-acknowledged keyless
publish-timestamp source; and oEmbed (200, title/author only, no
timestamps, undocumented by Google).

### 5.2 Current cost

$0 — quota-allocated, not billed. Default: "10,000 units per day combined"
for most endpoints, with **granular buckets since 2026-06-01**: `search.list`
and `videos.insert` each get their own 100 calls/day bucket at 1 unit/call
(https://developers.google.com/youtube/v3/determine_quota_cost, page-dated
2026-06-01, accessed 2026-07-16). Extensions require a compliance audit
(https://developers.google.com/youtube/v3/guides/quota_and_compliance_audits,
page-dated 2026-06-24).

### 5.3 Rate limits / quota costs

`videos.list` / `channels.list` / `playlistItems.list` = 1 unit;
`captions.list` = 50 units; `captions.download` = 200 units; "Every API
request, even if invalid, will cost at least one quota point"
(determine_quota_cost + method pages, accessed 2026-07-16).

### 5.4 Licensing / terms — the go/no-go core

All quotes re-verified against the live pages 2026-07-16:

- **30-day storage rule** — Developer Policies III.E.4(d) (page-dated
  2026-06-24): "API Clients may temporarily store limited amounts of
  Non-Authorized Data for as long as is necessary for the purposes of the API
  Client but not longer than 30 calendar days. As in section (III.E.4.c)
  immediately above, this means that after 30 calendar days, the API Client
  must either delete or refresh the stored data."
  (https://developers.google.com/youtube/terms/developer-policies). This is
  the clause governing API-key (non-OAuth) capture. **Direct conflict with
  indefinite point-in-time accrual**: "refresh" re-fetches current state —
  which mutates or re-dates the stored copy and silently drops
  deleted/private videos, i.e. it destroys exactly the point-in-time property
  the archive exists for, and its cost grows with archive size forever.
- **No audiovisual copies** — III.E.1(a): must not "download, import,
  backup, cache, or store copies of YouTube audiovisual content without
  YouTube's prior written approval" → local transcription of downloaded
  audio is not a permitted path.
- **No scraping** — III.E.6 (must not "scrape ... or obtain scraped YouTube
  data or content") and III.I.14 (no "technology other than YouTube API
  Services to access or retrieve API Data") → the watch-page/timedtext
  route used by unofficial transcript libraries is not a permitted path.
- Main YouTube ToS (US text effective 2023-12-15): no download of Content
  except where the Service provides it or with written permission; no
  automated access "except (a) in the case of public search engines, in
  accordance with YouTube's robots.txt file; or (b) with YouTube's prior
  written permission" (https://www.youtube.com/t/terms, accessed
  2026-07-16). **Jurisdiction note**: the ToS geo-localizes; some regional
  variants of these restriction items carry an additional "where applicable
  laws allow it" exception — whether the locally-governing variant plus
  local law changes anything is a legal question for the owner (which
  variant governs stays out of this public doc), not assumed here.
- Aggregation restriction (III.E.2): API data may only be aggregated across
  "YouTube channels that are under the same content owner" (per the fetched
  text; flagged for a manual read before any lane design leans on
  cross-channel aggregates — raw per-item capture is not aggregation).
- The YouTube Researcher Program is accredited-academic-institutions-only
  (https://research.youtube/how-it-works/) — not applicable.

### 5.5 Captions — what is actually permitted

- `captions.list` (50 units) returns caption-track **metadata** only:
  `trackKind` with documented value `ASR` ("A caption track generated using
  automatic speech recognition"), `language`, `lastUpdated`, `status`
  (https://developers.google.com/youtube/v3/docs/captions, page-dated
  2026-06-01). The reference documents OAuth scopes and no explicit
  key-only path — whether it serves third-party public videos with an API
  key alone is untested (open question the probe answers first).
- `captions.download` (200 units): "This method ... requires the user to
  have permission to edit the video"; 403 otherwise
  (https://developers.google.com/youtube/v3/docs/captions/download).
  **Transcript text of third-party videos is not obtainable via the API**,
  and the non-API routes are prohibited (§5.4). Net: for third-party
  channels, the permitted surface is metadata + caption-availability
  *signals*, not transcript text.
- `videos.list contentDetails.caption` (1 unit, batched 50 ids/call) is
  documented only as "Indicates whether captions are available for the
  video" — whether ASR-only videos count as `true` is undocumented (probe
  answers empirically).
- Availability timing has **no official SLA**: "Automatic captions may not
  be ready at the time that you upload a video. Processing time depends on
  the complexity of the video's audio"; live-stream live captions do not
  persist — "New automatic captions will be generated based on the VOD
  process" (https://support.google.com/youtube/answer/6373554, accessed
  2026-07-16). So publish→caption-availability lag is exactly the thing only
  a probe can quantify.

### 5.6 Timestamps

`snippet.publishedAt` = "The date and time that the video was published"
with the documented private→public caveat (a video uploaded private then
made public carries the made-public time —
https://developers.google.com/youtube/v3/docs/videos); premiere-specific
behavior is undocumented. Live streams: `liveStreamingDetails.actualStartTime`
/ `actualEndTime` (only populated once true). Mapping: `source_ts` =
`publishedAt` (or `actualEndTime` for VOD-caption timing); **caption
`availability_ts`** = `ingestion_ts` of the first poll where an `ASR` track
appears (track `lastUpdated` preserved as a claimed cross-check);
`ingestion_ts` = plant clock.

### 5.7 Bounded P0 probe design (blocked on two owner calls)

Preconditions: (1) an owner posture on the 30-day rule (§5.8 — without it the
probe answers a question that cannot become a lane); (2) a free API key
(Google account + Cloud project = an account-creation action, owner's).

- **Phase A (keyless, could run alongside the official-sources probe)**:
  72 h conditional-GET polling of `feeds/videos.xml` for a fixed ~25-channel
  list at 15-min intervals; measures `published`→observation lag and
  `updated` churn. Kept minimal-cadence because the main-ToS automation
  clause is broad (§5.4); WebSub push is the sanctioned mechanism but needs a
  public HTTPS callback the plant box does not expose.
- **Phase B (keyed, quota-bounded worked example ≈8.5k of 10k units/day)**:
  25 channels' uploads playlists via `playlistItems.list` every 15 min
  (25 × 96 = 2,400 u) + one batched `videos.list` per 15 min (96 u; also
  records `contentDetails.caption` at 1 u to test its ASR semantics) +
  `captions.list` on ≤4 fresh videos/day, each polled every 15 min for its
  first 2 h then hourly to h 24 (30 polls × 50 u × 4 = 6,000 u). **Measures**:
  publish-ts vs caption-`availability_ts` lag distribution (per video type:
  upload vs live-VOD), whether `captions.list` works key-only (if not, the
  availability signal is dead and the family drops to publish-metadata only),
  `contentDetails.caption`-vs-ASR ground truth, quota burn vs plan.
- Probe storage itself must honor the 30-day rule (delete probe API data at
  readout ≤30 days — trivially satisfiable for a 72 h probe).

### 5.8 Go/no-go

**Precondition (owner)**: accept one of — (i) a 30-day rolling-window lane
(store API data ≤30 days; useless to the accrual program as stated), (ii) a
refresh-based design (mutates the archive — breaks the plant's append-only
contract; not recommended), (iii) an owner/legal reading under locally applicable law that
permits longer retention (not assumed), or (iv) keyless-Atom-only capture
(publish metadata without the Data API — thin but retention-clean; the
automation-clause breadth then becomes the accepted risk). Absent an
accepted posture, **NO-GO regardless of probe results**. If probed: GO
requires `captions.list` key-only to work, the lag distribution to be
measurable and stable within the 10k/day default quota, and Phase A feed
polling to run 72 h clean.

---

## 6. Source family 4 — X

### 6.1 Access & authentication

Pay-per-use credits via a developer account (console.x.com; "Go to the
Developer Console ... Accept the Developer Agreement" —
https://docs.x.com/x-api/getting-started/getting-access, accessed
2026-07-16). No keyless or free read surface.

### 6.2 Current cost

- "The X API uses pay-per-usage pricing. No subscriptions—pay only for what
  you use." Posts "$0.005 per resource"; Users "$0.010"; Likes "$0.001";
  owned reads $0.001 (effective 2026-04-20); post creation $0.015
  (https://docs.x.com/x-api/getting-started/pricing, accessed 2026-07-16;
  re-verified this session). Credits are prepaid, non-refundable, and do not
  expire (https://docs.x.com/developer-terms/ppu-agreement).
- Launched 2026-02-06: "Today, we officially launched X API Pay-Per-Use
  pricing ... Public Utility Apps continue to receive free scaled access.
  Recently active Legacy Free tier users receive a one-time $10 voucher.
  Basic and Pro plans remain available, and existing subscribers can opt in
  to Pay-Per-Use." (https://docs.x.com/changelog, accessed 2026-07-16.)
  **No free read tier for new signups.** Legacy Basic ($200/mo) was migrated
  to pay-per-use ~2026-06-01 (official devcommunity announcement — 403 to
  anonymous fetch, corroborated only via search snippets; flagged as such).
  Enterprise remains open ("Apply for Enterprise access ... Contact our
  sales team", custom pricing —
  https://docs.x.com/enterprise-api/getting-started/pricing).
- Worked cost model at $0.005/post read: 1k posts/day ≈ $150/mo; 10k/day ≈
  $1,500/mo; the pay-per-use cap ("the 2 million monthly cap on pay-per-use
  plans", stated on the Enterprise pricing page) ≈ $10k/mo equivalent.
  **Compliance re-reads compound this**: keeping stored content current
  (§6.5) implies ongoing re-hydration or batch-compliance checks whose cost
  grows with the stored corpus, i.e. total cost is unbounded over time for
  an accruing archive.

### 6.3 Rate limits

Per-endpoint 15-min windows (not tier-differentiated except Enterprise
custom): recent search 450/15 min/app; full-archive search 1/s + 300/15 min;
user timeline 10,000/15 min/app; filtered-stream connect 50/15 min
(https://docs.x.com/x-api/fundamentals/rate-limits, accessed 2026-07-16).
Endpoint availability corrected vs the 2026-07-12 internal note: recent
search "available to all developers"; **full-archive search and filtered
stream are available on pay-per-use** (full-archive "Available to
pay-per-use and Enterprise customers"; filtered stream: 1,000 rules, 1
connection on pay-per-use —
https://docs.x.com/x-api/posts/search/introduction,
.../filtered-stream/introduction, accessed 2026-07-16).

### 6.4 Licensing / terms

- **No foundation/frontier-model training**: Developer Agreement III.A(k)
  ("use the X API or X Content to fine-tune or train a foundation or
  frontier model" is prohibited; restricted-use-cases page: "with the
  exception of Grok") — Agreement "Last Updated: April 27, 2026"
  (https://docs.x.com/developer-terms/agreement,
  https://docs.x.com/developer-terms/restricted-use-cases, accessed
  2026-07-16).
- **Redistribution caps**: only IDs may be distributed; ≤1,500,000 Post IDs
  per entity per 30 days; ≤50,000 hydrated objects/recipient/day; academic
  ID-redistribution exception
  (https://docs.x.com/developer-terms/policy, .../restricted-use-cases).
- No academic/research access track exists in current docs (sitemap checked
  2026-07-16; the legacy academic page returns 402).

### 6.5 Retention / deletion / edit semantics — the blocker

Re-verified this session: "If you store X Content offline, you must keep it
up to date with the current state of that content on X", with deletion or
modification required within **24 hours** (Developer Policy "Content
compliance"; the Developer Agreement IV.B mirrors it: "in any case within
twenty four (24) hours after a written request"). Protected/suspended status
must be respected. Batch-compliance jobs (upload Post/user ID datasets,
receive current compliance status) are documented in the pay-per-use docs
with no tier gate stated
(https://docs.x.com/x-api/compliance/batch-compliance/introduction). **An
append-only point-in-time archive is non-compliant by design**: storing X
content requires a standing compliance subsystem that mutates or redacts the
stored corpus within 24 h of upstream deletions/edits — the exact opposite of
the plant's STANDARDS §4.6 semantics (edits retained as new rows, nothing
deleted).

### 6.6 Timestamps / volume

`created_at` per post; keyword-scoped volume is whatever the rules match —
10³–10⁵ posts/day is plausible for broad crypto terms, directly metered in
dollars (§6.2). No probe exists to measure this without spend.

### 6.7 / 6.8 Probe & go/no-go

No $0 probe path exists — the gate is a decision, not a measurement.
**Standing NO-GO.** Revisit requires all of: an owner budget decision
(pay-per-use spend with a cap; entry is no longer $5k/mo — that recorded
premise is corrected); a deletion-compliance subsystem design (batch
compliance jobs + a mutable or redactable curated text tier — a STANDARDS
change); and acceptance of the redistribution and model-training clauses as
compatible with intended downstream use. Absent an owner ask, no X work.

---

## 7. Owner decision points (queued in ROADMAP — none urgent)

1. **Official-sources P0 probe**: approve the 72 h keyless scratch probe
   (§3.7). $0, no accounts, no repo changes. Recommended: yes.
2. **Farcaster read path**: (a) free hosted-API account+key for the probe
   (recommended, reversible), (b) dedicated ~2 TB node hardware (defer until
   a probe justifies it), or (c) defer the source. Without one of these,
   Farcaster stays parked (§4.7).
3. **YouTube storage-rule posture** (§5.8) — decide before any key creation
   or probe; options (i)–(iv) listed there. Recommended default: (iv)
   keyless-Atom-metadata-only or defer entirely; the family's permitted
   value is thin without transcript text.
4. **X**: record the standing NO-GO (§6.7) and the corrected access facts
   (supersedes the 2026-07-12 local-doc summary). No action.

## 8. What a P2 lane build would touch (for later, not now)

Envelope and machinery reuse STANDARDS §4.6 as-is for official sources
(new/edit rows, semantic `content_hash`, per-source `instrument` slot;
`availability_ts` stays derivable at read time — §2.1). Farcaster adds
delete/prune event semantics plus preserved hub-event arrival ids, and a
YouTube caption lane would add `availability_ts` as a first-class field for
track observations — each a `STANDARDS_VERSION` bump plus a STANDARDS §4.6
extension, called out now per the hard rule. Any new lane family also means: `-CollectorConcurrency` bump in BOTH
runner scripts, exactly one promoter per lane, arg-survival regression tests
through `_run_segmented_worker` — the standing CLAUDE.md rules.
