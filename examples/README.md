# Restaurant Wine Reference — synthetic implementation

This is an illustrative implementation with fictional data, authored for offline engineering demonstrations. It is not an employer application, production deployment, or historical source reconstruction. Owner-authorized public publication is complete; original-source confidentiality remains in effect. Hosted CI is verified in the [release acceptance record](../docs/release-acceptance.md).

## Run the complete offline tests

```sh
python -B -m unittest discover -s examples/tests -v
python examples/pipeline.py
python -B scripts/release_gate.py  # clean unpacked source only; for git checkouts scan git archive ZIP
```

The standard-library WSGI webhook, durable SQLite stores, recipient-safe HTML renderer, OpenAI structured-output adapter, Postmark adapter and pollable queue worker form a complete **reference code path**. The signed Typeform input, venue binding and persisted queue are tested through the actual WSGI callable. HTTP responses do not synchronously send email.

## Verified boundaries

- `webhook.py`, `webapp.py`: cryptographically verify raw Typeform body, match a configured form ID and independently mapped venue (including legacy hidden IDs), validate recipient and preferences, enqueue durable requests. Opaque route aliases are bound to secrets kept outside the repo.
- `catalog_store.py`, `pipeline.py`: transactional full-record catalog replacement, bounded CSV, venue and serving-format isolation, fixed-size preference fallback, pairing-aware candidate ranking and strict catalog-ID response validation.
- `delivery.py`, `runtime.py`, `worker.py`: durable queue, local worker polling, event state history, attempt fencing, retry limits, terminal failure review, operator reconciliation, retention cleanup. Remote submission is committed before the network call; timeouts and expired submission leases require operator verification and are **never automatically resent**.
- `providers.py`: real HTTP request adapters, configured explicitly. Offline tests inject fake provider responses. A provider's *acceptance* does not mean the diner received or opened the message.
- `postgres_catalog.py`, `migrations/001_portfolio_v2.sql`, `migrations/002_validate_tags.sql`, `migrations/003_legacy_review.sql`, `migrations/004_required_wine_fields.sql`, `migrations/005_canonical_tags.sql`, `migrations/006_review_archive_and_suggestions.sql`: staging Postgres implementation and non-destructive migration from the original database schema (when that database and an authorized staging instance are available). No real inventories are bundled.

## Suggested deployment

Supply secrets and an authenticated mapping of Typeform form ID → trusted venue (see `.env.example`). Leave `PG_DSN` empty for SQLite mode; set it only for an authorized PostgreSQL staging environment. Use a production WSGI server rather than the debugging server; e.g. configure Waitress to call `wsgi.get_application`. Run `python examples/worker.py --once` to process one batch, or `python examples/worker.py --poll-seconds 10` under a supervised worker process. Queue and catalog stores must be persistent files located outside source control with restricted filesystem access. The WSGI server and worker must share those state files and use a consistent UTC clock. Import a tested catalog before accepting real events. The worker has no credentials unless explicitly provided by the operator.

### Queue state and safe recovery

A pending request is claimable. Pre-submission errors are retried with backoff, at most three times. After recording `submitting`, any ambiguous network or database outcome becomes `reconciliation_required`. Manual requeue is forbidden for any event that has ever reached `submitting`, including timeouts and expired leases. To recover an unfulfilled customer request, obtain a new authenticated submission with a new token; do not replay the potentially sent event. For **confirmed acceptance**, the operator CLI authenticates a Postmark receipt lookup and checks the recipient and hashed event correlation metadata; free-text assertions alone cannot mark acceptance. Use `EventStore.unresolved()`, `history()` and `reconcile()` from a trusted maintenance console; record supporting evidence. **Exactly-once email delivery is not guaranteed.** Periodic cleanup removes terminal records after 30 days, with logs containing categories rather than full recipient payloads.

### Insufficient inventory notification

The customer-facing rule still requires exactly three catalog-eligible wine recommendations. When fewer than three exist, the **configured worker** sends an honest availability notice through the same durable outbox (`notify_insufficient=True`) without fabricating a wine. Standalone `process_event` calls default to terminal failure unless that notice policy is selected. A provider rejection or uncertain submission still follows the standard reconciliation states.

### Health, operations and data retention

`GET /live` checks that the HTTP process answers. `GET /health` verifies state-storage access, a bounded catalog readiness query (at most three rows per venue), and a recent worker heartbeat before reporting ready (503 otherwise). The worker emits `OPERATOR_ALERT` JSON records to stderr for stale queued or unresolved events; a real deployment must forward these to an external alert manager. Unresolved requests older than 90 days have their recipient-bearing payload scrubbed, while minimal hashed audit transitions are kept up to 365 days. Operators should reconcile delivery before data is scrubbed. Application-level rolling quotas limit requests per venue and recipient; denials are aggregated without retaining new recipient addresses. Model configuration/contract failures trigger redacted operator alerts even when deterministic fallback succeeds.

### Recommendation ranking and explanation safety

The safe worker passes up to 12 eligible candidates to the structured-output model, then validates three distinct catalog-scoped wine IDs. The model returns only ordered catalog wine IDs; factual descriptions are assembled locally from catalog fields, preventing unsupported generated price or health claims. The model-facing strict schema uses a catalog-scoped `enum` of eligible wine IDs and fixed three-item output; locally enforced invariants additionally reject empty, duplicate, invented and oversized data. Live schema acceptance still requires staging verification.

## Limits and staging work

The local regression suite tests the real WSGI callable, store, worker and mocked HTTP provider contracts. It does **not** demonstrate valid paid OpenAI/Postmark credentials, live Typeform delivery, a PostgreSQL server, network interruption under actual production load, or end-to-end delivery in a staging environment. PostgreSQL dependency (`psycopg2`) is optional and is not required for offline tests. Install pinned optional dependencies using `python -m pip install -r requirements-integration.txt`, review dependency advisories, then run version-tracked transactional migrations against a disposable PostgreSQL database before any real deployment. The dependency installation and live provider calls were not available in the offline audit. The independent SQLite reference architecture is not claimed to be a production-equivalent replacement for the historical PostgreSQL app.

See the [issue-by-issue repair matrix](../docs/prepublication-repair-matrix.md) and [independent case-study evidence map](../docs/evidence-map.md). No employer source code, personal diner data, original wine inventory, or access secrets are included. Public posting of this synthetic reference was expressly authorized by the owner; private historical material remains withheld.


### Operational integrity controls (v3)

- On POSIX, the event database is created with `0600` permissions and newly
  created state directories with `0700`; symbolic-link state paths are rejected.
  Windows deployments must configure a private NTFS ACL for the state directory.
- Only active/in-reconciliation events occupy queue capacity. Terminal events
  remain for bounded audit/duplicate protection and cannot starve admission.
- A corrupted, poisoned, or scrubbed event **cannot be blindly requeued**.
  Operators can cancel or confirm provider acceptance; reprocessing requires a
  newly verified source submission and an explicit recovery process.
- `/health` verifies queue/worker health plus the configured catalog in the
  deployed application. It does not imply every possible preference can be served.
- API failures **before mail submission** fall back to deterministic catalog-based
  recommendations; uncertain email submission **never** retries automatically.
- The model returns **exactly three ordered, eligible IDs**; it does not
  generate throwaway descriptions. Public descriptions come from catalog data.


### Historical v5 source hardening

[See the v5 remediation status](../docs/v5-remediation-status.md) for the 11 independently detected and repaired issues, 167 local regression tests, and honest external validation limits. PostgreSQL catalog replacement preserves approved pairing tags; new proposed pairings cannot silently overwrite reviewed data. The catalog importer stops reading after 10,001 rows, rather than exhausting arbitrary iterators. Operator reconciliation of a potentially accepted email requires an authenticated Postmark details lookup with matching `PalateEvent` metadata, sender and recipient. This is a **provider-acceptance** check, not proof of delivered mail. Workers must send with the event-correlation hash; old provider messages without it cannot be automatically confirmed.

### Signed-response age and retention (v8)

`submitted_at` must be an HMAC-authenticated Typeform UTC timestamp. Webhook ingress accepts responses submitted no more than 24 hours before the server clock (with at most 5 minutes future skew); older signed payloads fail validation even if an expired tombstone has been removed. Keep clocks synchronized. Typeform's normal retry window fits inside this horizon. This rule applies to the public WSGI HTTP path; trusted internal worker invocation already operates on persisted, previously authenticated requests.

### v10 full-snapshot and SQL validation rules

A trusted caller can explicitly replace an existing venue's catalog with zero wines using `replace_venue(venue_id, [], allow_empty=True)`. Without `allow_empty=True`, empty input is rejected to avoid accidental data loss. Both database adapters preserve venue registration while clearing wine rows inside a transaction; an unknown venue cannot be created by an empty snapshot. The SQLite adapter installs required-text and JSON-tag integrity triggers for direct SQL writers. Both `palate_valid_required_text` and `palate_valid_tag_array` must be registered with equivalent strict validators on every write connection; unregistered direct connections fail closed. Opening a database with invalid existing rows fails closed until approved repair or restoration. PostgreSQL migration `004_required_wine_fields.sql` adds an additive text-field integrity constraint; existing invalid rows must be repaired in staging before migration.

### v11 staging rules

A full source-registry import clears the staged wine rows for numeric `legacy-N`
venues that disappeared from the authoritative source registry, while retaining
reviewed identity maps and venue provenance for subsequent operator review.
Curated venues are never included in this cleanup. Migration
`005_canonical_tags.sql` requires normalized, nonempty lowercase tags using
letters, digits, `_` or `-`; noncanonical preexisting rows block migration and
require explicit operator repair. Run these against a disposable authorized
PostgreSQL instance before making live integration claims.

### v12 review archive (PostgreSQL staging only)

Migration 006 introduces `portfolio_v2.wine_review_archive` so reviewed
provenance/approvals survive temporary empty wine inventory. It backfills active
review records and validates historical `suggested_pairings` before committing.
Archived entries never participate in recommendations unless their stable wine
ID returns to active inventory. Run the migrations against a disposable staging
PostgreSQL database before using the adapter. Current CI offline mocks are not
evidence of a real PostgreSQL migration or provider acceptance.

## Historical v15 release

See [v15 remediation and verification](../docs/v15-five-finding-remediation.md) for
claim fencing and retry-budget changes, shared text validation, SQLite JSON
limits, raster-media preflight restrictions, and PostgreSQL migration 007.
Live PostgreSQL and provider staging are separate authorization gates.

## Historical v16 release

The [v16 remediation record](../docs/v16-eight-finding-remediation.md) explains
the complete claim-nonce enforcement, inert SVG preflight, canonical SQLite
JSON, and PostgreSQL migration 008 quarantine/readback semantics. Live
PostgreSQL and provider staging remain unverified. Owner-authorized publication is complete.


## Current v18 reference release

The [v18 release-closure record](../docs/v18-seven-finding-release-closure.md)
tracks the seven independently reproduced A17 findings and their verified source
repairs. Existing SQLite schemas are checked before use, with reviewed migration
required for incompatible state. Live PostgreSQL and authenticated providers
remain external integration gates. Owner-authorized publication and hosted CI are complete.
