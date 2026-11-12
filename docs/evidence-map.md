# Public reference engineering evidence map

This map intentionally cites **only files included in the synthetic publication preview**. Historical employer source names, private project documentation, nonpublic implementation observations, internal inventory exports and deployment evidence have been withheld.

| Reference engineering claim | Publicly reviewable file | Evidence boundary |
| --- | --- | --- |
| Venue-scoped recommendation selection | `examples/pipeline.py` and `examples/tests/test_restaurant_isolation.py` | Offline reference logic and tests |
| Catalog ingestion and transaction behavior | `examples/catalog_store.py`, `examples/postgres_catalog.py`, `examples/migrations/` | SQLite test coverage; real PostgreSQL pending |
| Signed webhook admission | `examples/webhook.py`, `examples/webapp.py`, `examples/tests/test_security.py` | Local synthetic HMAC tests |
| Durable delivery and reconciliation | `examples/delivery.py`, `examples/tests/test_delivery.py` | Synthetic worker and provider doubles |
| Model and email adapter contracts | `examples/providers.py`, `examples/tests/test_provider_contracts.py` | Provider staging not executed |
| Offline release checks | `scripts/acceptance_gate.py`, `scripts/release_gate.py` | No publication authorization conferred |

Internal historical source-to-claim evidence remains outside this redacted package and must not be attached to the public repository without rights clearance.
