# Synthetic catalog ingestion and integrity

This document covers the included fictional catalog and reference import behavior. Historical employer inventory sizes, real restaurant counts, private import scripts and source exports are intentionally withheld.

## Reference input

`examples/synthetic_wines.csv` provides two fictional venue identifiers and fictional wine attributes for deterministic testing. The example importer accepts a fully validated venue-scoped replacement, enforces meaningful text/category fields, and performs atomic writes. Empty replacement requires explicit authorization on an existing venue.

The PostgreSQL reference adapter uses a separate staging schema, additive SQL migrations and a reviewed alias/pairing workflow. Legacy import and archived reviewer-owned data are demonstrated through code and simulated tests; live migrations and concurrent transaction validation remain X01 external gates.

See `examples/catalog_store.py`, `examples/postgres_catalog.py`, the nine files in `examples/migrations/`, and the suite in `examples/tests/` for executable details.
