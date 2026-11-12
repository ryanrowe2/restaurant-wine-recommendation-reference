# Reference webhook and email delivery semantics

This synthetic architecture authenticates a signed Typeform-style webhook, validates a trusted form-to-venue mapping, and adds a canonical request to a durable SQLite queue. Worker claims carry lease and nonce fences. A submission with uncertain external acceptance remains in reconciliation rather than being blindly retried.

The offline adapter verifies provider acceptance and recipient identity using simulated responses. Template rendering escapes user-controlled fields. This reference does not expose any prior organization's private endpoint logic, message templates, source vulnerabilities, or delivery metrics.

Implementation: `examples/webapp.py`, `examples/webhook.py`, `examples/delivery.py`, `examples/providers.py`, `examples/worker.py`. Relevant tests reside in `examples/tests/test_delivery.py`, `test_provider_contracts.py`, and `test_v18_seven_finding_closure.py`.

No provider-backed staging or real customer email delivery is claimed.
