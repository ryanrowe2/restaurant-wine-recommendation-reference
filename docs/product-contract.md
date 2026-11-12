# Synthetic reference product contract

The demonstration accepts an authenticated venue identifier, one recipient address, and categorical wine preferences. Each response must be based on the selected fictional venue's eligible inventory. The preview does not describe any prior employer's commercial feature contract.

## Inputs and outputs

- Signed Typeform-style HTTP request and trusted form-to-venue binding
- Normalized category, sweetness, descriptors and meal preference
- Venue-scoped candidate wine identifiers
- Locally assembled customer-facing descriptions and a validated recommendation payload
- Durable delivery attempt state, a provider acceptance reference, and explicit reconciliation for uncertain sends

See `examples/recommendation_contract.json`, `examples/pipeline.py`, `examples/webhook.py`, `examples/delivery.py` and their tests for the executable contracts. No production-equivalent guarantee is implied.
