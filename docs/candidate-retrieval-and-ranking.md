# Reference candidate retrieval and ranking

The standalone reference implementation filters candidates by authenticated venue scope, availability, serving format and taste preferences before model selection. Preference relaxation remains bounded and never relaxes venue identity. This is a new demonstration algorithm, not a disclosure of any historical employer's recommendation code.

The adapter requests catalog wine identifiers, validates the model's structured answer against eligible candidates, and assembles **descriptions locally** from catalog facts. A deterministic fallback provides bounded candidate selection when an external model is unavailable. No claim is made about real-world ranking quality, customer conversion or a commercial algorithm.

Executable evidence: `examples/pipeline.py`, `examples/providers.py`, `examples/recommendation_contract.json`, `examples/tests/test_preference_fallback.py` and `examples/tests/test_recommendation_contract.py`.
