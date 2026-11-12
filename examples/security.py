"""Offline illustration of Typeform's documented HMAC-SHA256 webhook check.

Always verify the raw request bytes before parsing JSON or consulting a venue.
The test suite uses made-up secrets; this is not a deployed webhook endpoint.
"""

import base64
import hashlib
import hmac


MAX_BODY_BYTES = 1_048_576  # Example request-size policy, not a Typeform limit.


def verify_typeform_signature(raw_body: bytes, signature: str, secret: bytes) -> bool:
    """Check `Typeform-Signature: sha256=<base64 HMAC>` in constant time."""
    if (
        not isinstance(raw_body, bytes)
        or len(raw_body) > MAX_BODY_BYTES
        or not isinstance(signature, str)
        or not signature.startswith("sha256=")
        or not isinstance(secret, bytes)
        or not secret
    ):
        return False
    digest = hmac.new(secret, raw_body, hashlib.sha256).digest()
    expected = "sha256=" + base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(signature, expected)
