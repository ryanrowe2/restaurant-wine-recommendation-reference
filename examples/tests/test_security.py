"""Reject unsigned and modified Typeform-like payloads before processing."""

import base64
import hashlib
import hmac
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from security import MAX_BODY_BYTES, verify_typeform_signature  # noqa: E402


class TypeformSignatureTests(unittest.TestCase):
    def setUp(self):
        self.raw = b'{"form_response":{"token":"test-token"}}'
        self.secret = b"fictional-test-secret"
        self.signature = "sha256=" + base64.b64encode(hmac.new(self.secret, self.raw, hashlib.sha256).digest()).decode("ascii")

    def test_signed_payload_is_accepted(self):
        self.assertTrue(verify_typeform_signature(self.raw, self.signature, self.secret))

    def test_tampering_or_wrong_secret_fails(self):
        self.assertFalse(verify_typeform_signature(self.raw + b" ", self.signature, self.secret))
        self.assertFalse(verify_typeform_signature(self.raw, self.signature, b"wrong"))

    def test_missing_signature_secret_or_wrong_header_fails(self):
        self.assertFalse(verify_typeform_signature(self.raw, "", self.secret))
        self.assertFalse(verify_typeform_signature(self.raw, self.signature, b""))
        self.assertFalse(verify_typeform_signature(self.raw, "md5=" + self.signature[7:], self.secret))

    def test_size_limit_fails_closed(self):
        self.assertFalse(verify_typeform_signature(b"x" * (MAX_BODY_BYTES + 1), self.signature, self.secret))


if __name__ == "__main__":
    unittest.main()
