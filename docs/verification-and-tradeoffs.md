# Verification, limitations and design tradeoffs

The package is a newly authored fictional-data reference implementation. Its unit tests cover venue isolation, bounded catalog selection, durable event fencing, malformed input rejection, synthetic provider receipts, credential scanning and release-hash validation.

## Local verification

The frozen internal v18 source had 323 passing tests and 32 archive acceptance checks with 86% branch coverage. The current redacted preview has a **new archive identity** and must be verified independently; neither result implies production certification.

## Open external requirements

- X01: Real PostgreSQL migration, rollback, concurrency and archive restoration
- X02: Authenticated Typeform, OpenAI and Postmark staging using authorized test accounts
- X03: Hosted CI on an authorized commit
- X04: Original authorship, IP assignment and disclosure authorization
- X05: Production-equivalent security, fault, recovery and load checks if production readiness is claimed
- X06: Express approval for the exact candidate archive and public destination

The reference delivery system prioritizes safe reconciliation of ambiguous sends over automatic resend availability. Successful provider API acceptance is not equivalent to final email delivery. The data examples are fictional; no business impact metrics are claimed.

### Historical v9 release hardening

Earlier internal audit records are deliberately withheld from this preview. The current release acceptance policy is documented in `docs/release-acceptance.md`, and the `v11-six-finding-remediation.md` path is retained as a redacted navigation stub.
