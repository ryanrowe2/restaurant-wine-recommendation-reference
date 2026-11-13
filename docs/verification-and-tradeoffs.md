# Verification, limitations and design tradeoffs

The package is a newly authored fictional-data reference implementation. Its unit tests cover venue isolation, bounded catalog selection, durable event fencing, malformed input rejection, synthetic provider receipts, credential scanning and release-hash validation.

## Local verification

The frozen internal v18 source had 323 passing tests and 32 archive acceptance checks with 86% branch coverage. The current redacted preview has a **new archive identity** and must be verified independently; neither result implies production certification.

## Published baseline

**Publication status:** Public and branch-protected. Certified engineering baseline: `dc328b3f2d14acb1f8a0b59472c69d096f1832e1`, Git tree `e1b2f84fee8e1942f2a97e1d1aefbf4eeef05b6d`, 100 tracked files. [Hosted CI](https://github.com/ryanrowe2/restaurant-wine-recommendation-reference/actions/runs/43330352366) passed Python 3.10, 3.12 and 3.14: 325 tests executed, one optional skip, 86% branch coverage. Subsequent presentation edits have their own Git commit identity.

Owner authorization completed X04/X06 for this independent synthetic public reference. Private historical material remains withheld. Historical planning entries in the issue register are retained as engineering evidence.

## Remaining live-integration requirements

- X01: Real PostgreSQL migration, rollback, concurrency and archive restoration
- X02: Authenticated Typeform, OpenAI and Postmark staging using authorized test accounts
- X05: Production-equivalent security, fault, recovery and load checks if production readiness is claimed

The reference delivery system prioritizes safe reconciliation of ambiguous sends over automatic resend availability. Successful provider API acceptance is not equivalent to final email delivery. The data examples are fictional; no business impact metrics are claimed.

### Historical v9 release hardening

Earlier internal audit records are deliberately withheld from this preview. The current release acceptance policy is documented in `docs/release-acceptance.md`, and the `v11-six-finding-remediation.md` path is retained as a redacted navigation stub.
