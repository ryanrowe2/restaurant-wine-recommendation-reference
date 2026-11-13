# Release acceptance: ending the re-audit loop

## Completed publication

**Publication status:** Public and branch-protected. Certified engineering baseline: `dc328b3f2d14acb1f8a0b59472c69d096f1832e1`, Git tree `e1b2f84fee8e1942f2a97e1d1aefbf4eeef05b6d`, 100 tracked files. [Hosted CI](https://github.com/ryanrowe2/restaurant-wine-recommendation-reference/actions/runs/43330352366) passed Python 3.10, 3.12 and 3.14: 325 tests executed, one optional skip, 86% branch coverage. Subsequent presentation edits have their own Git commit identity.

The owner expressly authorized the published synthetic reference. Publication and hosted CI are complete. The historical offline gate behavior and review policy below are preserved: its `publication_ready=false` result is not the current publication status and does not reverse the separate owner authorization. Original private-source evidence remains withheld.

## Why the process changed

An automated-test count answers whether **known** assertions passed. It never proves that every possible software defect has been found. Earlier releases were repeatedly called "fully resolved" after addressing the most recent issue list, then independent audits uncovered new conditions. The corrective action is to preserve a single evidence-backed candidate, evaluate **explicit risk-based gates**, and record every new issue without repeatedly resetting the entire project.

## Authoritative candidate and verification

The authoritative artifact is **the SHA-256 identified ZIP**, not an uncommitted local directory, a prior conversation, or a mutable filename. Every repair produces a new checksum and requires verification of the exact archive, cleanly unpacked in temporary storage. Run:

```sh
python -m pip install -r requirements-quality.txt
python -B scripts/acceptance_gate.py --archive /absolute/path/to/release.zip \
  --expected-sha256 SHA256_FROM_SEPARATELY_PRESERVED_RELEASE_RECORD \
  --output /absolute/path/to/release-acceptance.json
```

The script checks exact-archive confidentiality, required files, Python syntax, JSON/SVG parsing, internal documentation links, registered regression-test evidence, known high-risk findings, all discovered tests, and **production-code branch coverage** (excluding tests). Its minimum measured overall branch-coverage floor is 84%, with at least 70% for each operational entry point and the PostgreSQL catalog adapter. Those are risk-based gates, not claims of complete coverage. The offline gate can pass while the disclosure/owner-approval gates remain blocked. The output records that explicitly.

The `quality/issue_register.json` file holds the current issue statuses. The immutable audited-ID history in `quality/issue_history_baseline.json` is verified against a pinned digest in the gate, so previously audited findings cannot be silently removed or reassigned. The qualified `unittest` evidence IDs in `quality/regression_execution_map.json` are likewise digest-pinned; the gate launches a real test runner and verifies that each linked test **actually executed successfully**, not merely that a matching function name appears in the tree. New findings may be added to the register and need a new separately reviewed baseline revision.

**Trust boundary:** The gate is still code shipped inside the candidate. A malicious party able to rewrite the whole ZIP, both anchors and the gate cannot be authenticated by an untrusted ZIP itself. Supply the **expected SHA-256 from a separate reviewed record**, not a digest freshly computed from the untrusted ZIP. `--expected-sha256` verifies this identity or rejects the archive before running the suite. Without it, the gate may report successful *offline checks* but **will not assert a verified frozen release candidate**. An output checksum is informational, not evidence of owner approval.

**Publication permission is never inferred from the checked-in issue register.** The `X04` and `X06` status fields are planning notes, not authorization. Even if someone changes them to `verified`, the gate always reports `publication_ready=false` and requires separate, documented disclosure rights and a fresh explicit user approval to publish that exact digest. Production staging and hosted CI are also not falsely certified by editable JSON.

## Issue handling, without infinite zero-defect claims

1. Reproduce each new issue with an exact test or a controlled trace. Link it to the affected file, observable impact, and existing issue if duplicated. Add a uniquely identified finding to the register before fixing it.
2. Classify it: **critical** = credential exposure/data loss or remote compromise; **high** = customer-visible correctness or delivery loss, tenant breach or irreversible state; **medium** = bounded degradation or operational risk; **low** = minor maintainability/UX issue. Severity is based on impact and exposure, not how surprising the bug seems.
3. Fix the cause, add an automated regression, and validate the affected invariant. Rerun the complete quality gate. A passing test without actual code coverage of the defect path does not close a finding.
4. Do **one** separate end-to-end adversarial review of the candidate after all known critical/high findings close. If it discovers a new critical/high defect, fix and recheck that area and dependencies. Duplicates are not counted as new issues. Do not claim that all imaginable defects are gone.
5. **Offline stopping rule:** no known open critical/high defects; no unadjudicated security/privacy findings; each closed issue has regression evidence; full gate and exact-artifact scan green. Document accepted lower-risk items rather than endlessly blocking on theoretical edge cases.
6. **Portfolio publication stopping rule:** document rights to describe the historical project and obtain explicit owner authorization to publish the exact candidate. Live PostgreSQL/provider tests are not prerequisites for publishing an **accurately disclosed independent reference example**. They are prerequisites for claims of tested live integration. GitHub-hosted CI follows an authorized push (possibly first to a private repository); it cannot be a condition preceding any repository creation.

## External checks and completed publication gates

| Gate | Required evidence |
|---|---|
| Disposable PostgreSQL integration (**production integration claim**) | Actual supported PostgreSQL instance, schema migrations, rollback, alias/metadata preservation and concurrent updates; sanitized execution log |
| Provider staging (**production integration claim**) | Authorized Typeform signed event, supported OpenAI strict JSON schema, Postmark acceptance/receipt/timeout, and recovery drills without real diner data |
| Hosted GitHub CI (**completed**) | [Successful baseline workflow](https://github.com/ryanrowe2/restaurant-wine-recommendation-reference/actions/runs/43330352366), Python 3.10 / 3.12 / 3.14 |
| Operational readiness (**production deployment claim**) | Backups/restore, failure injection under worker concurrency, alert routing, data-retention/security controls and abuse budgets |
| Portfolio publication (**completed**) | Owner-authorized synthetic reference published publicly; original-source material remains confidential |

The codebase is a **synthetic, independently authored portfolio reference**, not a production replacement. External checks are not implied to have passed by local mocks. A publication of the *case study* requires clarity about those limits, and production deployment claims require every operational gate.

## Decision terminology

- **BLOCKED:** exact-archive, security, required regression, correctness, or coverage gate failed.
- **OFFLINE VERIFIED / PUBLICATION PERMISSION PENDING:** the source passed local acceptance; publication still needs rights verification and the owner's explicit approval. Live service verification is reported, not misrepresented.
- **PORTFOLIO PUBLICATION:** must be authorized outside this offline gate and bound to the independently reviewed artifact hash. The offline tool never emits a publishing authorization or executes a GitHub write.

Never use "100% defect-free," "all possible issues resolved," or "production verified" from local tests alone.
