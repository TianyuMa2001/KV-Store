# Corrected full workloads; metadata finalization failed

All 12 cases and 36 measurement rounds completed, including PID/configuration checks, unreachable-port assertions, response validation, and shutdown verification. In case 10, 881/881 measured writes returned 503 and none returned 201.

Final environment collection failed on Git's dubious-ownership check. This is not a fully finalized benchmark bundle. Preserve the results and raw evidence for audit; do not label it as a successful final harness execution.

Observed artifact SHA-256 during this run: 051701b2e8f0fc15d341978107f622783787c6cb8e66f99f44d17a2197b1c217.
Harness SHA-256 for this run: 15cd80396cefa4ad10495df1621707d18d1785907a5f47b81f2c4b7b73f9fbfc.
Controller source SHA-256: 753b032014e8d375629555a337d30255c7816af08bbf4369f4fe18040e5c4b01.
Base commit: a863d1b5f8a4d4b43e4a14700b796dacc2f0101e, with local source/harness edits.

The final harness collects Git provenance before workloads, writes environment.json before measurement, checks source/harness stability, and persists fault proof. Its fresh end-to-end regression is in evidence/benchmark-quorum-regression.
