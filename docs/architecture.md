# Architecture Notes

## Logical layers
- **Ingestion:** read source files; assign run metadata; validate file-level contract.
- **Bronze:** raw landing layer for audit/replay.
- **Silver:** canonical names, emails, phones, country codes, and timestamps.
- **Quality:** evaluate configured rules; preserve rule-level outcomes; quarantine ERROR failures.
- **Identity:** match on normalized email or phone. Store the reason for each link.
- **Golden Record:** choose each attribute independently by configured source priority, then recency.
- **Serving:** publish Golden Records and lineage for downstream consumers.
- **Observability:** capture counts and alert on job failure or quality/reconciliation thresholds.

## Model choices
Canonical entity: customer/person. Every row carries `source_system`, `source_record_id`, and `run_id`. This allows a source row to be traced to its Golden Record.

## Matching and false positives
Exact email/phone matching is explainable and suitable for a small demo. Shared or placeholder contact values should be excluded from matching. Production matching should:
1. create candidate pairs with blocking keys;
2. score email, phone, name, and address similarities;
3. auto-link only above a validated threshold;
4. send borderline pairs to human review;
5. retain merge decisions and permit controlled unmerge.

## Survivorship
Use attribute-level priorities because CRM may be best for a name, while billing may be more authoritative for address. Within a source priority, prefer non-empty values, latest `updated_at`, then stable source record ID.

## Reliability
Use immutable raw data, run IDs, idempotent processing, Delta transactions, retries for transient failures, and quarantine for bad data. A production incremental pipeline should use CDC/watermarks and `MERGE`, not overwrite all historical data.

## Performance and scale
For tens of millions of records, use Spark DataFrames and native expressions; avoid Python row UDFs and driver collection. Block candidate comparisons. Inspect Spark UI for skew, shuffle spill, executor loss, and slow tasks. Compact small files and use partition pruning/clustering based on measured query patterns.

## Security and privacy
Use Unity Catalog, least privilege, managed identities/service principals, secret scopes or a secret manager, encryption, column masking, retention controls, and audited access. Do not log raw PII.

## Limitations and next steps
This repository is a batch reference implementation. For production, add persistent Golden ID cross-reference, pairwise confidence scoring, manual review, streaming checkpoints, job alerts, deployment automation, and integration tests on representative volumes.
