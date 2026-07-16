# Sample Prometheus Alerts

These examples are opt-in starting points. The chart installs no
`PrometheusRule`. Metrics/release evidence are incomplete; validate every query
against the release candidate.

```promql
max_over_time(sfs_subdir_controller_ready[10m]) < 1
max_over_time(sfs_subdir_controller_leader[10m]) < 1
time() - sfs_subdir_reconciliation_last_success_timestamp_seconds > 600
time() - sfs_subdir_attachment_inventory_last_success_timestamp_seconds > 600
sum(sfs_subdir_unknown_attachments) > 0
sum(sfs_subdir_published_node_fences) > 0
sum(sfs_subdir_eligible_nodes_ready) < sum(sfs_subdir_eligible_nodes_expected)
sum(sfs_subdir_node_config_generation_mismatches) > 0
sum(increase(sfs_subdir_mount_errors_total[15m])) > 0
sum by (pool, condition) (sfs_subdir_parent_condition{condition!="available"}) > 0
min by (pool) (sfs_subdir_pool_parent_available_bytes) < 10737418240
min by (pool) (sfs_subdir_pool_parent_actual_free_bytes) < 10737418240
max_over_time(sfs_subdir_controller_mutations_queued[10m]) > 0
```

The 600-second inventory and reconciliation thresholds are twice the default
five-minute controller maintenance interval. When the interval is customized,
set both thresholds to at least two complete intervals plus normal scrape
delay; a zero timestamp means no successful full pass has completed.

First actions are read-only: inspect readiness/events, Lease evidence, parent
status/inventories, eligible-node generation, and the blocked operation. Never
automate Lease deletion, detach, unmount, record edits, GC, or parent removal
from an alert.
