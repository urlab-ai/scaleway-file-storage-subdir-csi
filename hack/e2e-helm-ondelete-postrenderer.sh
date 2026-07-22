#!/bin/sh
set -eu

# Release qualification deliberately holds the candidate node DaemonSet in an
# OnDelete rollout so the N-1 and N node plugins coexist long enough to prove
# the controller's mixed-generation write barrier. A final unmodified Helm
# upgrade restores the chart's production RollingUpdate strategy afterwards.
sed '/^kind: DaemonSet$/,/^---$/ {
  s/type: RollingUpdate/type: OnDelete/
  /rollingUpdate: {maxUnavailable: 1}/d
}'
