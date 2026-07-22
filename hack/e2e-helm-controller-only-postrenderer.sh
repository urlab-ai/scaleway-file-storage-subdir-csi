#!/bin/sh
set -eu

# Same-cluster checkpoint recovery must start the controller provisionally while
# every node plugin remains absent. Helm still owns the exact rendered release;
# this release-candidate-only post-renderer removes the single node DaemonSet
# document until the missing-Lease recovery approval has been consumed. A
# subsequent unmodified Helm upgrade restores the production DaemonSet.
awk '
  function emit_document() {
    if (document != "" && !node_daemonset) {
      printf "%s", document
    }
    document = ""
    node_daemonset = 0
  }
  $0 == "---" {
    emit_document()
    document = $0 ORS
    next
  }
  {
    document = document $0 ORS
    if ($0 == "kind: DaemonSet") {
      node_daemonset = 1
    }
  }
  END {
    emit_document()
  }
'
