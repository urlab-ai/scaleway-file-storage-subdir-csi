{{/* Common names and immutable labels. */}}
{{- define "scaleway-sfs-subdir-csi.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "scaleway-sfs-subdir-csi.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.labels" -}}
app.kubernetes.io/name: {{ include "scaleway-sfs-subdir-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.controllerServiceAccount" -}}
{{- default (printf "%s-controller" (include "scaleway-sfs-subdir-csi.fullname" .)) .Values.serviceAccounts.controller.name -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.nodeServiceAccount" -}}
{{- default (printf "%s-node" (include "scaleway-sfs-subdir-csi.fullname" .)) .Values.serviceAccounts.node.name -}}
{{- end -}}

{{/* A digest is the production pull identity; the tag remains version metadata. */}}
{{- define "scaleway-sfs-subdir-csi.driverImage" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.sidecarImage" -}}
{{- if .digest -}}
{{- printf "%s@%s" .image .digest -}}
{{- else -}}
{{- printf "%s:%s" .image .tag -}}
{{- end -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.durationSeconds" -}}
{{- $value := . -}}
{{- if hasSuffix "s" $value -}}
{{- trimSuffix "s" $value | atoi -}}
{{- else if hasSuffix "m" $value -}}
{{- mul (trimSuffix "m" $value | atoi) 60 -}}
{{- else if hasSuffix "h" $value -}}
{{- mul (trimSuffix "h" $value | atoi) 3600 -}}
{{- else -}}
{{- fail (printf "unsupported duration %q" $value) -}}
{{- end -}}
{{- end -}}

{{- define "scaleway-sfs-subdir-csi.nodeConfigGeneration" -}}
{{- $parents := dict -}}
{{- range $poolName, $pool := .Values.pools -}}
{{- range $parent := $pool.filesystems -}}
{{- $_ := set $parents $parent.id (dict "pool" $poolName "basePath" $pool.basePath) -}}
{{- end -}}
{{- end -}}
{{- $commercialTypes := .Values.compatibility.qualifiedCommercialTypes | sortAlpha -}}
{{- dict "driverName" .Values.driver.name "region" .Values.scaleway.region "parents" $parents "nodeParentMountRoot" .Values.node.parentMountRoot "kubeletPath" .Values.node.kubeletPath "accessModes" (list "SINGLE_NODE_WRITER" "MULTI_NODE_MULTI_WRITER") "ownershipSchema" "1" "qualifiedCommercialTypes" $commercialTypes | toJson | sha256sum -}}
{{- end -}}

{{/* Cross-field validation that JSON Schema cannot express. */}}
{{- define "scaleway-sfs-subdir-csi.validate" -}}
{{- $v := .Values -}}
{{- if ne (int $v.controller.replicas) 1 }}{{ fail "controller.replicas must equal 1 in v1" }}{{ end -}}
{{- if ne $v.controller.updateStrategy "Recreate" }}{{ fail "controller.updateStrategy must be Recreate in v1" }}{{ end -}}
{{- if not $v.controller.leadership.enabled }}{{ fail "controller.leadership.enabled must be true" }}{{ end -}}
{{- if not $v.controller.leaderElection }}{{ fail "controller.leaderElection must be true for supported sidecars" }}{{ end -}}
{{- if not $v.controller.privilegedMounts }}{{ fail "controller.privilegedMounts must be true for the v1 virtiofs contract" }}{{ end -}}
{{- if and (not $v.serviceAccounts.controller.create) (eq $v.serviceAccounts.controller.name "") }}{{ fail "serviceAccounts.controller.name is required when create=false" }}{{ end -}}
{{- if and (not $v.serviceAccounts.node.create) (eq $v.serviceAccounts.node.name "") }}{{ fail "serviceAccounts.node.name is required when create=false" }}{{ end -}}
{{- if eq $v.scaleway.credentials.accessKeyKey $v.scaleway.credentials.secretKeyKey }}{{ fail "credential Secret key names must be distinct" }}{{ end -}}
{{- if not (hasPrefix (printf "%s-" $v.scaleway.region) $v.scaleway.defaultZone) }}{{ fail "scaleway.defaultZone must belong to scaleway.region" }}{{ end -}}
{{- if $v.integrationTest.fakeDriver.enabled -}}
{{- if ne $v.release.mode "development" }}{{ fail "integrationTest.fakeDriver is development-only" }}{{ end -}}
{{- if $v.metrics.enabled }}{{ fail "integrationTest.fakeDriver requires metrics.enabled=false because the test endpoint exposes no metrics" }}{{ end -}}
{{- if ne (clean $v.integrationTest.fakeDriver.binaryPath) $v.integrationTest.fakeDriver.binaryPath }}{{ fail "integrationTest.fakeDriver.binaryPath must be absolute and lexically normalized" }}{{ end -}}
{{- end -}}

{{- $retry := include "scaleway-sfs-subdir-csi.durationSeconds" $v.controller.leadership.retryPeriod | atoi -}}
{{- $renew := include "scaleway-sfs-subdir-csi.durationSeconds" $v.controller.leadership.renewDeadline | atoi -}}
{{- $lease := include "scaleway-sfs-subdir-csi.durationSeconds" $v.controller.leadership.leaseDuration | atoi -}}
{{- if or (ge $retry $renew) (ge $renew $lease) }}{{ fail "leadership timing must satisfy retryPeriod < renewDeadline < leaseDuration" }}{{ end -}}
{{- $attach := include "scaleway-sfs-subdir-csi.durationSeconds" $v.controller.attachReadyDeadline | atoi -}}
{{- $operation := include "scaleway-sfs-subdir-csi.durationSeconds" $v.sidecars.operationTimeout | atoi -}}
{{- if lt $operation (add $attach 60) }}{{ fail "sidecars.operationTimeout must exceed controller.attachReadyDeadline by at least one minute" }}{{ end -}}
{{- $shutdown := include "scaleway-sfs-subdir-csi.durationSeconds" $v.controller.shutdownDeadline | atoi -}}
{{- if lt (int $v.controller.terminationGracePeriodSeconds) (add $shutdown 30) }}{{ fail "controller termination grace period must exceed shutdownDeadline by at least 30 seconds" }}{{ end -}}
{{- $startupBudget := mul (int $v.probes.startup.periodSeconds) (int $v.probes.startup.failureThreshold) -}}
{{- if lt (int $v.controller.progressDeadlineSeconds) (add $startupBudget 300) }}{{ fail "controller progressDeadlineSeconds must cover startup probe budget plus five minutes" }}{{ end -}}
{{- if gt (int $v.sidecars.externalProvisioner.workerThreads) (int $v.controller.maxConcurrentMutations) }}{{ fail "externalProvisioner.workerThreads must not exceed controller.maxConcurrentMutations" }}{{ end -}}
{{- if gt (int $v.sidecars.externalAttacher.workerThreads) (int $v.controller.maxConcurrentMutations) }}{{ fail "externalAttacher.workerThreads must not exceed controller.maxConcurrentMutations" }}{{ end -}}

{{- range $label, $path := dict "controller.parentMountRoot" $v.controller.parentMountRoot "node.parentMountRoot" $v.node.parentMountRoot "node.kubeletPath" $v.node.kubeletPath -}}
{{- if ne (clean $path) $path }}{{ fail (printf "%s must be absolute and lexically normalized" $label) }}{{ end -}}
{{- end -}}
{{- $parentRootSlash := printf "%s/" $v.node.parentMountRoot -}}
{{- $kubeletSlash := printf "%s/" $v.node.kubeletPath -}}
{{- if or (eq $v.node.parentMountRoot $v.node.kubeletPath) (hasPrefix $parentRootSlash $kubeletSlash) (hasPrefix $kubeletSlash $parentRootSlash) }}{{ fail "node.parentMountRoot and node.kubeletPath must be disjoint" }}{{ end -}}

{{- $parentIDs := dict -}}
{{- range $poolName, $pool := $v.pools -}}
{{- if eq $pool.basePath "/" }}{{ fail (printf "pool %s basePath must not be root" $poolName) }}{{ end -}}
{{- if ne (clean $pool.basePath) $pool.basePath }}{{ fail (printf "pool %s basePath must be normalized" $poolName) }}{{ end -}}
{{- if hasPrefix "/.sfs-subdir-csi-owner" $pool.basePath }}{{ fail (printf "pool %s basePath uses the reserved parent-owner namespace" $poolName) }}{{ end -}}
{{- if gt (len $pool.filesystems) (int $pool.maxParentsPerEligibleNode) }}{{ fail (printf "pool %s exceeds maxParentsPerEligibleNode" $poolName) }}{{ end -}}
{{- if le (float64 $pool.maxLogicalOvercommitRatio) 0.0 }}{{ fail (printf "pool %s maxLogicalOvercommitRatio must be positive" $poolName) }}{{ end -}}
{{- if gt (atoi $pool.directoryUid) 2147483647 }}{{ fail (printf "pool %s directoryUid exceeds 2147483647" $poolName) }}{{ end -}}
{{- if gt (atoi $pool.directoryGid) 2147483647 }}{{ fail (printf "pool %s directoryGid exceeds 2147483647" $poolName) }}{{ end -}}
{{- range $parent := $pool.filesystems -}}
{{- if hasKey $parentIDs $parent.id }}{{ fail (printf "parent filesystem ID %s appears more than once" $parent.id) }}{{ end -}}
{{- $_ := set $parentIDs $parent.id $poolName -}}
{{- end -}}
{{- range $field := list $poolName $pool.basePath $pool.directoryMode $pool.directoryUid $pool.directoryGid -}}
{{- if gt (len $field) 128 }}{{ fail (printf "pool %s contains a fixed volume_context value over 128 bytes" $poolName) }}{{ end -}}
{{- end -}}
{{- end -}}

{{- $defaults := 0 -}}
{{- $storageClassNames := dict -}}
{{- range $class := $v.storageClasses -}}
{{- if not (hasKey $v.pools $class.poolName) }}{{ fail (printf "StorageClass %s references missing pool %s" $class.name $class.poolName) }}{{ end -}}
{{- if hasKey $storageClassNames $class.name }}{{ fail (printf "StorageClass name %s is duplicated" $class.name) }}{{ end -}}
{{- $_ := set $storageClassNames $class.name true -}}
{{- if $class.defaultClass }}{{ $defaults = add $defaults 1 }}{{ end -}}
{{- end -}}
{{- if gt $defaults 1 }}{{ fail "at most one StorageClass may be default" }}{{ end -}}

{{- if eq $v.release.mode "production" -}}
{{- $nodeAffinityJSON := $v.node.affinity | toJson -}}
{{- if contains "\"requiredDuringSchedulingIgnoredDuringExecution\":" $nodeAffinityJSON }}{{ fail "production node affinity must not narrow the all-schedulable-Linux-node set" }}{{ end -}}
{{- $controllerPlacementJSON := dict "nodeSelector" $v.controller.nodeSelector "affinity" $v.controller.affinity | toJson -}}
{{- if contains "kubernetes.io/hostname" $controllerPlacementJSON }}{{ fail "production controller placement must not pin kubernetes.io/hostname" }}{{ end -}}
{{- if eq $v.driver.name "sfs-subdir.csi.example.com" }}{{ fail "production requires a final globally unique CSI driver name" }}{{ end -}}
{{- if or (eq $v.scaleway.projectId "00000000-0000-4000-8000-000000000000") (hasPrefix "00000000-0000-4000-8000-" (first (keys $parentIDs | sortAlpha))) }}{{ fail "production values contain synthetic provider identifiers" }}{{ end -}}
{{- if or $v.installation.generateForDevelopmentOnly (not $v.scheduling.allSchedulableLinuxNodesAreEligible) (not $v.scheduling.requireHomogeneousEligibleNodes) $v.scheduling.skipNodePreflightForDevelopmentOnly }}{{ fail "production requires external identity and homogeneous all-Linux-node preflight" }}{{ end -}}
{{- if ne $v.node.parentMountRoot "/var/lib/scaleway-sfs-subdir-csi/parents" }}{{ fail "production node.parentMountRoot is fixed" }}{{ end -}}
{{- if or (ne (get $v.node.nodeSelector "kubernetes.io/os") "linux") (ne (len $v.node.nodeSelector) 1) }}{{ fail "production node.nodeSelector must cover all schedulable Linux nodes" }}{{ end -}}
{{- if ne (get $v.controller.nodeSelector "kubernetes.io/os") "linux" }}{{ fail "controller.nodeSelector must require kubernetes.io/os=linux" }}{{ end -}}
{{- range $entry := list $v.image $v.sidecars.externalProvisioner $v.sidecars.externalAttacher $v.sidecars.nodeDriverRegistrar $v.sidecars.livenessProbe -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $entry.digest) }}{{ fail "production requires an immutable sha256 digest for every image" }}{{ end -}}
{{- if or (eq $entry.tag "") (eq $entry.tag "latest") (eq $entry.tag "development-unpinned") }}{{ fail "production requires an explicit qualified non-latest tag for every image" }}{{ end -}}
{{- end -}}
{{- if eq (index .Chart.Annotations "scaleway-sfs-subdir-csi.io/release-status") "development-only" }}{{ fail "this development chart version cannot render in production mode" }}{{ end -}}
{{- end -}}
{{- end -}}
