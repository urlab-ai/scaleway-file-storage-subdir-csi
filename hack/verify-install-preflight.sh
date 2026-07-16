#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM

cat >"$TMP_DIR/kubectl" <<'EOF'
#!/bin/sh
set -eu
scenario=${PREFLIGHT_SCENARIO:-success}
if [ "$1" = get ] && [ "$2" = namespace ]; then
  if [ "$scenario" = missing-psa ]; then
    printf '%s\n' '{"metadata":{"labels":{}}}'
  else
    printf '%s\n' '{"metadata":{"labels":{"pod-security.kubernetes.io/enforce":"privileged"}}}'
  fi
  exit 0
fi
if [ "$1" = -n ] && [ "$3" = get ] && [ "$4" = secret ]; then
  if [ "$scenario" = missing-secret-key ] && [ "$5" = provider-secret ]; then
    printf '%s\n' '{"data":{"SCW_ACCESS_KEY":"encoded"}}'
  elif [ "$scenario" = custom-keys ] && [ "$5" = provider-secret ]; then
    printf '%s\n' '{"data":{"custom.access-key":"encoded","custom.secret-key":"encoded"}}'
  elif [ "$scenario" = custom-keys-mismatch ] && [ "$5" = provider-secret ]; then
    printf '%s\n' '{"data":{"SCW_ACCESS_KEY":"encoded","SCW_SECRET_KEY":"encoded"}}'
  elif [ "$scenario" = custom-keys ] && [ "$5" = identity-secret ]; then
    printf '%s\n' '{"data":{"custom.identity-key":"encoded"}}'
  elif [ "$scenario" = custom-keys-mismatch ] && [ "$5" = identity-secret ]; then
    printf '%s\n' '{"data":{"installationID":"encoded"}}'
  elif [ "$5" = identity-secret ]; then
    printf '%s\n' '{"data":{"installationID":"encoded"}}'
  else
    printf '%s\n' '{"data":{"SCW_ACCESS_KEY":"encoded","SCW_SECRET_KEY":"encoded"}}'
  fi
  exit 0
fi
if [ "$1" = -n ] && [ "$3" = create ]; then
  sed -n '1,200p' >/dev/null
  [ "$scenario" != rejected-dry-run ]
  exit
fi
exit 2
EOF

cat >"$TMP_DIR/scw" <<'EOF'
#!/bin/sh
set -eu
if [ "${PREFLIGHT_SCENARIO:-success}" = missing-cluster-tag ]; then
  tags='[]'
else
  tags='["scw-filestorage-csi"]'
fi
printf '{"id":"11111111-1111-4111-8111-111111111111","project_id":"22222222-2222-4222-8222-222222222222","region":"fr-par","type":"kapsule","tags":%s}\n' "$tags"
EOF
chmod +x "$TMP_DIR/kubectl" "$TMP_DIR/scw"

run_preflight() {
  PREFLIGHT_SCENARIO=$1 \
    KUBECTL="$TMP_DIR/kubectl" SCW="$TMP_DIR/scw" \
    "$ROOT_DIR/hack/install-preflight.sh" \
      --namespace=driver-system \
      --credentials-secret=provider-secret \
      --identity-secret=identity-secret \
      --cluster-id=11111111-1111-4111-8111-111111111111 \
      --project-id=22222222-2222-4222-8222-222222222222 \
      --region=fr-par
}

run_custom_preflight() {
  PREFLIGHT_SCENARIO=$1 \
    KUBECTL="$TMP_DIR/kubectl" SCW="$TMP_DIR/scw" \
    "$ROOT_DIR/hack/install-preflight.sh" \
      --namespace=driver-system \
      --credentials-secret=provider-secret \
      --credentials-access-key=custom.access-key \
      --credentials-secret-key=custom.secret-key \
      --identity-secret=identity-secret \
      --identity-key=custom.identity-key \
      --cluster-id=11111111-1111-4111-8111-111111111111 \
      --project-id=22222222-2222-4222-8222-222222222222 \
      --region=fr-par
}

run_preflight success >"$TMP_DIR/success.out"
grep -Fq 'Install preflight passed' "$TMP_DIR/success.out"
run_custom_preflight custom-keys >"$TMP_DIR/custom-keys.out"
grep -Fq 'Install preflight passed' "$TMP_DIR/custom-keys.out"

expect_failure() {
  scenario=$1
  message=$2
  if run_preflight "$scenario" >"$TMP_DIR/$scenario.out" 2>"$TMP_DIR/$scenario.err"; then
    echo "install preflight verification failed: $scenario unexpectedly passed" >&2
    exit 1
  fi
  if ! grep -Fq "$message" "$TMP_DIR/$scenario.err"; then
    echo "install preflight verification failed: $scenario did not report $message" >&2
    cat "$TMP_DIR/$scenario.err" >&2
    exit 1
  fi
}

expect_failure missing-psa 'must enforce the privileged Pod Security level'
expect_failure missing-secret-key 'lacks configured non-empty SCW_ACCESS_KEY/SCW_SECRET_KEY'
expect_failure rejected-dry-run 'rejected the privileged Pod server-side dry-run'
expect_failure missing-cluster-tag 'required cluster-level tag scw-filestorage-csi'

if run_custom_preflight custom-keys-mismatch >"$TMP_DIR/custom-keys-mismatch.out" 2>"$TMP_DIR/custom-keys-mismatch.err"; then
  echo "install preflight verification failed: mismatched custom keys unexpectedly passed" >&2
  exit 1
fi
grep -Fq 'lacks configured non-empty custom.access-key/custom.secret-key keys' "$TMP_DIR/custom-keys-mismatch.err"

echo "Install preflight verification passed"
