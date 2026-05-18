#!/usr/bin/env bash
#
# create-topics.sh — apply topics.yaml against the chitragupta Kafka cluster.
#
# Usage:
#   BOOTSTRAP=10.12.0.219:9092 ./create-topics.sh                     # apply all topics with create_in_milestone unset OR == this-milestone
#   BOOTSTRAP=10.12.0.219:9092 MILESTONE=1a ./create-topics.sh        # apply only Phase 1 topics
#   BOOTSTRAP=10.12.0.219:9092 DRY_RUN=1 ./create-topics.sh           # show kafka-topics.sh commands without executing
#
# Requirements:
#   - kafka-topics.sh on PATH (from kafka_2.13-3.x package)
#   - yq v4 on PATH
#   - If SASL is enabled on the cluster, set KAFKA_OPTS / command-config appropriately:
#       export CMD_CFG=/etc/kafka/client.properties        # contains security.protocol + sasl.* lines
#
# Idempotent: re-running on existing topics is a no-op (we ALTER configs to match where they drift).

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-}"
MILESTONE="${MILESTONE:-1a}"            # only topics with create_in_milestone == this OR unset are applied
DRY_RUN="${DRY_RUN:-0}"
CMD_CFG="${CMD_CFG:-}"                   # path to client.properties for SASL; optional

if [[ -z "${BOOTSTRAP}" ]]; then
  echo "ERROR: set BOOTSTRAP=host:port (e.g. 10.12.0.219:9092)" >&2
  exit 2
fi

for bin in kafka-topics.sh yq jq; do
  command -v "${bin}" >/dev/null || { echo "ERROR: ${bin} not on PATH" >&2; exit 2; }
done

SPEC="${SPEC:-$(dirname "$0")/topics.yaml}"
[[ -f "${SPEC}" ]] || { echo "ERROR: spec not found: ${SPEC}" >&2; exit 2; }

# Build the bootstrap + auth flags once.
KT_FLAGS=(--bootstrap-server "${BOOTSTRAP}")
[[ -n "${CMD_CFG}" ]] && KT_FLAGS+=(--command-config "${CMD_CFG}")

run() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "DRY-RUN: $*"
  else
    "$@"
  fi
}

# List once for the diff; cheap.
existing=$(kafka-topics.sh "${KT_FLAGS[@]}" --list 2>/dev/null || true)

# Iterate topics from YAML.
n=$(yq '.topics | length' "${SPEC}")
applied=0
for ((i = 0; i < n; i++)); do
  name=$(yq ".topics[$i].name"                "${SPEC}")
  parts=$(yq ".topics[$i].partitions"         "${SPEC}")
  rf=$(yq ".topics[$i].replication_factor"    "${SPEC}")
  ms=$(yq ".topics[$i].create_in_milestone // \"1a\"" "${SPEC}")

  if [[ "${ms}" != "${MILESTONE}" ]]; then
    echo "SKIP  ${name} (milestone ${ms} != ${MILESTONE})"
    continue
  fi

  # Build --config k=v list
  cfg_keys=$(yq ".topics[$i].config | keys | .[]" "${SPEC}")
  cfg_args=()
  while IFS= read -r k; do
    [[ -z "${k}" ]] && continue
    v=$(yq ".topics[$i].config.\"${k}\"" "${SPEC}")
    cfg_args+=(--config "${k}=${v}")
  done <<< "${cfg_keys}"

  if grep -qx "${name}" <<< "${existing}"; then
    echo "EXISTS ${name} — reconciling configs"
    # Build a single --alter --add-config list (kafka-topics expects k1=v1,k2=v2)
    add_cfg=$(yq ".topics[$i].config | to_entries | map(\"\(.key)=\(.value)\") | join(\",\")" "${SPEC}")
    run kafka-topics.sh "${KT_FLAGS[@]}" --alter --topic "${name}" --partitions "${parts}" 2>/dev/null || \
      echo "  note: partition count cannot decrease; current count preserved if higher"
    run kafka-configs.sh "${KT_FLAGS[@]}" --alter --entity-type topics --entity-name "${name}" \
      --add-config "${add_cfg}"
  else
    echo "CREATE ${name} (parts=${parts} rf=${rf})"
    run kafka-topics.sh "${KT_FLAGS[@]}" --create \
      --topic "${name}" \
      --partitions "${parts}" \
      --replication-factor "${rf}" \
      "${cfg_args[@]}"
  fi
  applied=$((applied + 1))
done

echo
echo "Applied ${applied} topic(s) for milestone=${MILESTONE} (dry-run=${DRY_RUN})."
