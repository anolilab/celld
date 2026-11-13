#!/usr/bin/env bash
# Runs the object-store contract suite against one store from compose.yml and
# prints a one-line result: `<provider> <image> pass|fail`.
#
#   crates/celld/tests/stores/run.sh rustfs|seaweedfs
#
# Needs Docker with Compose. Exits non-zero when the suite fails.
set -euo pipefail

provider=${1:?usage: run.sh rustfs|seaweedfs}
here=$(cd "$(dirname "$0")" && pwd)
compose=(docker compose -f "$here/compose.yml" -p "celld-store-$provider")

case $provider in
  rustfs)    port=9000 key=rustfsadmin ;;
  seaweedfs) port=8333 key=seaweedadmin ;;
  *) echo "unknown provider: $provider" >&2; exit 2 ;;
esac

cleanup() { "${compose[@]}" down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

"${compose[@]}" up -d "$provider"
image=$("${compose[@]}" config --images "$provider")

# Wait for the S3 port to answer, then create the bucket from a container on
# the same network, so no AWS CLI is needed on the host.
timeout 90 bash -c "until curl -s -o /dev/null http://127.0.0.1:$port/; do sleep 1; done"
docker run --rm --network "container:$("${compose[@]}" ps -q "$provider")" \
  -e AWS_ACCESS_KEY_ID="$key" -e AWS_SECRET_ACCESS_KEY="$key" -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url "http://127.0.0.1:$port" s3 mb s3://contract >/dev/null

status=pass
AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$key \
AWS_ENDPOINT_URL="http://127.0.0.1:$port" AWS_REGION=us-east-1 \
CELLD_STORE_CONTRACT_BUCKET=s3://contract \
  cargo test -p celld --test store_contract --locked || status=fail

echo "$provider $image $status"
[ "$status" = pass ]
