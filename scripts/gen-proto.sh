#!/usr/bin/env bash

set -eu

CLONE_DIR="${1:-}"
PROTO_DIR="${2:-}"
BRANCH="${3:-master}"

if [[ -z "$CLONE_DIR" || -z "$PROTO_DIR" ]]; then
  echo "Usage: ${0} [CLONE_DIR] [PROTO_DIR] [BRANCH]" >&2
  exit 1
fi

GIT="git -C ${CLONE_DIR}"

if [ -d "$CLONE_DIR" ]; then
  $GIT reset -q --hard HEAD
  $GIT checkout -q "$BRANCH" &>/dev/null || $GIT fetch --all
else
  git clone -q https://github.com/pingcap/kvproto.git "$CLONE_DIR"
fi

$GIT checkout -q "$BRANCH"

echo "proto dir is ${PROTO_DIR}"

rm -rf "$PROTO_DIR"
mkdir -p "$PROTO_DIR"

cp -r "$CLONE_DIR"/include/gogoproto "$PROTO_DIR"/
cp "$CLONE_DIR"/include/*.proto "$PROTO_DIR"/
cp "$CLONE_DIR"/proto/*.proto "$PROTO_DIR"/

patch_proto() {
  local p="$1"
  sed -i "s/^ *option *java_package *=.*$//g" "$p"
  echo -e "\noption java_package = \"com.pingcap.kvproto\";" >>"$p"
}

while IFS= read -r -d '' file; do
  patch_proto "$file"
done < <(find "$PROTO_DIR" -type f -name "*.proto" -print0)
