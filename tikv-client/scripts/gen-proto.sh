#!/usr/bin/env bash

set -eu

CLONE_DIR="${1:-}"
BRANCH="${2:-master}"

if [ -z "$CLONE_DIR" ]; then
  echo "Usage: ${0} [CLONE_DIR] [BRANCH]" >&2
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

patch_proto() {
  local p="$1"
  sed -i "s/^ *option *java_package *=.*$//g" "$p"
  echo -e "\noption java_package = \"com.pingcap.kvproto\";" >>"$p"
}

patch_proto "$CLONE_DIR"/include/gogoproto/*.proto

for p in "$CLONE_DIR"/include/*.proto; do
  patch_proto "$p"
done

for p in "$CLONE_DIR"/proto/*.proto; do
  patch_proto "$p"
done
