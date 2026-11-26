#!/usr/bin/env bash
set -euo pipefail

mvn -q package

java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator &
PID1=$!

java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2 &
PID2=$!

java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3 &
PID3=$!

trap "kill $PID1 $PID2 $PID3 2>/dev/null || true" EXIT
wait
