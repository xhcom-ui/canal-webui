#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

BASE=".."
CLASSPATH="$BASE/conf"
for jar in "$BASE"/lib/*; do
  CLASSPATH="$jar:$CLASSPATH"
done

exec /Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home/bin/java \
  -server -Xms128m -Xmx512m -Xss1m \
  -XX:-OmitStackTraceInFastThrow \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="$BASE/logs" \
  -Djava.awt.headless=true \
  -Djava.net.preferIPv4Stack=false \
  -Dfile.encoding=UTF-8 \
  -DappName=otter-canal \
  -Dlogback.configurationFile="$BASE/conf/logback.xml" \
  -Dcanal.conf="$BASE/conf/canal.properties" \
  -classpath ".:$CLASSPATH" \
  com.alibaba.otter.canal.deployer.CanalLauncher
