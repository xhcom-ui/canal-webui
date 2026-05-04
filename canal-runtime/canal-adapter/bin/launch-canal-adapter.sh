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
  '-Xlog:gc*:file=../logs/adapter/gc.log::filecount=5,filesize=32M' \
  -Djava.awt.headless=true \
  -Djava.net.preferIPv4Stack=true \
  -Dfile.encoding=UTF-8 \
  -Dhttp.proxyHost= -Dhttps.proxyHost= -DsocksProxyHost= \
  -Dhttp.proxyPort=0 -Dhttps.proxyPort=0 -DsocksProxyPort=0 \
  "-Dhttp.nonProxyHosts=localhost|127.0.0.1|::1" \
  "-DsocksNonProxyHosts=localhost|127.0.0.1|::1" \
  -DappName=canal-adapter \
  -classpath ".:$CLASSPATH" \
  com.alibaba.otter.canal.adapter.launcher.CanalAdapterApplication
