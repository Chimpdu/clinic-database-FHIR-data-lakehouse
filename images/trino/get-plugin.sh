#!/usr/bin/env bash
set -euo pipefail

VERSION=476
GROUP_ID=io.trino
ARTIFACT_ID=trino-hudi
REPO=https://repo1.maven.org/maven2

# Convert groupId (io.trino → io/trino)
GROUP_PATH=$(echo "${GROUP_ID}" | tr '.' '/')

# Build jar URL
JAR_URL="${REPO}/${GROUP_PATH}/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}.jar"

PLUGIN_DIR="/usr/lib/trino/plugin/hudi"

echo "Downloading ${ARTIFACT_ID}-${VERSION}.jar from Maven Central..."
mkdir -p "${PLUGIN_DIR}"
curl -fLo "${PLUGIN_DIR}/${ARTIFACT_ID}-${VERSION}.jar" "${JAR_URL}"

echo "Downloaded ${ARTIFACT_ID}-${VERSION}.jar to ${PLUGIN_DIR}"
