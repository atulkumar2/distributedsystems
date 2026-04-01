#!/bin/sh
# Maven Wrapper Script — downloads the declared Maven version on first use.
# Usage: ./mvnw <goals>

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WRAPPER_JAR="$SCRIPT_DIR/.mvn/wrapper/maven-wrapper.jar"
WRAPPER_PROPS="$SCRIPT_DIR/.mvn/wrapper/maven-wrapper.properties"

# Read distribution URL from properties file
DISTRIBUTION_URL=$(grep "^distributionUrl" "$WRAPPER_PROPS" | cut -d'=' -f2 | tr -d '[:space:]')
MAVEN_VERSION=$(echo "$DISTRIBUTION_URL" | grep -oP '(?<=apache-maven-)\d+\.\d+\.\d+')
MAVEN_HOME="$HOME/.m2/wrapper/dists/apache-maven-${MAVEN_VERSION}"
MVN_BIN="$MAVEN_HOME/bin/mvn"

# If the target Maven version is already installed locally, prefer it.
if command -v mvn > /dev/null 2>&1; then
    exec mvn "$@"
fi

# Fall back to downloading via wrapper jar if present.
if [ -f "$WRAPPER_JAR" ]; then
    exec java -jar "$WRAPPER_JAR" "$@"
fi

echo "ERROR: mvn not found on PATH and maven-wrapper.jar is missing." >&2
echo "Install Maven (https://maven.apache.org/install.html) or run:" >&2
echo "  mvn wrapper:wrapper" >&2
exit 1
