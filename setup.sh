#!/usr/bin/env bash
set -euxo pipefail

SPARK_HOME="$(python3 -c 'import site; print(site.getsitepackages()[0])')/pyspark"
SPARK_BIN="$SPARK_HOME/bin"
BARE_PYTHON="$(dirname $(which python3))/python"

if [[ ! -f "BARE_PYTHON" ]]; then
    ln -s $(which python3) "$BARE_PYTHON"
fi

if [[ $1 = "install-packages" ]]; then
    shift

    $SPARK_BIN/spark-shell \
        --conf spark.driver.extraJavaOptions="-Dsbt.ivy.home=$SPARK_HOME -Divy.home=$SPARK_HOME" \
        --packages "$(printf -- '%s,' $@ | cut -d "," -f 1-${#@})" \
    <<EOF
System.exit(0)
EOF

fi
