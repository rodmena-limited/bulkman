#!/usr/bin/env bash
# Audit evaluation runner for bulkman.
#
# Runs every probe in this directory against the local codebase (through the
# project venv if present, else the active python), summarises PASS/FAIL per
# probe, and exits non-zero if any probe FAILs.
#
# All probes are safe: they exercise the library API in-process/subprocess
# and never touch external services.  Run from anywhere:
#   audit/evaluations/run_all.sh

set -u

cd "$(dirname "$0")"

if [ -x "../../.venv/bin/python" ]; then
    PY="../../.venv/bin/python"
elif [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
else
    PY="python3"
fi

pass=0
fail=0
failed_probes=()

for probe in probe_*.py; do
    out="$("$PY" "$probe" 2>&1)"
    rc=$?
    if [ $rc -eq 0 ]; then
        echo "PASS  $probe"
        pass=$((pass + 1))
    else
        echo "FAIL  $probe"
        echo "$out" | sed 's/^/      /'
        failed_probes+=("$probe")
        fail=$((fail + 1))
    fi
done

echo
echo "summary: $pass passed, $fail failed"
if [ $fail -gt 0 ]; then
    printf 'failed probes: %s\n' "${failed_probes[*]}"
    exit 1
fi
exit 0
