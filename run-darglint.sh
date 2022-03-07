#!/bin/bash
set -e
set -o pipefail # Prevents pipes from swallowing errors

mkdir -p darglint

# pass stdin arguments to darglint
find tasrif -type d -name test_scripts -prune -false -o -name "*.py" |
	xargs darglint -v 2 -s google -i "^_(.*)" |
	tee darglint/report.xml
