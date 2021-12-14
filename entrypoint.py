import os
import pathlib
import runpy
import pytest

files_patterns = [
    'examples/fitbit_interday/*.py',
    'examples/fitbit_intraday/*.py',
    'examples/my_heart_counts/[!test_]*.py'
    'examples/siha/*.py',
    'examples/sleep_health/*.py',
    'examples/withings/*.py',
    'examples/zenodo_fitbit/*.py',
    'tasrif/test_scripts/*.py'
]

test_scripts = []
for files_pattern in files_patterns:
    test_scripts.extend(list(pathlib.Path(os.path.dirname(os.path.realpath(__file__))).rglob(files_pattern)))

@pytest.mark.parametrize('script', test_scripts, ids=lambda script: script.stem)
def test_script_execution(script):
    runpy.run_path(script)
