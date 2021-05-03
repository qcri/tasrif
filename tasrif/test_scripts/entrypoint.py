import os
import pathlib
import runpy
import pytest

test_scripts = pathlib.Path(os.path.dirname(os.path.realpath(__file__))).glob('test_pipeline_*.py')

@pytest.mark.parametrize('script', test_scripts, ids=lambda script: script.stem)
def test_script_execution(script):
    runpy.run_path(script)
