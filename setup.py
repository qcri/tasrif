"""Module to setup the library. All library dependencies are added here.
"""
import sys
import os
from setuptools import setup, find_packages

print(find_packages(), file=sys.stderr)

# Due to a dependency conflict with a package used by tsfresh, the numpy
# version has to be below <= 1.20.
NUMPY_VERSION='numpy <= 1.20'

dir_path = os.path.dirname(os.path.realpath(__file__))

with open(dir_path + "/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='tasrif',
    author='QCRI',
    author_email='uabbas@hbku.edu.qa',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/qcri/tasrif',
    version='0.0.6',
    packages=find_packages(),
    license=' BSD-3-Clause',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires='>= 3.7',
    install_requires=[
        'pandas >= 1.1.1',
        NUMPY_VERSION,
        'pyjq >= 2.5.1',
        'ummalqura >= 2.0.1',
        'scikit-learn >= 0.22.1',
        'tqdm >= 4.52.0',
        'ray >= 1.7.0',
        'dataprep >= 0.3.0'
    ],
    # numpy also needs to be specified in setup_requires,
    # see https://github.com/numpy/numpy/issues/2434#issuecomment-65252402
    setup_requires=[
        NUMPY_VERSION,
    ],
)

