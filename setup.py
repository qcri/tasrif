"""Module to setup the library. All library dependencies are added here.
"""
import sys
from setuptools import setup, find_packages


print(find_packages(), file=sys.stderr)

setup(
    name='tasrif',
    version='0.1',
    packages=find_packages(),
    python_requires='>= 3.7',
    install_requires=[
        'pandas >= 1.1.1',
        'numpy >= 1.19.5',
        'pyjq >= 2.5.1',
        'matplotlib >= 3.0.0',
        'ummalqura>=2.0.1',
    ],
    # numpy also needs to be specified in setup_requires,
    # see https://github.com/numpy/numpy/issues/2434#issuecomment-65252402
    setup_requires=[
        'numpy >= 1.19.5',
    ],
    extras_require={
        "Kats":  ["Kats>=0.1.0"],
    }
)
