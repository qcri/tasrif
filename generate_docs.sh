#!/bin/bash

# exit when any command fails
set -e

rm -rf docs-generator/*
sphinx-apidoc -o docs-generator tasrif sphinx-apidoc --full -M -e -H 'Tasrif toolkit' -A 'Qatar Computing Research Institute, HBKU' --templatedir docs/_templates


cd docs-generator
mkdir _templates/autosummary
mkdir -p _static/img
mkdir _static/css

cp -r ../docs/_templates/autosummary/*.rst _templates/autosummary/
cp ../docs/_templates/layout.html _templates/layout.html
cp ../docs/index.rst .
cp ../docs/_static/img/logo.png _static/img/logo.png
cp ../docs/_static/css/custom.css _static/css/custom.css

echo "

extensions.extend(['sphinx.ext.napoleon', 'sphinx.ext.autosummary', 'sphinx_autopackagesummary'])

napolean_google_docstring = False

# Adds method descriptions
autosummary_generate = True

autodoc_default_options = {
	'exclude-members': '__weakref__,Default,Defaults'
}

html_theme = 'pydata_sphinx_theme'
html_logo = '../docs/_static/img/logo.png'
autodoc_member_order = 'bysource'

html_css_files = [
    'css/custom.css',
]

" >> conf.py

make html
