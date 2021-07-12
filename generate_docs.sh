
#!/bin/bash

# exit when any command fails
set -e

export MYHEARTCOUNTS_HEALTHKITDATA_CSV_FOLDER_PATH="/path/to/myheartcounts/"
export MYHEARTCOUNTS_SIXMINUTEWALKACTIVITY_JSON_FOLDER_PATH="/path/to/myheartcounts"

rm -rf docs-generator
sphinx-apidoc -o docs-generator tasrif sphinx-apidoc --full -M -e -H 'Tasrif toolkit' -A 'Qatar Computing Research Institute, HBKU' --templatedir docs/_templates


cd docs-generator
mkdir _templates/autosummary

cp -r ../docs/_templates/autosummary/*.rst _templates/autosummary/
cp ../docs/index.rst .

echo "

extensions.extend(['sphinx.ext.napoleon', 'sphinx.ext.autosummary', 'sphinx_autopackagesummary'])

napolean_google_docstring = False

# Adds method descriptions
autosummary_generate = True

autodoc_default_options = {
	'exclude-members': '__weakref__,Default,Defaults'
}

html_theme = 'pydata_sphinx_theme'
# html_logo = '_static/img/logo.svg'
autodoc_member_order = 'bysource'
" >> conf.py



make html

open _build/html/index.html