#!/bin/sh
poetry build
poetry run pip install --upgrade -t package dist/*.whl
cd package ; zip -r ../artifact.zip . -x '*.pyc'
cd ..
#code_version=`python -c 'from cumulus_granule_to_cnm import __version__; print(__version__)'`
code_version=`poetry version | awk '{print $2}'`
cp artifact.zip artifact-$code_version.zip
echo \*\* artifact-$code_version.zip created \*\*