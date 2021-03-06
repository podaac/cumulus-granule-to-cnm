====
cumulus-granule-to-cnm
====
This is the python code for the lambda `cumulus-granule-to-cnm`.
It takes in a list of Granules and converts each one to a CNM (Cloud Notification Mechanism) and returns said list.

Required input
====
This function is intended to be used as a cumulus task and this requires ``provider`` and ``provider_path`` inputs from the ``task_config``
::
    "task_config": {
        "provider": "{$.meta.provider}",
        "provider_path": "{$.meta.provider_path}"
    }


Build (as a zip to load to AWS Lambda)
====
`This <https://chariotsolutions.com/blog/post/building-lambdas-with-poetry/>`_ page contains some good info on the overall "building a zip with poetry that's compatible with AWS Lambda".

Auto build script
----
TL;DR ::

    sh ./build.sh;

to run the following commands in order and build the artifact.zip

Manual build
----
This command creates the ``dist/`` folder::

    poetry build

This command downloads the dependency files from the just created .whl file, along with the lambda_handler function in ``cumulus_publish_cnm/cumulus_publish_cnm.py``, and places them in the ``package`` folder::

    poetry run pip install --upgrade -t package dist/*.whl

Note that **boto3** and **moto** are not being pulled in. I've specifically excluded them via having them as ``tool.poetry.dev-dependencies`` only (via the ``pyproject.toml`` file)

The last command used is::

    cd package ; zip -r ../artifact.zip . -x '*.pyc'

Which zips and creates the ``artifact.zip`` file, containing all files found in ``package/`` excluding .pyc files

Then upload ``artifact.zip`` to any location you plan to use it

* Upload directly as a lambda with ``aws lambda update-function-code``
* Upload to your AWS S3 ``lambdas/`` folder so that your Cumulus Terraform Build can use it

Testing & Coverage
====
Run the ``poetry run pytest --cov cumulus_granule_to_cnm tests -v`` command; you should see a similar output::

    platform darwin -- Python 3.8.9, pytest-7.1.0, pluggy-1.0.0 -- /Users/hryeung/Library/Caches/pypoetry/virtualenvs/cumulus-granule-to-cnm-iV9scENW-py3.8/bin/python
    cachedir: .pytest_cache
    rootdir: /Users/hryeung/PycharmProjects/jpl/cumulus-granule-to-cnm
    collected 3 items

    tests/test_cumulus_granule_to_cnm.py::test_version PASSED                                                 [ 33%]
    tests/test_cumulus_granule_to_cnm.py::test_granule_to_cnm_translation PASSED                              [ 66%]
    tests/test_cumulus_granule_to_cnm.py::test_granule_to_cnm_cumulus_process_catches_missing_config PASSED   [100%]

    ---------- coverage: platform darwin, python 3.8.9-final-0 -----------
    Name                                               Stmts   Miss  Cover
    ----------------------------------------------------------------------
    cumulus_granule_to_cnm/__init__.py                     1      0   100%
    cumulus_granule_to_cnm/cumulus_granule_to_cnm.py      38      0   100%
    ----------------------------------------------------------------------
    TOTAL                                                 39      0   100%

        ====== 3 passed in 0.99s ======

