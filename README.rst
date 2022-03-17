====
cumulus-granule-to-cnm
====
This is the python code for the lambda `cumulus-granule-to-cnm`.
It takes in a list of Granules and converts each one to a CNM (Cloud Notification Mechanism) and returns said list.

Build (as a zip to load to AWS Lambda)
====
`This <https://chariotsolutions.com/blog/post/building-lambdas-with-poetry/>`_ page contains some good info on the overall "building a zip with poetry that's compatible with AWS Lambda".

Auto build script
----
TL;DR ::

    chmod u+x build.sh;
    ./build.sh;
    chmod u-x build.sh;

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

Testing
====
Run the ``poetry run pytest tests -v`` command; you should see a similar output::

    platform darwin -- Python 3.8.9, pytest-7.1.0, pluggy-1.0.0 -- /Users/hryeung/Library/Caches/pypoetry/virtualenvs/cumulus-granule-to-cnm-iV9scENW-py3.8/bin/python
    cachedir: .pytest_cache
    rootdir: /Users/hryeung/PycharmProjects/jpl/cumulus-granule-to-cnm
    collected 3 items

    tests/test_cumulus_granule_to_cnm.py::test_version PASSED                                                 [ 33%]
    tests/test_cumulus_granule_to_cnm.py::test_granule_to_cnm_translation PASSED                              [ 66%]
    tests/test_cumulus_granule_to_cnm.py::test_granule_to_cnm_cumulus_process_catches_missing_config PASSED   [100%]

        ====== 3 passed in 0.99s ======

Notes
====
Currently as of 2022-03-17 we're doing a manual pull of the CNM schema (cumulus_cloud_notification_message folder, sourced from https://github.com/podaac/cloud-notification-message-schema/tree/develop) and once it's available on pypi we'll convert to use that.