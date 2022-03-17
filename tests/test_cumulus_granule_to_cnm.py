import json
import boto3
import pytest

from cumulus_granule_to_cnm import __version__
from cumulus_granule_to_cnm.cumulus_granule_to_cnm import lambda_handler
from moto import mock_s3

# Tests
# input full normal CMA object, scan and get list of granules, convert said granules to CNM and count 7

lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "queueExecutionLimits": {
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-background-job-queue": 200,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-backgroundProcessing": 5,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-big-background-job-queue": 20,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-forge-background-job-queue": 200,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-tig-background-job-queue": 200
                },
                "state_machine": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverWorkflow",
                "system_bucket": "dummy_bucket",
                "queueUrl": "arn:aws:sqs:us-west-2:***REMOVED***:hryeung-ia-podaac-startSF"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            },
            "task_config": {
                "provider": "{$.meta.provider}",
                "provider_path": "{$.meta.provider_path}"
            }
        }
    }
}

bad_lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "queueExecutionLimits": {
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-background-job-queue": 200,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-backgroundProcessing": 5,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-big-background-job-queue": 20,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-forge-background-job-queue": 200,
                    "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-tig-background-job-queue": 200
                },
                "state_machine": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverWorkflow",
                "system_bucket": "dummy_bucket",
                "queueUrl": "arn:aws:sqs:us-west-2:***REMOVED***:hryeung-ia-podaac-startSF"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            }
        }
    }
}

s3_file_content = {
    "cumulus_meta": {
        "cumulus_version": "9.9.0",
        "message_source": "sfn",
        "queueExecutionLimits": {
            "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-background-job-queue": 200,
            "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-backgroundProcessing": 5,
            "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-big-background-job-queue": 20,
            "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-forge-background-job-queue": 200,
            "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-tig-background-job-queue": 200
        },
        "state_machine": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverWorkflow",
        "system_bucket": "dummy_bucket",
        "queueUrl": "arn:aws:sqs:us-west-2:***REMOVED***:hryeung-ia-podaac-startSF"
    },
    "exception": "None",
    "meta": {
        "buckets": {
            "dashboard": {
                "name": "hryeung-ia-podaac-dashboard",
                "type": "public"
            },
            "internal": {
                "name": "dummy_bucket",
                "type": "internal"
            },
            "orca_default": {
                "name": "hryeung-ia-podaac-archive",
                "type": "orca"
            },
            "private": {
                "name": "hryeung-ia-podaac-private",
                "type": "private"
            },
            "protected": {
                "name": "hryeung-ia-podaac-protected",
                "type": "protected"
            },
            "public": {
                "name": "hryeung-ia-podaac-public",
                "type": "public"
            }
        },
        "collection": {
            "duplicateHandling": "replace",
            "files": [
                {
                    "bucket": "protected",
                    "regex": "^[0-9]{14}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0\\.nc$",
                    "sampleFileName": "20210101000119-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                    "type": "data"
                },
                {
                    "bucket": "public",
                    "regex": "^[0-9]{14}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0\\.nc\\.md5$",
                    "sampleFileName": "20210101000119-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                    "type": "metadata"
                },
                {
                    "bucket": "private",
                    "regex": "^[0-9]{14}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0\\.cmr\\.json$",
                    "sampleFileName": "20210101000119-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.cmr.json",
                    "type": "metadata"
                },
                {
                    "bucket": "protected",
                    "regex": "^[0-9]{14}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0\\.nc\\.dmrpp$",
                    "sampleFileName": "20210101000119-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.dmrpp",
                    "type": "metadata"
                }
            ],
            "granuleId": "^20220111135[0-9]{3}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0$",
            "granuleIdExtraction": "^(20220111135[0-9]{3}-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\\.0-fv03\\.0)((\\.nc)|(\\.nc\\.md5)|(\\.cmr\\.json)|(\\.nc\\.dmrpp))?$",
            "meta": {
                "glacier-bucket": "hryeung-ia-podaac-glacier",
                "granuleRecoveryWorkflow": "DrRecoveryWorkflow",
                "response-endpoint": [
                    "arn:aws:sns:us-west-2:***REMOVED***:hryeung-ia-podaac-provider-response-sns"
                ],
                "workflowChoice": {
                    "compressed": False,
                    "convertNetCDF": False,
                    "dmrpp": True,
                    "glacier": True,
                    "readDataFileForMetadata": True
                }
            },
            "name": "VIIRS_NPP-NAVO-L2P-v3.0",
            "sampleFileName": "20210101000119-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
            "url_path": "{cmrMetadata.CollectionReference.ShortName}",
            "version": "3.0"
        },
        "provider": {
            "globalConnectionLimit": 1,
            "host": "ops-metis.jpl.nasa.gov",
            "id": "podaac-test-sftp",
            "password": "password",
            "protocol": "sftp",
            "username": "cumulus-test"
        },
        "provider_path": "/cumulus-test/gds2/NAVO/",
        "workflow_tasks": {
            "0": {
                "name": "hryeung-ia-podaac-DiscoverGranules",
                "version": "$LATEST",
                "arn": "arn:aws:lambda:us-west-2:***REMOVED***:function:hryeung-ia-podaac-DiscoverGranules"
            }
        }
    },
    "payload": {
        "granules": [
            {
                "granuleId": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18167706,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18294159,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 17146221,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 18654568,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 17730761,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 16983663,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            },
            {
                "granuleId": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "dataType": "VIIRS_NPP-NAVO-L2P-v3.0",
                "version": "3.0",
                "files": [
                    {
                        "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 16316733,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 97,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-public",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "metadata"
                    }
                ]
            }
        ]
    }
}

sample_cnm = {
    "version": "1.5.1",
    "provider": "PODAAC",
    "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
    "submissionTime": "2022-03-16T22:12:08.257529",
    "identifier": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
    "product": {
        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
        "files": [
            {
                "type": "data",
                "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                "size": 18167706,
                "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
            },
            {
                "type": "metadata",
                "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                "size": 97,
                "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
            }
        ],
        "dataVersion": "3.0"
    }
}

def test_version():
    assert __version__ == '0.1.0'


@mock_s3
def test_granule_to_cnm_translation():

    # Fake aws s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    response = {}
    try:
        response = lambda_handler(lambda_input, {})
    except Exception as e:
        print(e)

    assert len(response['payload']['cnm_list']) is 7
    assert response['payload']['cnm_list'][0]['product'] == sample_cnm['product']


@mock_s3
def test_granule_to_cnm_cumulus_process_catches_missing_config():
    """Expect error raised due to missing task config"""

    # Fake aws s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    with pytest.raises(Exception) as exc_info:
        response = lambda_handler(bad_lambda_input, {})

    assert "config key is missing" in str(exc_info.value)
