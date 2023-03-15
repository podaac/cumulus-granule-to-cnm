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
                "system_bucket": "dummy_bucket"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            }
        },
        "task_config": {
            "collection": "{$.meta.collection}",
            "provider": "{$.meta.provider}",
            "provider_path": "{$.meta.provider_path}",
            "cumulus_meta": "{$.cumulus_meta}"
        }
    }
}

lambda_input2 = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "system_bucket": "dummy_bucket"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object_2.json",
                "TargetPath": "$"
            }
        },
        "task_config": {
            "collection": "{$.meta.collection}",
            "provider": "{$.meta.provider}",
            "provider_path": "{$.meta.provider_path}",
            "cumulus_meta": "{$.cumulus_meta}"
        }
    }
}

bad_lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "system_bucket": "dummy_bucket"
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
        "system_bucket": "dummy_bucket"
    },
    "exception": "None",
    "meta": {
        "collection": {
            "name": "VIIRS_NPP-NAVO-L2P-v3.0",
            "meta": {
                "provider_path": "/cumulus-test/gds2/NAVO/"
            }
        },
        "provider": {
            "globalConnectionLimit": 1,
            "host": "ops-metis.jpl.nasa.gov",
            "id": "podaac-test-sftp",
            "password": "password",
            "protocol": "sftp",
            "username": "cumulus-test"
        },
        "provider_path": "/cumulus-test/gds2/NAVO/"
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

s3_file_content_2 = {
      "cumulus_meta": {
        "cumulus_version": "11.1.7",
        "execution_name": "514d1636-e0be-129f-0005-2900e7b7b692",
        "message_source": "sfn",
        "queueExecutionLimits": {
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-background-job-queue": 200,
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-backgroundProcessing": 5,
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-big-background-job-queue": 20,
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-dmrpp-background-job-queue": 50,
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-forge-background-job-queue": 200,
          "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-tig-background-job-queue": 200
        },
        "state_machine": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverConvertPublishWorkflow",
        "system_bucket": "dummy_bucket",
        "workflow_start_time": 1675357242556,
        "queueUrl": "arn:aws:sqs:us-west-2:***REMOVED***:hryeung-ia-podaac-startSF"
      },
      "exception": "None",
      "meta": {
        "buckets": {
          "dashboard": {
            "name": "hryeung-ia-podaac-dashboard",
            "type": "private"
          },
          "ecco-staging": {
            "name": "podaac-ecco-v4r4",
            "type": "internal"
          },
          "glacier": {
            "name": "hryeung-ia-podaac-glacier",
            "type": "orca"
          },
          "internal": {
            "name": "hryeung-ia-podaac-internal",
            "type": "internal"
          },
          "podaac-dev-swot-simulated-ocean-l2-glorys": {
            "name": "podaac-dev-swot-simulated-ocean-l2-glorys",
            "type": "internal"
          },
          "podaac-dev-swot-simulated-ocean-l2-llc4320": {
            "name": "podaac-dev-swot-simulated-ocean-l2-llc4320",
            "type": "internal"
          },
          "pre-swot-staging": {
            "name": "podaac-dev-pre-swot-ocean-sim",
            "type": "internal"
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
          },
          "test": {
            "name": "podaac-dev-cumulus-test-input-v2",
            "type": "internal"
          },
          "test-staging": {
            "name": "podaac-sndbx-staging",
            "type": "internal"
          }
        },
        "cmr": {
          "clientId": "POCUMULUS",
          "cmrEnvironment": "UAT",
          "cmrLimit": 100,
          "cmrPageSize": 50,
          "oauthProvider": "earthdata",
          "passwordSecretName": "hryeung-ia-podaac-message-template-cmr-password20220811164219884600000007",
          "provider": "POCUMULUS",
          "username": "hkryeung"
        },
        "collection": {
          "createdAt": 1675293206971,
          "updatedAt": 1675293206971,
          "name": "SWOTCalVal_WM_GNSS_L0_Rec2",
          "sampleFileName": "SWOTCalVal_WM_GNSS_L0_Rec2_20220727T191701_20220727T192858_20220920T142800.xml",
          "version": "1",
          "duplicateHandling": "replace",
          "files": [
            {
              "bucket": "public",
              "regex": "^SWOTCalVal_WM_GNSS_L0_Rec2_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}\\.22_$",
              "sampleFileName": "SWOTCalVal_WM_GNSS_L0_Rec2_20220727T191701_20220727T192858_20220920T142800.22_",
              "type": "data",
              "reportToEms": "true"
            },
            {
              "bucket": "private",
              "regex": "^SWOTCalVal_WM_GNSS_L0_Rec2_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}\\.xml$",
              "sampleFileName": "SWOTCalVal_WM_GNSS_L0_Rec2_20220727T191701_20220727T192858_20220920T142800.xml",
              "type": "metadata",
              "reportToEms": "true"
            }
          ],
          "granuleId": "^SWOTCalVal_WM_GNSS_L0_Rec2_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}$",
          "granuleIdExtraction": "^(SWOTCalVal_WM_GNSS_L0_Rec2_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6}_[0-9]{8}T[0-9]{6})((\\.22_)|(\\.xml))?$",
          "meta": {
            "glacier-bucket": "hryeung-ia-podaac-glacier",
            "granuleRecoveryWorkflow": "OrcaRecoveryWorkflow",
            "rangeIs360": "true",
            "response-endpoint": [
              "arn:aws:sns:us-west-2:***REMOVED***:hryeung-ia-podaac-provider-response-sns"
            ],
            "workflowChoice": {
              "compressed": "false",
              "convertNetCDF": "false",
              "dmrpp": "false",
              "glacier": "false",
              "readDataFileForMetadata": "false"
            },
            "discover_tf": {
              "depth": 0,
              "force_replace": "true",
              "dir_reg_ex": ".*"
            },
            "provider_path": "temp/SWOT_CALVAL/"
          },
          "reportToEms": "true",
          "url_path": "{cmrMetadata.CollectionReference.ShortName}",
          "timestamp": 1675293207113
        },
        "distribution_endpoint": "https://jh72u371y2.execute-api.us-west-2.amazonaws.com:9000/DEV/",
        "launchpad": {
          "api": "https://api.launchpad.nasa.gov/icam/api/sm/v1",
          "certificate": "launchpad.pfx",
          "passphraseSecretName": "hryeung-ia-podaac-message-template-launchpad-passphrase20220811164323373000000013"
        },
        "provider": {
          "id": "PODAAC-INTERNAL-S3",
          "globalConnectionLimit": 1000,
          "protocol": "s3",
          "host": "hryeung-ia-podaac-internal"
        },
        "stack": "hryeung-ia-podaac",
        "template": "s3://hryeung-ia-podaac-internal/hryeung-ia-podaac/workflow_template.json",
        "workflow_name": "DiscoverConvertPublishWorkflow",
        "workflow_tasks": {
          "0": {
            "name": "hryeung-ia-podaac-discover-granules-tf-module",
            "version": "$LATEST",
            "arn": "arn:aws:lambda:us-west-2:***REMOVED***:function:hryeung-ia-podaac-discover-granules-tf-module"
          }
        },
        "retries": 0,
        "visibilityTimeout": 1800,
        "ingest_workflow_sns": "arn:aws:sns:us-west-2:***REMOVED***:hryeung-ia-podaac-provider-input-sns",
        "queueUrl": "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-provider-input-queue"
      },
      "payload": {
        "granules": [
          {
            "granuleId": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500",
            "dataType": "SWOTCalVal_WM_GNSS_L0_Rec2",
            "version": "1",
            "files": [
              {
                "name": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.22_",
                "path": "temp/SWOT_CALVAL/2022/07/29",
                "size": 97270412,
                "time": 1675703434126,
                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                "bucket": "hryeung-ia-podaac-public",
                "type": "data"
              },
              {
                "name": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.xml",
                "path": "temp/SWOT_CALVAL/2022/07/29",
                "size": 8438,
                "time": 1675703434126,
                "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                "bucket": "hryeung-ia-podaac-private",
                "type": "metadata"
              }
            ]
          }
        ]
      }
    }

s3_file_content_zero_file_size = {
    "cumulus_meta": {
        "cumulus_version": "9.9.0",
        "message_source": "sfn",
        "system_bucket": "dummy_bucket"
    },
    "exception": "None",
    "meta": {
        "collection": {
            "name": "VIIRS_NPP-NAVO-L2P-v3.0",
            "meta": {
                "provider_path": "/cumulus-test/gds2/NAVO/"
            }
        },
        "provider": {
            "globalConnectionLimit": 1,
            "host": "ops-metis.jpl.nasa.gov",
            "id": "podaac-test-sftp",
            "password": "password",
            "protocol": "sftp",
            "username": "cumulus-test"
        },
        "provider_path": "/cumulus-test/gds2/NAVO/"
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
                        "size": 0,
                        "time": 1641930305000,
                        "bucket": "hryeung-ia-podaac-protected",
                        "url_path": "{cmrMetadata.CollectionReference.ShortName}",
                        "type": "data"
                    },
                    {
                        "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                        "path": "/cumulus-test/gds2/NAVO",
                        "size": 0,
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

sample_cnm_calval = {
                "version": "1.6.0",
                "provider": "PODAAC-INTERNAL-S3",
                "collection": "SWOTCalVal_WM_GNSS_L0_Rec2",
                "submissionTime": "2023-02-06T20:40:07.190298Z",
                "identifier": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500",
                "product": {
                    "name": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500",
                    "files": [
                        {
                            "type": "data",
                            "uri": "s3://hryeung-ia-podaac-internal/temp/SWOT_CALVAL/2022/07/29/SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.22_",
                            "size": 97270412,
                            "name": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.22_"
                        },
                        {
                            "type": "metadata",
                            "uri": "s3://hryeung-ia-podaac-internal/temp/SWOT_CALVAL/2022/07/29/SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.xml",
                            "size": 8438,
                            "name": "SWOTCalVal_WM_GNSS_L0_Rec2_20220729T222100_20220730T023300_20220927T221500.xml"
                        }
                    ],
                    "dataVersion": "1"
                },
                "meta": {
                    "source": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverConvertPublishWorkflow",
                    "author": "hryeung",
                    "contact": "hong-kit.r.yeung@jpl.nasa.gov",
                    "execution_name": "514d1636-e0be-129f-0005-2900e7b7b692"
                }
            }


def test_version():
    assert __version__ == '0.2.0'


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
def test_granule_to_cnm_translation_zero_size():

    # Fake aws s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content_zero_file_size), Bucket=test_bucket_name, Key=test_bucket_key)

    response = {}
    try:
        response = lambda_handler(lambda_input, {})
    except Exception as e:
        print(e)

    size = response['payload']['cnm_list'][0]['product']['files'][0]['size']
    assert len(response['payload']['cnm_list']) is 1
    assert size is 0
    assert isinstance(size, int)
    # assert response['payload']['cnm_list'][0]['product'] == sample_cnm['product']


@mock_s3
def test_granule_to_cnm_translation_swot_calval():

    # Fake aws s3 bucket
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content_2), Bucket=test_bucket_name, Key=test_bucket_key)

    response = {}
    try:
        response = lambda_handler(lambda_input, {})
    except Exception as e:
        print(e)

    assert len(response['payload']['cnm_list']) is 1
    assert response['payload']['cnm_list'][0]['product'] == sample_cnm_calval['product']


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
