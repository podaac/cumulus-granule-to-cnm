import boto3
import json
from cumulus_logger import CumulusLogger
from cumulus_process import Process
from cumulus_cloud_notification_message.cloudnotificationmessage import CloudNotificationMessage

logger = CumulusLogger('granule_to_cnm_logger')

'''
Receive a CMA that contains the replace key, which would contain a list of granules, which would be part of payload
for each of the granules in the list, generate a CNM (Cloud notification mechanism) version and append to a list
upload list to S3 events and return the CMA JSON with new set of data in replace key
'''


class GranuleToCNM(Process):
	className = 'cumulusGranuleToCNM'

	def __init__(self, *args, **kwargs):
		super(GranuleToCNM, self).__init__(*args, **kwargs)
		self.logger = logger
		self.logger.debug('{} Entered __init__', self.className)

	def process(self):
		# Stealing data from logger to get `meta` content (mainly for provider and provider path)
		# This is actually awful since it's calling S3 2 times... (once for this once as part of the framework)
		s3 = boto3.resource('s3')
		bucket = self.logger.event['cma']['event']['replace']['Bucket']
		file = self.logger.event['cma']['event']['replace']['Key']
		content_object = s3.Object(bucket, file)
		file_content = content_object.get()['Body'].read().decode('utf-8')
		json_content = json.loads(file_content)
		meta_provider = json_content['meta']['provider']
		meta_provider_path = json_content['meta']['provider_path']

		uri = f'{meta_provider["protocol"]}://{meta_provider["host"]}{meta_provider_path}'

		cnm_list = []

		for i in self.input['granules']:
			granule = i
			# print(json.dumps(granule, indent=4))

			cnm_provider = 'PODAAC'
			cnm_dataset = granule['dataType']
			cnm_data_version = granule['version']
			cnm_files = []
			data_file = granule['files'][0]
			metadata_file = granule['files'][1]
			cnm_granule_data = {
				'type': data_file.get('type', '') or '',
				'uri': uri + data_file.get('name', '') or '',
				'size': data_file.get('size', '') or ''
			}
			cnm_granule_metadata = {
				'type': metadata_file.get('type', '') or '',
				'uri': uri + metadata_file.get('name', '') or '',
				'size': metadata_file.get('size', '') or ''
			}
			cnm_files.append(cnm_granule_data)
			cnm_files.append(cnm_granule_metadata)
			cnm = CloudNotificationMessage(
				cnm_dataset, cnm_files, cnm_data_version, cnm_provider
			)
			cnm_list.append(cnm.message)
			# data = json.loads(test_cnm.get_json())
			# print(json.dumps(data, indent=4))
		return_data = {
			"cnm_list": cnm_list
		}
		# print(return_data)
		return return_data


def lambda_handler(event, context):
	logger.setMetadata(event, context)
	return GranuleToCNM.cumulus_handler(event, context=context)


# input_test = {
#     "cma": {
#         "ReplaceConfig": {
#             "FullMessage": "True"
#         },
#         "event": {
#             "cumulus_meta": {
#                 "cumulus_version": "9.9.0",
#                 "message_source": "sfn",
#                 "queueExecutionLimits": {
#                     "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-background-job-queue": 200,
#                     "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-backgroundProcessing": 5,
#                     "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-big-background-job-queue": 20,
#                     "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-forge-background-job-queue": 200,
#                     "https://sqs.us-west-2.amazonaws.com/***REMOVED***/hryeung-ia-podaac-tig-background-job-queue": 200
#                 },
#                 "state_machine": "arn:aws:states:us-west-2:***REMOVED***:stateMachine:hryeung-ia-podaac-DiscoverWorkflow",
#                 "system_bucket": "hryeung-ia-podaac-internal",
#                 "queueUrl": "arn:aws:sqs:us-west-2:***REMOVED***:hryeung-ia-podaac-startSF"
#             },
#             "replace": {
#                 "Bucket": "hryeung-ia-podaac-internal",
#                 "Key": "events/eb481fe4-2299-4f31-b697-16862060dde6",
#                 "TargetPath": "$"
#             }
#         }
#     }
# }
#
# print(lambda_handler(input_test, ''))
