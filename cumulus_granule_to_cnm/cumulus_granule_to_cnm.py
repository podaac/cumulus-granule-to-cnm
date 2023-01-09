import boto3
import os
from datetime import datetime
from cloudnotificationmessage import CloudNotificationMessage
from cumulus_logger import CumulusLogger
from cumulus_process import Process

logger = CumulusLogger('granule_to_cnm_logger')


class GranuleToCNM(Process):
    className = 'GranuleToCNM'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.logger.debug('{} Entered __init__', self.className)

        required = ['provider', 'provider_path']

        for requirement in required:
            if requirement not in self.config.keys():
                raise Exception(f'"{requirement}" config key is missing')

    # file name: YYYYMMDDHHMMSS-CollectionName-Report.txt
    def generate_report(self, bucket, collection_name, files):
        filename = datetime.now().strftime('%Y%m%d%H%M%S') + "-" + collection_name + "-Report.txt"
        f = open('/tmp/'+filename, 'a')
        for i in files:
            f.write(i + '\n')
        f.write('-- end of file --')
        f.close()

        # f = open(filename, "r")
        # print(f.read())

        s3 = boto3.resource('s3')
        self.logger.debug('generating report: {}/{}', bucket, 'temp/'+filename)
        s3.meta.client.upload_file('/tmp/'+filename, bucket, 'temp/'+filename)
        # response = s3.Bucket(bucket).upload_file('/tmp/'+filename, '/temp/'+filename)
        # self.logger.debug('upload response: {}', response)
        os.remove('/tmp/'+filename)


    def process(self):
        # Config Content
        meta_provider = self.config.get('provider', [])
        meta_provider_path = self.config.get('provider_path', [])
        meta_collection = self.config.get('collection')
        meta_cumulus = self.config.get('cumulus_meta')

        self.logger.debug('provider: {}', meta_provider)
        self.logger.debug('provider_path: {}', meta_provider_path)

        # Building the URI from info provided by provider since the granule itself might not have it
        uri = f'{meta_provider["protocol"]}://{meta_provider["host"]}{meta_provider_path}'

        cnm_list = []

        # if has granules, read first item and find collection
        if 'granules' not in self.input.keys():
            raise Exception(f'"granules" is missing from self.input')

        self.logger.debug('collection: {} | granules found: {}',
                          meta_collection.get('name'),
                          len(self.input['granules']))

        file_list = []

        for granule in self.input['granules']:
            self.logger.debug('granuleId: {}', granule['granuleId'])

            cnm_provider = meta_provider['id']
            cnm_dataset = granule['dataType']
            cnm_data_version = granule['version']
            cnm_files = []
            for file in granule['files']:
                cnm_granule_file = {
                    'type': file.get('type', '') or '',
                    'uri': uri + file.get('name', '') or '',
                    'size': file.get('size', '') or ''
                }
                cnm_files.append(cnm_granule_file)
                file_list.append(uri + file.get('name', '') or '')

            # Create CNM object
            cnm = CloudNotificationMessage(
                cnm_dataset, cnm_files, cnm_data_version, cnm_provider
            )

            # Extra metadata marking CNM is from discover granule
            cnm.message['meta'] = dict(
                source='discover_granule_queue',
                author='hryeung',
                contact='hong-kit.r.yeung@jpl.nasa.gov'
            )

            cnm_list.append(cnm.message)

        return_data = {
            "cnm_list": cnm_list
        }

        self.generate_report(meta_cumulus.get('system_bucket'), meta_collection.get('name'), file_list)

        return return_data


def lambda_handler(event, context):
    logger.setMetadata(event, context)
    return GranuleToCNM.cumulus_handler(event, context=context)
