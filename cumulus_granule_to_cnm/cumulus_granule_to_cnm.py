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

    def process(self):
        # Config Content
        meta_provider = self.config.get('provider', [])
        meta_collection = self.config.get('collection')
        meta_cumulus = self.config.get('cumulus_meta')

        self.logger.debug('provider: {}', meta_provider)

        # Building the URI from info provided by provider since the granule itself might not have it
        uri = f'{meta_provider["protocol"]}://{meta_provider["host"]}/'

        cnm_list = []

        # if has granules, read first item and find collection
        if 'granules' not in self.input.keys():
            raise Exception('"granules" is missing from self.input')

        self.logger.debug('collection: {} | granules found: {}',
                          meta_collection.get('name'),
                          len(self.input['granules']))

        for granule in self.input['granules']:
            self.logger.debug('granuleId: {}', granule['granuleId'])

            cnm_provider = meta_provider['id']
            cnm_dataset = granule['dataType']
            cnm_data_version = granule['version']
            cnm_files = []
            for file in granule['files']:
                cnm_granule_file = {
                    'type': file.get('type', '') or '',
                    'uri': uri + (file.get('path', '')).lstrip('/') + '/' + file.get('name', '') or '',
                    'size': file.get('size', 0) or 0
                }
                cnm_files.append(cnm_granule_file)

            # Create CNM object
            cnm = CloudNotificationMessage(
                cnm_dataset, cnm_files, cnm_data_version, cnm_provider, granule_name=granule['granuleId']
            )

            # Extra metadata marking CNM is from discover granule
            cnm.message['meta'] = {
                "source": meta_cumulus.get('state_machine'),
                "execution_name": meta_cumulus.get('execution_name')
            }

            cnm_list.append(cnm.message)

        return_data = {
            "cnm_list": cnm_list
        }

        return return_data


def lambda_handler(event, context):
    logger.setMetadata(event, context)
    return GranuleToCNM.cumulus_handler(event, context=context)
