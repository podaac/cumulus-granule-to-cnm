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
                raise Exception(f'{requirement} config key is missing')

    def process(self):
        # Config Content
        meta_provider = self.config.get('provider', [])
        meta_provider_path = self.config.get('provider_path', [])

        self.logger.debug('provider: {}', meta_provider)
        self.logger.debug('provider_path: {}', meta_provider_path)

        # Building the URI from info provided by provider since the granule itself might not have it
        uri = f'{meta_provider["protocol"]}://{meta_provider["host"]}{meta_provider_path}'

        cnm_list = []

        self.logger.debug('total number of granules found: {}', len(self.input['granules']))

        for i in self.input['granules']:
            granule = i

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

            # Create CNM object
            cnm = CloudNotificationMessage(
                cnm_dataset, cnm_files, cnm_data_version, cnm_provider
            )
            cnm_list.append(cnm.message)

        return_data = {
            "cnm_list": cnm_list
        }
        return return_data


def lambda_handler(event, context):
    logger.setMetadata(event, context)
    return GranuleToCNM.cumulus_handler(event, context=context)
