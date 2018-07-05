import json
from . import KafkaAPI, MSGAssembler, MSGStore


class GymFactory(object):

    def __init__(self, path_to_config):
        self.config = json.load(path_to_config)

    def make_messaging_api(self):
        schemas = {}
        for schema, schema_file in self.config['message_schemas'].items():
            schemas[schema] = json.load(schema_file)

        api = KafkaAPI(schemas=schemas, message_store=MSGStore())
        api.init_messaging_api(subscribe_to=self.config['pup_sub_properties']['subscription_topics'],
                               bootstrap_servers=self.config['pup_sub_properties']['bootstrap_servers'])
        return api

    def make_message_assembler(self):
        return MSGAssembler(name='RL Gym', message_properties=self.config['message_properties'])
