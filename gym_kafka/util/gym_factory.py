import json

from gym import spaces

from . import KafkaAPI, MSGParser, MSGStore

PARSERS = {'simple': MSGParser}

SPACES = {'box': spaces.Box}


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

    def make_message_parser(self):
        name = self.config['parser']['name']
        props = self.config['message_properties']
        return PARSERS[self.config['type']](name=name, message_properties=props)

    def make_action_space(self):
        space_config = self.config['action_space']
        return self._make_space(space_config)

    def make_observation_space(self):
        space_config = self.config['observation_space']
        return self._make_space(space_config)

    @staticmethod
    def _make_space(config):
        space_type = config.pop('type')
        return SPACES[space_type](**config)
