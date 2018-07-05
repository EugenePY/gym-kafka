import json
from . import KafkaAPI


class GymFactory(object):
    @classmethod
    def make_messaging_api(cls, path_to_config):
        config = json.load(path_to_config)
        api = KafkaAPI()
        api.init_messaging_api()
        return api
