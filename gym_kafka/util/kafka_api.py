import json
import uuid

import jsonschema

from kafka import KafkaConsumer, KafkaProducer


class KafkaAPI(object):

    def __init__(self, schemas):
        self.schemas = schemas

        self._kafka_consumer = None
        self._kafka_producer = None
        self.is_initialised = False
        self.has_subscribed = False

    def init_messaging_api(self, subscribe_to=None, bootstrap_servers=None):
        self._kafka_consumer = KafkaConsumer(*subscribe_to,
                                             bootstrap_servers=bootstrap_servers,
                                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self._kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        self.is_initialised = True
        if not self._kafka_consumer.topics():
            raise ConnectionError("You must specify topics to subscribe to in your call of function "
                                  "'init_messaging_api()'.")
        else:
            self.has_subscribed = True

    def poll_msgs(self):
        self._assert_initialization()
        self._assert_subscription()
        return self._kafka_consumer.poll(timeout_ms=1000)

    def send_msg(self, topic, msg):
        schema = self._infer_outgoing_msg_schema(msg)
        jsonschema.validate(msg, schema)
        self._assert_initialization()
        self._kafka_producer.send(topic=topic, value=msg)

    def flush_msgs(self):
        self._assert_initialization()
        self._kafka_producer.flush()

    def close(self):
        self._kafka_producer.close(timeout=None)
        self._kafka_consumer.close(autocommit=True)

    def _infer_outgoing_msg_schema(self, msg):
        msg_type = msg['body']['_type']
        schema_key = '{}_outgoing'.format(msg_type)
        if schema_key in self.schemas:
            return self.schemas[schema_key]
        else:
            raise ValueError('Cannot find a corresponding schema for message type {}'.format(msg_type))

    def _assert_initialization(self):
        if not self.is_initialised:
            raise ConnectionError("You must call function 'init_messaging_api()' first.")

    def _assert_subscription(self):
        if not self.has_subscribed:
            raise ConnectionError("You must specify topics to subscribe to in your call of function "
                                  "'init_messaging_api()'.")


class MSGAssembler(object):
    def __init__(self, name, message_properties):
        self.name = name
        self.msg_props = message_properties

    def assemble_action_msg(self, action):
        return self._assemble_msg('action_outgoing', {'action': action})

    def assemble_reset_msg(self):
        return self._assemble_msg('reset_outgoing', {'request': 'reset'})

    def assemble_observation_msg(self):
        return self._assemble_msg('observation_outgoing', {'request': 'observation'})

    def assemble_reward_msg(self):
        return self._assemble_msg('reward_outgoing', {'request': 'reward'})

    def _assemble_msg(self, type, msg_payload):
        msg = self.msg_props[type].copy()
        msg = self._parse_in_header_info(msg)
        msg['body'].update(msg_payload)
        topic = msg.pop('topic')
        return topic, msg

    def _parse_in_header_info(self, msg):
        msg['header']['from'] = self.name
        msg['header']['reqID'] = str(uuid.uuid4())
        return msg
