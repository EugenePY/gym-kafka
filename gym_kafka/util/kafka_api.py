import json
import uuid

import jsonschema
from kafka import KafkaConsumer, KafkaProducer


class KafkaAPI(object):

    def __init__(self, schemas, message_store):
        self.schemas = schemas
        self.msg_store = message_store

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

    def send_msg(self, topic, msg):
        schema = self._infer_outgoing_msg_schema(msg)
        jsonschema.validate(msg, schema)
        self._assert_initialization()
        self._kafka_producer.send(topic=topic, value=msg)

    def flush_msgs(self):
        self._assert_initialization()
        self._kafka_producer.flush()

    def get_reply(self, req_id, timeout_ms=1000):
        while req_id not in self.msg_store:
            msgs = self._poll_msgs(timeout_ms)

            assert msgs is not None, "Gym failed to get a response for {} within the " \
                                     "configured time out period of {} ms".format(req_id, timeout_ms)

            msgs = self._unpack_msgs(msgs)
            self.msg_store.put_all(msgs)
        return self.msg_store.get(req_id)

    def close(self):
        self._kafka_producer.close(timeout=None)
        self._kafka_consumer.close(autocommit=True)

    def _poll_msgs(self, timeout_ms):
        self._assert_initialization()
        self._assert_subscription()
        return self._kafka_consumer.poll(timeout_ms=timeout_ms)

    @staticmethod
    def _unpack_msgs(msgs):
        return [r.value for records in msgs.values() for r in records]

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


class MSGStore(object):

    def __init__(self):
        self._msgs = dict()

    def __contains__(self, item):
        return item in self._msgs

    def put_all(self, msgs):
        for msg in msgs:
            msg = self._extract_body(msg)
            assert msg['regID'] not in self._msgs, "Reply to message with reqID {}" \
                                                   " already exists in the message store".format(msg['reqID'])
            self._msgs[msg['regID']] = msg

    def get(self, req_id):
        return self._msgs.pop(req_id)

    @staticmethod
    def _extract_body(msg):
        return msg.pop('body')
