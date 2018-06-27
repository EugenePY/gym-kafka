import json

from kafka import KafkaConsumer, KafkaProducer


class KafkaAPI(object):

    def __init__(self):
        self._kafka_consumer = None
        self._kafka_producer = None
        self.is_initialised = False
        self.has_subscribed = False

    def init_messaging_api(self, subscribe_to=None):
        self._kafka_consumer = KafkaConsumer(*subscribe_to,
                                             bootstrap_servers=['localhost:9092'],
                                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self._kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
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

    def send_msg(self, topic, value):
        self._assert_initialization()
        self._kafka_producer.send(topic=topic, value=value)

    def flush_msgs(self):
        self._assert_initialization()
        self._kafka_producer.flush()

    def _assert_initialization(self):
        if not self.is_initialised:
            raise ConnectionError("You must call function 'init_messaging_api()' first.")

    def _assert_subscription(self):
        if not self.has_subscribed:
            raise ConnectionError("You must specify topics to subscribe to in your call of function "
                                  "'init_messaging_api()'.")
