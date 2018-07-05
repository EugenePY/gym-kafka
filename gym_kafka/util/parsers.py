import uuid
from numpy import asarray


class MSGParser(object):
    def __init__(self, name, message_properties):
        self.name = name
        self.msg_props = message_properties

    def assemble_action_msg(self, action):
        return self._assemble_msg('action_outgoing', {'request': action})

    def assemble_reset_msg(self):
        return self._assemble_msg('reset_outgoing', {})

    def assemble_observation_msg(self):
        return self._assemble_msg('observation_outgoing', {})

    def assemble_reward_msg(self):
        return self._assemble_msg('reward_outgoing', {})

    @staticmethod
    def parse_response(msg):
        if msg['_type'] == 'reward':
            return msg['response'][0]
        else:
            return asarray(msg['response']), msg['done']

    def _assemble_msg(self, type, msg_payload):
        msg = self.msg_props[type].copy()
        msg = self._parse_in_header_info(msg)
        msg['body'].update(msg_payload)
        topic = msg.pop('topic')
        return topic, msg['header']['reqID'], msg

    def _parse_in_header_info(self, msg):
        msg['header']['from'] = self.name
        msg['header']['reqID'] = str(uuid.uuid4())
        return msg
