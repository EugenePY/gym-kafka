import pprint

import gym
from gym.utils import seeding

from gym_kafka.util import GymFactory


class KafkaEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        factory = GymFactory('gym_kafka/config.json')
        self.messaging_api = factory.make_messaging_api()
        self.msg_parser = factory.make_message_parser()

        self.action_space = factory.make_action_space()
        self.observation_space = factory.make_observation_space()

        self.last_obs = None
        self.seed()
        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        assert self.action_space.contains(action)

        action_topic, msg_id, msg = self.msg_parser.assemble_action_msg(action)
        self.messaging_api.send_msg(action_topic, msg)

        reward_topic, msg_id, msg = self.msg_parser.assemble_reward_msg()
        reward = self._send_and_get_reply(reward_topic, msg_id, msg)

        obs_topic, msg_id, msg = self.msg_parser.assemble_observation_msg()
        obs, done = self._send_and_get_reply(obs_topic, msg_id, msg)

        self.last_obs = obs
        return obs, reward, done, {}

    def _send_and_get_reply(self, topic, msg_id, msg):
        self.messaging_api.send_msg(topic, msg)
        reply = self.messaging_api.get_reply(msg_id, timeout_ms=5000)
        return self.msg_parser(reply)

    def reset(self):
        reset_topic, msg_id, msg = self.msg_parser.assemble_reset_msg()
        self._send_and_get_reply(reset_topic, msg_id, msg)

        obs_topic, msg_id, msg = self.msg_parser.assemble_observation_msg()
        obs = self._send_and_get_reply(obs_topic, msg_id, msg)

        self.last_obs = obs
        return obs

    def render(self, mode='human', close=False):
        pprint.pprint(self.last_obs)

    def close(self):
        self.messaging_api.close()
