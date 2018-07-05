import gym
from gym.utils import seeding

from gym_kafka.util import GymFactory


class KafkaEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        factory = GymFactory('gym_kafka/config.json')
        self.messaging_api = factory.make_messaging_api()
        self.msg_assembler = factory.make_message_assembler()

        # TODO initialize action and observation space
        self.action_space = None
        self.observation_space = None

        self.time = 0
        self.last_obs = None
        self.seed()
        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        assert self.action_space.contains(action)
        self.time += 1

        action_topic, msg = self.msg_assembler.assemble_action_msg(action)
        self.messaging_api.send_msg(action_topic, msg)

        reward_topic, msg = self.msg_assembler.assemble_reward_msg()
        self.messaging_api.send_msg(reward_topic, msg)

        obs_topic, msg = self.msg_assembler.assemble_observation_msg()
        self.messaging_api.send_msg(obs_topic, msg)
        # TODO wait for new observation & reward

        # TODO self.last_obs = obs
        # TODO return (obs, reward, done, info)

        ...

    def reset(self):
        reset_topic, msg = self.msg_assembler.assemble_reset_msg()
        self.messaging_api.send_msg(reset_topic, msg)

        obs_topic, msg = self.msg_assembler.assemble_observation_msg()
        self.messaging_api.send_msg(obs_topic, msg)

        # TODO wait for acknowledgement and state
        self.time = 0
        # TODO self.last_obs = obs
        # TODO return obs

    def render(self, mode='human', close=False):
        # TODO think about best way to render the observation (e.g. print message?)
        ...

    def close(self):
        self.messaging_api.close()
