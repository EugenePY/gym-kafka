import gym
from gym.utils import seeding

from gym_kafka.util import GymFactory


class KafkaEnv(gym.Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        # TODO read Kafka config from file
        # TODO initialize action and observation space
        self.messaging_api = GymFactory.make_messaging_api('gym_kafka/config.json')
        self.action_space = None
        self.observation_space = None

        self.time = 0
        self.seed()
        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        assert self.action_space.contains(action)
        self.time += 1
        # TODO send action
        # TODO wait for new observation & reward
        # TODO return (obs, reward, done, info)

        ...

    def reset(self):
        # TODO send reset command and wait for acknowledgement and state
        self.time = 0
        # TODO return obs

    def render(self, mode='human', close=False):
        # TODO think about best way to render the observation (e.g. print message?)
        ...
