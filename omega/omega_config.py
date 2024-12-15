
import pprint

from metaflow import FlowSpec, step, Config, Parameter
from omegaconf import OmegaConf

def omega_parse(txt):
    config = OmegaConf.create(txt)
    return OmegaConf.to_container(config, resolve=True)

def parse_overrides(config, overrides):
    cfg = config.to_dict()
    if overrides:
        base = OmegaConf.create(cfg)
        config = OmegaConf.from_dotlist(overrides.split(','))
        return OmegaConf.to_container(OmegaConf.merge(base, config))
    else:
        return cfg

class OmegaConfigFlow(FlowSpec):

    base = Config('config', default='omega.yaml', parser=omega_parse)
    overrides = Parameter('overrides', default='')

    @step
    def start(self):
        self.config = parse_overrides(self.base, self.overrides)
        pprint.pp(self.config)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    OmegaConfigFlow()