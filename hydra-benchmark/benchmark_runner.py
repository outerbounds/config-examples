import json
import os
from metaflow import Runner

from omegaconf import DictConfig, OmegaConf
import hydra
import uuid

TAG = f'benchmark-{uuid.uuid4()}'

@hydra.main(config_name="config", config_path=".", version_base=None)
def benchmark(cfg: DictConfig) -> None:
    print("Tagging all runs as", TAG)
    
    dict_conf = OmegaConf.to_container(cfg, resolve=True)
    json_conf = json.dumps({"config": dict_conf})
    env = os.environ.copy()
    env.update({"METAFLOW_FLOW_CONFIG_VALUE": json_conf})
    if 'remote' in cfg:
        specs = [cfg['remote']]
    else:
        specs = []

    with Runner("benchmark_flow.py", env=env, environment="pypi", decospecs=specs).run(
        tags=[TAG, f'backend:{cfg.backend.name}']
    ) as running:
        secs = running.run.data.stats["running benchmark"]
        print(f"Backend {cfg.backend.name} took {secs}ms")


if __name__ == "__main__":
    benchmark()
