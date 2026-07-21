import json
import os
import uuid

import hydra
from metaflow import Runner
from omegaconf import DictConfig, OmegaConf

TAG = f"benchmark-{uuid.uuid4()}"
os.environ["HYDRA_FULL_ERROR"] = "1"


@hydra.main(config_name="config", config_path=".", version_base=None)
def benchmark(cfg: DictConfig) -> None:
    print("Tagging all runs as", TAG)

    dict_conf = OmegaConf.to_container(cfg, resolve=True)
    json_conf = json.dumps({"config": dict_conf})
    # Runner's `env=` kwarg only applies to the spawned subprocess. Metaflow's
    # click API pre-computes the flow's config CLI args in *this* process by
    # reading os.environ directly, so it must be set here too.
    os.environ["METAFLOW_FLOW_CONFIG_VALUE"] = json_conf
    env = os.environ.copy()
    if "remote" in cfg:
        specs = [cfg["remote"]]
    else:
        specs = []

    with Runner(
        "benchmark_flow.py", env=env, environment="fast-bakery", decospecs=specs
    ).run(tags=[TAG, f"backend:{cfg.backend.name}"]) as running:
        secs = running.run.data.stats["running benchmark"]
        print(f"Backend {cfg.backend.name} took {secs}ms")


if __name__ == "__main__":
    benchmark()
