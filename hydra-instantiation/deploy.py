import os
import json
import uuid
import hydra
from omegaconf import DictConfig, OmegaConf
from metaflow import Deployer


def main(my_dict):
    cfg = OmegaConf.create(my_dict)
    hydra_config = OmegaConf.to_container(cfg, resolve=True)
    json_config = json.dumps({"config": hydra_config})
    # Deployer's `env=` kwarg only applies to the spawned subprocess. Metaflow's
    # click API pre-computes the flow's config CLI args in *this* process by
    # reading os.environ directly, so it must be set here too.
    os.environ["METAFLOW_FLOW_CONFIG_VALUE"] = json_config
    env = os.environ.copy()
    deployer = Deployer("flow.py", env=env, decospecs=["retry"], environment="conda")
    deployed_flow = deployer.argo_workflows().create()
    execution1 = deployed_flow.trigger()
    execution2 = deployed_flow.trigger()


if __name__ == "__main__":
    my_dict = {
        "optimizer": {
            "_target_": "optimizer_module.Optimizer",
            "algo": "SGD",
            "lr": 0.01,
        },
        "lookback_days": 30,
    }
    main(my_dict)
