import os
import time
import json
from metaflow import Deployer

from omegaconf import DictConfig, OmegaConf
import hydra

TAG = f"sweep_{int(time.time())}"
EVENT = f"event_{TAG}"


def trigger_and_wait(deployed, timeout=300):
    triggered_run = deployed.trigger()
    started_at = time.time()
    run_id = None
    while triggered_run.status not in ("Succeeded", "Failed", "Terminated"):
        print(f"Run[{run_id}] is {triggered_run.status}")
        time.sleep(5)
        if run_id is None and triggered_run.run:
            run_id = triggered_run.run.id
            print("Run ID", run_id)
        if time.time() - started_at > timeout:
            print(f"{run_id} timed out - terminating")
            triggered_run.terminate()
            break
    print(f"{run_id} finished")


def deploy_analytics():
    conf = json.dumps({"config": {"event": EVENT, "tag": TAG}})
    env = os.environ.copy()
    env.update({"METAFLOW_FLOW_CONFIG_VALUE": conf})
    deployer = Deployer("sweep_analytics.py", branch=TAG, env=env)
    return deployer.argo_workflows().create(tags=[TAG])


@hydra.main(config_name="config", config_path=".", version_base=None)
def sweep_deployer(cfg: DictConfig) -> None:
    branch = f"{TAG}x{cfg.variant_id}"
    cfg.event = EVENT

    print(f"Deploying a branch {branch} with tag {TAG}")

    dict_conf = OmegaConf.to_container(cfg, resolve=True)
    json_conf = json.dumps({"config": dict_conf})
    env = os.environ.copy()
    env.update({"METAFLOW_FLOW_CONFIG_VALUE": json_conf})

    deployer = Deployer("torchtest.py", branch=branch, env=env, environment="pypi")
    deployed = deployer.argo_workflows().create(tags=[TAG])
    trigger_and_wait(deployed, timeout=cfg.timeout)
    deployed.delete()


if __name__ == "__main__":
    analytics = deploy_analytics()
    try:
        sweep_deployer()
        trigger_and_wait(analytics)
    finally:
        analytics.delete()
