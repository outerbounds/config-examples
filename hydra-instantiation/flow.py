from metaflow import FlowSpec, step, Config, conda_base


@conda_base(packages={"hydra-core": "1.3.2"})
class MyFlow(FlowSpec):

    config = Config(name="config", default_value="")

    @step
    def start(self):
        from omegaconf import OmegaConf
        from hydra.utils import instantiate
        from datetime import datetime, timedelta
        
        cfg = OmegaConf.create(self.config._data) 
        cfg.start_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cfg.close_date = (datetime.now() - timedelta(days=cfg.lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
        instance = instantiate(cfg)
        print(f"Instantiated: {instance}")
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    MyFlow()