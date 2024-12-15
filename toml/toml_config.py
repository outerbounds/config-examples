import pprint
from metaflow import FlowSpec, step, Config, resources


class TomlConfigFlow(FlowSpec):
    config = Config("config", default="myconfig.toml", parser="tomllib.loads")

    @resources(cpu=config.resources.cpu)
    @step
    def start(self):
        print("Config loaded:")
        pprint.pp(self.config)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TomlConfigFlow()
