import os
from importlib import import_module
from metaflow import FlowSpec, step, Config, S3, pypi, resources, profile


class ConfigurableBenchmark(FlowSpec):
    config = Config("config", default_value="")

    @resources(**config.resources)
    @pypi(packages=config.backend.packages)
    @step
    def start(self):
        self.stats = {}
        mod = import_module(f"backend.{self.config.backend.name}.benchmark")
        with S3() as s3:
            with profile("loading data"):
                objs = s3.get_recursive(self.config.parquet_urls)
            with profile("running benchmark", stats_dict=self.stats):
                self.result = mod.benchmark([obj.path for obj in objs])
            print("result", self.result)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigurableBenchmark()
