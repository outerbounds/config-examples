import os
import tempfile
from importlib import import_module
from urllib.parse import urlparse
from urllib.request import urlretrieve
from metaflow import FlowSpec, step, Config, pypi, resources, profile


class ConfigurableBenchmark(FlowSpec):
    config = Config("config", default_value="")

    @resources(**config.resources)
    @pypi(packages=config.backend.packages)
    @step
    def start(self):
        self.stats = {}
        mod = import_module(f"backend.{self.config.backend.name}.benchmark")
        with tempfile.TemporaryDirectory() as tmp_dir:
            with profile("loading data"):
                paths = []
                for url in self.config.parquet_urls:
                    dst = os.path.join(tmp_dir, os.path.basename(urlparse(url).path))
                    urlretrieve(url, dst)
                    paths.append(dst)
            with profile("running benchmark", stats_dict=self.stats):
                self.result = mod.benchmark(paths)
            print("result", self.result)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConfigurableBenchmark()
