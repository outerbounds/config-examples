from metaflow import step, Config
from tracked_flow import TrackedFlowSpec


class MyTrackedFlow(TrackedFlowSpec):
    config = Config("config", default="myconfig.json")

    @step
    def start(self):
        self.output_git_info()
        self.next(self.end)

    @step
    def end(self):
        print("config", self.config)


if __name__ == "__main__":
    MyTrackedFlow()
