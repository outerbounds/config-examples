from metaflow import (
    FlowSpec,
    step,
    Config,
    trigger,
    pypi_base,
    config_expr,
    card,
    current,
)
from metaflow.cards import Table


@pypi_base(packages=config_expr("config.packages"))
@trigger(event=config_expr("config.event"))
class TopLevelConfigFlow(FlowSpec):
    config = Config("config", default="myconfig.json")

    @step
    def start(self):
        import pandas as pd

        self.df = pd.DataFrame({"col": ["first", "second", "third"]})
        self.next(self.end)

    @card
    @step
    def end(self):
        print("outputing dataframe", self.df)
        current.card.append(Table.from_dataframe(self.df))


if __name__ == "__main__":
    TopLevelConfigFlow()
