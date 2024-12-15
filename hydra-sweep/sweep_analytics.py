from metaflow import FlowSpec, project, step, Flow, trigger, card, current, Config, config_expr, namespace
from metaflow.cards import Markdown, Table, VegaChart

CHART = {
  "width": 800,
  "height": 400,
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {"url": "data/stocks.csv"},
  "mark": {
    "type": "line",
    "point": True
  },
  "encoding": {
    "x": {"field": "cpu", "type": "quantitative", "scale": {"tickStep": 1}},
    "y": {"field": "perf", "type": "quantitative"},
    "color": {"field": "dim", "type": "nominal"}
  }
}


@project(name='torchperf')
@trigger(event=config_expr('config.event'))
class SweepAnalyticsFlow(FlowSpec):

    config = Config('config', default_value='{"event": "foo"}')

    @card(type='blank')
    @step
    def start(self):
        namespace(self.config.tag)
        runs = list(Flow('TorchPerfFlow').runs(self.config.tag))
        rows = []
        chart = []
        for run in runs:
            d = run.data
            if d is not None:
                rows.append([d.config['cpu'], d.config['tensor_dim'], d.count])
                chart.append({
                    'perf': d.count,
                    'cpu': d.config['cpu'],
                    'dim': d.config['tensor_dim']
                })
        rows.sort()
        current.card.append(Markdown(f"# Found {len(rows)} completed runs for `{self.config.tag}`"))
        current.card.append(Table(rows, headers=['cpu', 'tensor_dim', 'squarings/second']))
        CHART['data'] = {'values': chart}
        current.card.append(VegaChart(CHART))
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    SweepAnalyticsFlow()