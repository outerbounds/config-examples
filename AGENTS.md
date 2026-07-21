# AGENTS.md

Standalone example flows demonstrating Metaflow's `Config` object (github: outerbounds/config-examples). There is no
package manifest, build, lint, test, or CI setup anywhere in this repo — each top-level directory is an
independent, runnable example, not part of a shared app.

## Layout

Each directory is self-contained; nothing is shared across directories.

- `flow-level/` — `Config` values used inside decorator args via `config_expr(...)` (`@pypi_base`, `@trigger`).
- `git-info/`, `tracked-flow/` — `Config(parser=...)` that shells out to `git` to embed commit info; `tracked-flow`
  shows subclassing a `FlowSpec` that defines a `Config` shared by a subclass (`my_tracked_flow.py` extends
  `tracked_flow.TrackedFlowSpec`).
- `omega/`, `pydantic/`, `toml/` — custom `parser=` functions for OmegaConf, Pydantic-validated JSON, and TOML.
- `timeout/`, `photo/` — `Config` values feeding decorator args (`@timeout(seconds=config.timeout)`) or
  `Parameter` defaults (`Parameter("id", default=cfg.id)`).
- `hydra-instantiation/`, `hydra-sweep/`, `hydra-benchmark/` — Hydra drives config *outside* Metaflow, then hands
  it to the flow via an env var (see below). These are the least obvious pattern in the repo.

## The Hydra pattern (hydra-instantiation, hydra-sweep, hydra-benchmark)

The flow file (`flow.py`, `torchtest.py`, `benchmark_flow.py`) is **not** meant to be run directly with
`python x.py run`. It declares `Config(name="config", default_value="")` with no default file, so run it that way
and every config lookup (`self.config.foo`) will KeyError.

Instead there is a separate hydra entrypoint (`benchmark_runner.py`, `sweep_deployer.py`, or `deploy.py`) that:
1. Builds/merges config via `@hydra.main` (or a hand-built dict for `deploy.py`).
2. Serializes it to JSON and sets it on `env["METAFLOW_FLOW_CONFIG_VALUE"]`.
3. Launches the flow via Metaflow's `Runner` (local run) or `Deployer(...).argo_workflows().create()` (deploy to
   Argo Workflows), passing that `env`.

So: run `python benchmark_runner.py` / `python sweep_deployer.py`, not the flow file, to exercise these examples.
Hydra CLI overrides (e.g. `python benchmark_runner.py backend=pandas`) apply to the *runner* script.

`hydra-benchmark` backend selection: `backend/<name>.yaml` is a Hydra config group (`name` + `pypi packages`) and
`backend/<name>/benchmark.py` must expose `benchmark(paths) -> result`; `benchmark_flow.py` does
`import_module(f"backend.{self.config.backend.name}.benchmark")`. Adding a backend means adding both files.

`hydra-benchmark/config.yaml` sets `remote: kubernetes`, which `benchmark_runner.py` turns into a decospec for
`Runner`, so by default the flow schedules on Kubernetes, not locally — remove/change `remote:` to force local
execution.

`hydra-sweep` uses the `joblib` Hydra sweeper/launcher to fan out multiple `torchtest.py` deploys (one Argo
Workflows branch per param combo), triggers each, polls for completion, then deletes the deployed workflow. This
needs a real Outerbounds/Metaflow deployment target (Argo Workflows, k8s) configured — it cannot be verified with
a purely local run.

## Running / verifying changes

There are no tests. To sanity-check a flow file, use Metaflow's own dry-run tooling rather than inventing test
infra:
- `python <flow>.py show` — validates the flow graph without executing steps.
- `python <flow>.py run` — for flows with a default config file (`Config("config", default="myconfig.json")`,
  etc.), this runs locally end to end.
- For the Hydra examples, run the runner/deployer script (see above), not the flow file.

No formatter/linter config exists, but the git history includes a "blackify files" commit — match Black's
formatting style by convention when editing existing example files, even though nothing enforces it.
