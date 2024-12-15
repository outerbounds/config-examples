from metaflow import (
    FlowSpec,
    step,
    current,
    project,
    Flow,
    resources,
    card,
    pypi,
    Config,
)
from metaflow.integrations import ArgoEvent
from metaflow.cards import Markdown
import time


@project(name="torchperf")
class TorchPerfFlow(FlowSpec):
    config = Config("config", default_value="")

    @card(type="blank", refresh_interval=1, id="status")
    @pypi(python="3.11.0", packages={"torch": "2.5.1"})
    @resources(cpu=config.cpu, memory=16000)
    @step
    def start(self):
        t = self.create_tensor(self.config.tensor_dim)
        self.run_squarings(t)
        self.next(self.end)

    def create_tensor(self, dim):
        import torch  # pylint: disable=import-error

        print("Creating a tensor")
        self.tensor = t = torch.rand((dim, dim))
        print("Tensor created! Shape", self.tensor.shape)
        print("Tensor is stored on", self.tensor.device)
        if torch.cuda.is_available():
            print("CUDA available! Moving tensor to GPU memory")
            t = self.tensor.to("cuda")
            print("Tensor is now stored on", t.device)
        else:
            print("CUDA not available")
        return t

    def run_squarings(self, tensor, seconds=30):
        import torch  # pylint: disable=import-error

        print(f"Using {self.config.cpu} threads for computation")
        torch.set_num_threads(self.config.cpu)
        torch.set_num_interop_threads(self.config.cpu)

        print("Starting benchmark")
        counter = Markdown("# Starting to square...")
        current.card["status"].append(counter)
        current.card["status"].refresh()

        self.count = 0
        s = time.time()
        while time.time() - s < seconds:
            for i in range(10):
                torch.matmul(tensor, tensor)
            self.count += 10
            counter.update(f"# {self.count} squarings completed ")
            current.card["status"].refresh()
        elapsed = time.time() - s

        msg = f"⚡ {self.count/elapsed} squarings per second ⚡"
        current.card["status"].append(Markdown(f"# {msg}"))
        print(msg)

    @step
    def end(self):
        ArgoEvent(self.config.event).safe_publish()


if __name__ == "__main__":
    TorchPerfFlow()
