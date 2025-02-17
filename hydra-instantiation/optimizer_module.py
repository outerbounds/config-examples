class Optimizer:
    def __init__(self, algo: str, lr: float) -> None:
        self.algo = algo
        self.lr = lr

    def __repr__(self):
        return f"Optimizer(algo={self.algo}, lr={self.lr})"