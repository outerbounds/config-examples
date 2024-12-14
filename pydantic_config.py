

import json, pprint
from metaflow import FlowSpec, step, Config, resources

def pydantic_parser(txt):
    from pydantic import BaseModel, PositiveInt
    from datetime import datetime

    class ConfigSchema(BaseModel):
        id: int  
        signup_ts: datetime | None  
        tastes: dict[str, PositiveInt]

    cfg = json.loads(txt)
    ConfigSchema.model_validate(cfg)
    return cfg

class PydanticConfigFlow(FlowSpec):

    config = Config('config', parser=pydantic_parser)

    @step
    def start(self):
        print("Config loaded and validated:")
        pprint.pp(self.config)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    PydanticConfigFlow()