from subprocess import check_output

from metaflow import FlowSpec, Config, current
from metaflow.cards import Markdown, Table

def git_info(args):
    info = {
        'commit': ['git', 'rev-parse', 'HEAD'],
        'branch': ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        'message': ['git', 'log',  '-1', '--pretty=%B']
    }
    cfg = {}
    for key, cmd in info.items():
        cfg[key] = check_output(cmd, text=True).strip()
    return cfg

class TrackedFlowSpec(FlowSpec):
    git_info = Config('git_info', default_value='', parser=git_info)

    def output_git_info(self):
        print('deployment info', self.git_info)
        if hasattr(current, 'card'):
            table = [[Markdown(f'**{k.capitalize()}**'), v]
                     for k, v in self.git_info.items()]        
            current.card.append(Markdown(f"# Deployment info"))
            current.card.append(Table(table))