

from subprocess import check_output
from metaflow import FlowSpec, step, Config, current, card
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

class GitInfoFlow(FlowSpec):

    git_info = Config('git_info', default_value='', parser=git_info)

    def output_git_info(self):
        table = [[Markdown(f'**{k.capitalize()}**'), v]
                 for k, v in self.git_info.items()]        
        current.card['git'].append(Markdown(f"# Deployment info"))
        current.card['git'].append(Table(table))

    @card(type='blank', id='git')
    @step
    def start(self):
        self.output_git_info()
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    GitInfoFlow()