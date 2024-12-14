

from subprocess import check_output
from metaflow import FlowSpec, step, Config, current
from metaflow.cards import Markdown

def git_info():
    commit = check_output(['git', 'rev-parse', 'HEAD']).stdout
    branch = check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).stdout
    return {'commit': commit, 'branch': branch}

class GitInfoFlow(FlowSpec):

    git_info = Config('git_info', default_value='', parser='git_info')

    @card(type='blank')
    @step
    def start(self):
        current.card.append(Markdown(f"""
        # Deployment info

        Git commit: {git_info.commit}
        Git branch: {git_info.branch}"""))
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    TomlConfigFlow()