from task import BasicTask


class BasicWorkflow:
    tasks: list[str]
    entry: BasicTask

    def __init__(self, entry: BasicTask):
        self.tasks = []
        self.entry = entry

    def run(self, *args, **kwargs):
        kwargs['task_list'] = self.tasks
        return self.entry(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.entry(*args, **kwargs)

    def dry_run(self, *args, **kwargs):
        kwargs['dry_run'] = True
        self.run(self.entry, *args, **kwargs)

    def to_viz(self):
        print('digraph {')
        print('\trankdir=LR;')
        print('\tnode [shape=rectangle style=rounded];')
        for edge in self.tasks:
            print(f'\t{edge}')
        print('}')


def workflow(func: BasicTask) -> BasicWorkflow:
    return BasicWorkflow(func)
