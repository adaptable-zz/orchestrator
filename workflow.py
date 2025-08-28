from typing import Any

from graph import Graph
from task import BasicTask


class BasicWorkflow:
    entry: BasicTask
    graph: Graph

    def __init__(self, entry: BasicTask) -> None:
        self.entry = entry
        self.graph = Graph(self.entry)

    def run(self, *args, **kwargs) -> Any:
        kwargs['graph'] = self.graph
        kwargs['call_stack'] = []
        kwargs['static_constraints'] = {}
        return self.entry(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> Any:
        return self.entry(*args, **kwargs)

    def dry_run(self, *args, **kwargs) -> None:
        kwargs['dry_run'] = True
        self.run(self.entry, *args, **kwargs)
        self.graph.save()

    def to_graphviz(self) -> None:
        print(self.graph.to_graphviz())


def workflow(func: BasicTask) -> BasicWorkflow:
    return BasicWorkflow(func)
