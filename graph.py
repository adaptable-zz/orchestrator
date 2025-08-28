from enum import Enum
from typing import override

from task import BasicTask


class SpeculativeTask(BasicTask):
    _name: str

    def __init__(self, name: str) -> None:
        super().__init__(None)
        self._name = name

    @override
    def name(self) -> str:
        return self._name


class Status(Enum):
    STATIC_KNOWN = 1
    BRANCH_POSSIBLE = 2
    BRANCH_NOT_TAKEN = 3
    IN_PROGRESS = 4
    COMPLETED = 5
    ERROR = 6
    SPECULATED = 7


def style(status: Status) -> str:
    match status:
        case Status.STATIC_KNOWN:
            return ''
        case Status.BRANCH_POSSIBLE:
            return ''
        case Status.BRANCH_NOT_TAKEN:
            return ' [color="gray"]'
        case Status.IN_PROGRESS:
            return ' [style="rounded,filled" fillcolor="yellow"]'
        case Status.COMPLETED:
            return ' [style="rounded,filled" fillcolor="green"]'
        case Status.ERROR:
            return ' [style="rounded,filled" fillcolor="red"]'
        case Status.SPECULATED:
            return ' [style="rounded,dotted"]'


def edge_style(status: Status) -> str:
    match status:
        case Status.BRANCH_POSSIBLE | Status.BRANCH_NOT_TAKEN | Status.SPECULATED:
            return ' [style="dotted"]'

    return ''


class Graph:
    edges: dict[BasicTask, list[BasicTask]]
    status: dict[BasicTask, Status]

    def __init__(self, root: BasicTask) -> None:
        self.edges = {}
        self.status = {root: Status.STATIC_KNOWN}

    def add_edge(self, parent: BasicTask, child: BasicTask) -> None:
        if parent not in self.edges.keys():
            self.edges[parent] = []

        if child not in self.edges.keys():
            self.edges[child] = []

        if child not in self.edges[parent]:
            self.edges[parent].append(child)

        self.status[child] = Status.STATIC_KNOWN

    def completed(self, node: BasicTask) -> None:
        self.status[node] = Status.COMPLETED

        orphans = [child
                   for child in self.edges[node]
                   if self.status[child] == Status.SPECULATED]
        for orphan in orphans:
            del self.status[orphan]
            self.edges[node].remove(orphan)

    def started(self, node: BasicTask) -> None:
        self.status[node] = Status.IN_PROGRESS

    def speculate(self, node: BasicTask) -> None:
        spec = SpeculativeTask(f'{node.name()}_children')
        self.add_edge(node, spec)
        self.status[spec] = Status.SPECULATED

    def to_graphviz(self) -> str:
        result: list[str] = []

        result.append('digraph {')
        result.append('\trankdir=LR;')
        result.append('\tnode [shape="rectangle" style="rounded"];')

        for node, status in self.status.items():
            result.append(f'\t{node.name()}{style(status)};')

        for parent, children in self.edges.items():
            for child in children:
                result.append(f'\t{parent.name()} -> {child.name()}{edge_style(self.status[child])};')

        result.append('}')

        return '\n'.join(result)
