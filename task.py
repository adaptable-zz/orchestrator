import itertools
import logging
import math
import sys
from functools import partial
from typing import Any, Callable, overload, override

logger = logging.getLogger(__name__)


class BasicTask:
    func: Callable

    def __init__(self, func: Callable) -> None:
        self.func = func

    def name(self) -> str:
        return self.func.__name__

    def __call__(self, *args, **kwargs) -> Any:
        call_stack = kwargs.get('call_stack')
        call_stack.append(self)
        logger.debug(f'Current call stack: {[task.name() for task in call_stack]}')

        graph = kwargs.get('graph')
        if len(call_stack) > 1:
            graph.add_edge(call_stack[-2], call_stack[-1])

        # Fail if undeclared call from a static task.
        if not kwargs.get('dry_run', False) and (len(call_stack)) > 1:
            constraints = kwargs.get('static_constraints')
            if call_stack[-2] in constraints.keys() and self not in constraints[call_stack[-2]]:
                graph.error(self)
                error_msg = f'Static call failure: {call_stack[-2].name()}->{self.name()}'
                logger.info(error_msg)
                sys.exit(error_msg)

        result = self.execute(*args, **kwargs)

        call_stack.pop()

        return result

    def execute(self, *args, **kwargs) -> Any:
        result = None

        dry_run = kwargs.get('dry_run', False)
        if not dry_run:
            logger.debug(f'Calling {self.name()}')
            graph = kwargs.get('graph')
            graph.started(self)
            result = self.func(*args, **kwargs)
            graph.completed(self)
        else:
            logger.debug(f'Dry running {self.name()}')
            graph = kwargs.get('graph')
            graph.speculate(self)

        call_stack = kwargs.get('call_stack')
        logger.debug(f'Current call stack: {[task.name() for task in call_stack]}')

        logger.info(f'{self.name()} completed with result: {result}')

        return result


def task(func: Callable) -> BasicTask:
    return BasicTask(func)


class StaticTask(BasicTask):
    deps: list[BasicTask]
    branches: list[BasicTask]

    def __init__(self, func: Callable, deps: list[BasicTask] = None, branches: list[BasicTask] = None) -> None:
        super().__init__(func)
        self.deps = deps or []
        self.branches = branches or []

    @override
    def execute(self, *args, **kwargs) -> Any:
        result = None

        dry_run = kwargs.get('dry_run', False)
        if not dry_run:
            logger.debug(f'Calling static {self.name()}')
            graph = kwargs.get('graph')

            constraints = kwargs.get('static_constraints')
            for branch in self.branches:
                if branch not in self.deps:
                    self.deps.append(branch)
            if self not in constraints.keys():
                constraints[self] = list(self.deps)

            graph.started(self)
            result = self.func(*args, **kwargs)
            graph.completed(self)
        else:
            logger.debug(f'Dry running {self.name()}')
            for branch in self.branches:
                if branch not in self.deps:
                    self.deps.append(branch)

            for dep in self.deps:
                dep(*args, **kwargs)

            graph = kwargs.get('graph')
            for branch in self.branches:
                graph.branch(branch)

        call_stack = kwargs.get('call_stack')
        logger.debug(f'Current stack: {[task.name() for task in call_stack]}')

        logger.debug(f'{self.name()} completed with result: {result}')

        return result


@overload
def static_task(func: Callable) -> StaticTask:
    ...


@overload
def static_task(*, deps: list[BasicTask], branches: list[BasicTask]) -> Callable[[Callable], StaticTask]:
    ...


def static_task(func: Callable = None, deps: list[BasicTask] = None, branches: list[BasicTask] = None) -> Any:
    if func is None:
        return partial(static_task, deps=deps, branches=branches)

    return StaticTask(func, deps, branches)


def leaf_task(func: Callable) -> StaticTask:
    return StaticTask(func, None, None)


class BatchTask(BasicTask):
    _name: str

    def __init__(self, func: Callable, name: str) -> None:
        super().__init__(func)
        self._name = name

    @override
    def name(self) -> str:
        return self._name


class MapTask(BasicTask):
    mr_input: str
    threads: int

    def __init__(self, func: Callable, mr_input: str, threads: int) -> None:
        super().__init__(func)
        self.mr_input = mr_input
        self.threads = threads

    @override
    def execute(self, *args, **kwargs) -> Any:
        result = []

        if kwargs.get('dry_run', False):
            return None

        data = kwargs.get(self.mr_input)
        batches = itertools.batched(data, math.ceil(len(data) / self.threads))
        tasks = []
        graph = kwargs.get('graph')
        graph.started(self)

        for i, _ in enumerate(batches):
            tasks.append(BatchTask(self.func, f'{self.name()}_{i}'))
            graph.add_edge(self, tasks[i])
            graph.scheduled(tasks[i])

        batches = itertools.batched(data, math.ceil(len(data) / self.threads))
        for i, batch in enumerate(batches):
            kwargs[self.mr_input] = batch
            result.append(tasks[i](*args, **kwargs))

        graph.completed(self)
        return result


@overload
def map_task(func: Callable) -> MapTask:
    ...


@overload
def map_task(*, mr_input: str, threads: int) -> Callable[[Callable], MapTask]:
    ...


def map_task(func: Callable = None, mr_input: str = None, threads: int = 1) -> Any:
    if func is None:
        return partial(map_task, mr_input=mr_input, threads=threads)

    return MapTask(func, mr_input, threads)
