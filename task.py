import logging
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
            # if self.name() == 'print_brup5':
            if self.name() == 'print_brup3_unknown_dep':
                print(graph.to_graphviz())
            result = self.func(*args, **kwargs)
            graph.completed(self)
        else:
            logger.debug(f'Dry running {self.name()}')
            graph = kwargs.get('graph')
            graph.speculate(self)
            task_list = kwargs.get('task_list')
            if task_list is not None:
                task_list.append(f'{self.name()}_children [style="rounded,dotted"];')
                task_list.append(f'{self.name()} -> {self.name()}_children;')

        call_stack = kwargs.get('call_stack')
        logger.debug(f'Current call stack: {[task.name() for task in call_stack]}')

        logger.debug(f'{self.name()} completed with result: {result}')

        return result


def task(func: Callable) -> BasicTask:
    return BasicTask(func)


class StaticTask(BasicTask):
    deps: list[BasicTask]

    def __init__(self, func: Callable, deps: list[BasicTask] = None) -> None:
        super().__init__(func)
        self.deps = deps or []

    @override
    def execute(self, *args, **kwargs) -> Any:
        result = None

        dry_run = kwargs.get('dry_run', False)
        if not dry_run:
            logger.debug(f'Calling static {self.name()}')
            graph = kwargs.get('graph')
            graph.started(self)
            if self.name() == 'print_brup4':
                graph = kwargs.get('graph')
                print(graph.to_graphviz())
            result = self.func(*args, **kwargs)
            graph.completed(self)
        else:
            logger.debug(f'Dry running {self.name()}')
            if self.name() == 'print_brup4':
                graph = kwargs.get('graph')
                print(graph.to_graphviz())
            for dep in self.deps:
                dep(*args, **kwargs)

        call_stack = kwargs.get('call_stack')
        logger.debug(f'Current stack: {[task.name() for task in call_stack]}')

        logger.debug(f'{self.name()} completed with result: {result}')

        return result


@overload
def static_task(func: Callable) -> StaticTask:
    ...


@overload
def static_task(*, deps: list[BasicTask]) -> Callable[[Callable], StaticTask]:
    ...


def static_task(func: Callable = None, deps: list[BasicTask] = None) -> Any:
    if func is None:
        return partial(static_task, deps=deps)

    return StaticTask(func, deps)


def leaf_task(func: Callable) -> StaticTask:
    return StaticTask(func, None)
