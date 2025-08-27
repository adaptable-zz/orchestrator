from functools import partial
from typing import Callable, Any

from typing_extensions import override, overload


class BasicTask:
    func: Callable
    stack: list[str]

    def __init__(self, func: Callable, stack: list[str]):
        self.func = func
        self.stack = stack

    def name(self):
        return self.func.__name__

    def __call__(self, *args, **kwargs):
        stack = kwargs.get('stack')
        if stack is None:
            stack = []
            kwargs['stack'] = stack
        stack.append(self.name())
        print(f'Current stack: {stack}')

        task_list = kwargs.get('task_list')
        if task_list is not None and len(stack) > 1:
            task_list.append(f'{stack[-2]} -> {self.name()};')

        return self.execute(*args, **kwargs)

    def execute(self, *args, **kwargs):
        result = None
        dry_run = kwargs.get('dry_run', False)
        if not dry_run:
            print(f'Calling {self.name()}')
            result = self.func(*args, **kwargs)
        else:
            print(f'Dry running {self.name()}')
            task_list = kwargs.get('task_list')
            if task_list is not None:
                task_list.append(f'{self.name()}_children [style="rounded,dotted"];')
                task_list.append(f'{self.name()} -> {self.name()}_children;')

        stack = kwargs.get('stack')
        print(f'Current stack: {stack}')
        stack.pop()
        print(f'{self.name()} completed with result: {result}')

        return result


def task(func: Callable) -> BasicTask:
    return BasicTask(func, [])


class StaticTask(BasicTask):
    deps: list[BasicTask]

    def __init__(self, func: Callable, deps: list[BasicTask] = None):
        super().__init__(func, [])
        self.deps = deps or []

    @override
    def execute(self, *args, **kwargs):
        result = None
        dry_run = kwargs.get('dry_run', False)
        if not dry_run:
            print(f'Calling static {self.name()}')
            result = self.func(*args, **kwargs)
        else:
            print(f'Dry running {self.name()}')
            for dep in self.deps:
                dep(*args, **kwargs)
        stack = kwargs.get('stack')
        print(f'Current stack: {stack}')
        stack.pop()
        print(f'{self.name()} completed with result: {result}')

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
    return StaticTask(func, [])