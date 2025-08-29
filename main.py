import logging
from random import random

from task import task, static_task, leaf_task
from workflow import workflow


@task
def task6(*args, **kwargs) -> bool:
    print('*****\ntask6\n*****')
    return True


@task
def task5(*args, **kwargs) -> bool:
    print('*****\ntask5\n*****')
    task6(*args, **kwargs)
    return True


@task
def task3_subtask(*args, **kwargs) -> bool:
    return True


@task
def task3(*args, **kwargs) -> bool:
    print('*****\ntask3\n*****')
    task3_subtask(*args, **kwargs)
    return True


@task
def task4(*args, **kwargs) -> bool:
    print('*****\ntask4\n*****')
    return True


@static_task(deps=[task3, task4])
def task2(*args, **kwargs) -> bool:
    print('*****\ntask2\n*****')
    task3(*args, **kwargs)
    task4(*args, **kwargs)

    # task5 is not declared as a static dependency.
    # Uncomment to cause failure.
    #task5(*args, **kwargs)
    return True

@leaf_task
def task1_a(*args, **kwargs) -> bool:
    return True

@leaf_task
def task1_b(*args, **kwargs) -> bool:
    return True

@static_task(branches=[task1_a, task1_b])
def task1(*args, **kwargs) -> bool:
    print('*****\ntask1\n*****')

    if random() < 0.5:
        task1_a(*args, **kwargs)
    else:
        task1_b(*args, **kwargs)

    return True


@workflow
@static_task(deps=[task1, task2, task5])
def do_work(*args, **kwargs) -> None:
    task1(*args, **kwargs)
    task2(*args, **kwargs)
    task5(*args, **kwargs)


if __name__ == '__main__':
    logging.basicConfig(filename='orchestrator.log', level=logging.DEBUG)

    print()
    do_work.dry_run()

    print()
    do_work.run()
