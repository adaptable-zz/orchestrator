import logging
from random import random

from task import task, static_task, leaf_task, map_task
from workflow import workflow


@map_task(mr_input='nums', threads=4)
def total(nums, *args, **kwargs) -> int:
    print(f'Batch input: {nums} -- Result: {sum(nums)}')
    return sum(nums)


# @static_task(deps=[total])
# def mr_task(*args, **kwargs) -> bool:
#    t = total(nums=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], *args, **kwargs)
#    print(t)
#    if isinstance(t, list):
#        print(sum(t))
#    return True

@task
def task6(*args, **kwargs) -> bool:
    print('*****\ntask6\n*****')
    t = total(nums=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], *args, **kwargs)
    print(t)
    if isinstance(t, list):
        print(sum(t))
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
    # task5(*args, **kwargs)
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
def hybrid_workflow(*args, **kwargs) -> None:
    task1(*args, **kwargs)
    task2(*args, **kwargs)
    task5(*args, **kwargs)


@workflow
@task
def dynamic_workflow(*args, **kwargs) -> None:
    task1(*args, **kwargs)
    task2(*args, **kwargs)
    task5(*args, **kwargs)


if __name__ == '__main__':
    logging.basicConfig(filename='orchestrator.log', level=logging.DEBUG)

    hybrid_workflow.dry_run()
    hybrid_workflow.run()

    # dynamic_workflow.run()
