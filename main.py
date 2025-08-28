import logging

from task import task, static_task, leaf_task
from workflow import workflow


@task
def task6(*args, **kwargs):
    print('*****\ntask6\n*****')
    return True


@task
def task5(*args, **kwargs):
    print('*****\ntask5\n*****')
    task6(*args, **kwargs)
    return True


@task
def task3_subtask(*args, **kwargs):
    return True


@task
def task3(*args, **kwargs):
    print('*****\ntask3\n*****')
    task3_subtask(*args, **kwargs)
    return True


@task
def task4(*args, **kwargs):
    print('*****\ntask4\n*****')
    return True


@static_task(deps=[task3, task4])
def task2(*args, **kwargs):
    print('*****\ntask2\n*****')
    task3(*args, **kwargs)
    task4(*args, **kwargs)

    # Uncomment to cause failure.
    #task5(*args, **kwargs)
    return True


# @static_task(deps=[])
@leaf_task
def task1(*args, **kwargs):
    print('*****\ntask1\n*****')
    return True


@workflow
@static_task(deps=[task1, task2, task5])
def do_work(*args, **kwargs):
    task1(*args, **kwargs)
    task2(*args, **kwargs)
    task5(*args, **kwargs)
    return True


if __name__ == '__main__':
    logging.basicConfig(filename='orchestrator.log', level=logging.DEBUG)

    print()
    do_work.dry_run()

    print()
    do_work.run()
