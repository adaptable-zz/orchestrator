import logging

from task import task, static_task, leaf_task
from workflow import workflow


@task
def print_brup1(*args, **kwargs):
    print('*****\nbrup1\n*****')
    return True


@task
def print_brup2(*args, **kwargs):
    print('*****\nbrup2\n*****')
    print_brup1(*args, **kwargs)
    return True


@task
def print_brup3_unknown_dep(*args, **kwargs):
    return True


@task
def print_brup3(*args, **kwargs):
    print('*****\nbrup3\n*****')
    print_brup3_unknown_dep(*args, **kwargs)
    return True


@task
def print_brup5(*args, **kwargs):
    print('*****\nbrup5\n*****')
    return True


@static_task(deps=[print_brup3, print_brup5])
def print_brup4(*args, **kwargs):
    print('*****\nbrup4\n*****')
    print_brup3(*args, **kwargs)
    print_brup5(*args, **kwargs)
    return True


# @static_task(deps=[])
@leaf_task
def print_brup6(*args, **kwargs):
    print('*****\nbrup6\n*****')
    return True


@workflow
@task
def do_stuff(*args, **kwargs):
    print_brup6(*args, **kwargs)
    print_brup4(*args, **kwargs)
    print_brup2(*args, **kwargs)
    return True


@workflow
@static_task(deps=[print_brup6, print_brup4, print_brup2])
def do_stuff2(*args, **kwargs):
    print_brup6(*args, **kwargs)
    print_brup4(*args, **kwargs)
    print_brup2(*args, **kwargs)
    return True


if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(filename='orchestrator.log', level=logging.DEBUG)

    print()
    do_stuff2.dry_run()

    print()
    do_stuff2.run()
