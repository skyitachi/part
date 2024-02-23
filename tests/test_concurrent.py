#!/usr/bin/env python3

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, REMAINDER
from typing import List, Any, Optional, TextIO, Tuple
from subprocess import Popen, PIPE
import os
import sys


def run(*args: str, msg: Optional[str] = None, verbose: bool = False, **kwargs: Any) -> Popen:
    sys.stdout.flush()
    if verbose:
        print(f"$ {' '.join(args)}")

    p = Popen(args, **kwargs)
    code = p.wait()
    if code != 0:
        err = f"\nfailed to run: {args}\nexit with code: {code}\n"
        if msg:
            err += f"error message: {msg}\n"
        raise RuntimeError(err)

    return p


def run_pipe(*args: str, msg: Optional[str] = None, verbose: bool = False, **kwargs: Any) -> TextIO:
    p = run(*args, msg=msg, verbose=verbose, stdout=PIPE, universal_newlines=True, **kwargs)
    return p.stdout


def run_multi_thread(test_path: str, runs: int):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    abs_binary_path = os.path.join(current_directory, "..", "cmake-build-debug", "tests", test_path)
    print("test path: ", abs_binary_path)
    succ = 0
    failed = 0
    for i in range(0, runs):
        try:
            run(abs_binary_path, "--gtest_filter=ConcurrentARTTest.BigMultiThreadTest")
            succ += 1
        except RuntimeError as e:
            failed += 1
    print(f"success: {succ}, failed: {failed}")


if __name__ == "__main__":
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)

    subparsers = parser.add_subparsers()

    multi_thread = subparsers.add_parser(
        "conc",
        description="test in multi thread env",
        help="test in multi thread env"
    )
    multi_thread.set_defaults(func=lambda **args: run_multi_thread(**args, runs=100))
    multi_thread.add_argument('--test-path', default='test_concurrent_art',
                              help="path of test binary")

    args = parser.parse_args()

    arg_dict = dict(vars(args))
    del arg_dict['func']
    args.func(**arg_dict)
