#!/usr/bin/env python3

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, REMAINDER
from glob import glob
from os import makedirs
from pathlib import Path
import re
from subprocess import Popen, PIPE
import sys
from typing import List, Any, Optional, TextIO, Tuple
from shutil import which

CLANG_FORMAT_REQUIRED_VERSION = (12, 0, 0)
CLANG_TIDY_REQUIRED_VERSION = (12, 0, 0)

SEMVER_REGEX = re.compile(
    r"""
        ^
        (?P<major>0|[1-9]\d*)
        \.
        (?P<minor>0|[1-9]\d*)
        \.
        (?P<patch>0|[1-9]\d*)
        (?:-(?P<prerelease>
            (?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
            (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*
        ))?
        (?:\+(?P<build>
            [0-9a-zA-Z-]+
            (?:\.[0-9a-zA-Z-]+)*
        ))?
        $
    """,
    re.VERBOSE,
)


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


def find_command(command: str, msg: Optional[str] = None) -> str:
    return run_pipe("which", command, msg=msg).read().strip()


def get_source_files(dir: Path) -> List[str]:
    return [
        *glob(str(dir / "src/**/*.h"), recursive=True),
        *glob(str(dir / "include/**/*.h"), recursive=True),
        *glob(str(dir / "src/**/*.cpp"), recursive=True),
        *glob(str(dir / "tests/**/*.cpp"), recursive=True),
    ]


def run_pipe(*args: str, msg: Optional[str] = None, verbose: bool = False, **kwargs: Any) -> TextIO:
    p = run(*args, msg=msg, verbose=verbose, stdout=PIPE, universal_newlines=True, **kwargs)
    return p.stdout


def check_version(current: str, required: Tuple[int, int, int], prog_name: Optional[str] = None) -> Tuple[
    int, int, int]:
    require_version = '.'.join(map(str, required))
    semver_match = SEMVER_REGEX.match(current)
    if semver_match is None:
        raise RuntimeError(f"{prog_name} {require_version} or higher is required, got: {current}")
    semver_dict = semver_match.groupdict()
    semver = (int(semver_dict["major"]), int(semver_dict["minor"]), int(semver_dict["patch"]))
    if semver < required:
        raise RuntimeError(f"{prog_name} {require_version} or higher is required, got: {current}")

    return semver


def clang_format(clang_format_path: str, fix: bool = False) -> None:
    command = find_command(clang_format_path, msg="clang-format is required")

    version_res = run_pipe(command, '--version').read().strip()
    version_str = re.search(r'version\s+((?:\w|\.)+)', version_res).group(1)

    check_version(version_str, CLANG_FORMAT_REQUIRED_VERSION, "clang-format")

    basedir = Path(__file__).parent.absolute()
    sources = get_source_files(basedir)

    if fix:
        options = ['-i']
    else:
        options = ['--dry-run', '--Werror']

    run(command, *options, *sources, verbose=True, cwd=basedir)


def clang_tidy(dir: str, jobs: Optional[int], clang_tidy_path: str, run_clang_tidy_path: str, fix: bool) -> None:
    # use the run-clang-tidy Python script provided by LLVM Clang
    run_command = find_command(run_clang_tidy_path, msg="run-clang-tidy is required")
    tidy_command = find_command(clang_tidy_path, msg="clang-tidy is required")

    version_res = run_pipe(tidy_command, '--version').read().strip()
    version_str = re.search(r'version\s+((?:\w|\.)+)', version_res).group(1)

    check_version(version_str, CLANG_TIDY_REQUIRED_VERSION, "clang-tidy")

    if not (Path(dir) / 'compile_commands.json').exists():
        raise RuntimeError(f"expect compile_commands.json in build directory {dir}")

    basedir = Path(__file__).parent.absolute()

    options = ['-p', dir, '-clang-tidy-binary', tidy_command]
    if jobs is not None:
        options.append(f'-j{jobs}')

    options.extend(['-fix'] if fix else [])

    regexes = ['kvrocks/src/', 'utils/kvrocks2redis/', 'tests/cppunit/']

    options.append(f'-header-filter={"|".join(regexes)}')

    run(run_command, *options, *regexes, verbose=True, cwd=basedir)


if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.set_defaults(func=parser.print_help)

    subparsers = parser.add_subparsers()

    parser_format = subparsers.add_parser(
        'format',
        description="Format source code",
        help="Format source code")
    parser_format.set_defaults(func=lambda **args: clang_format(**args, fix=True))
    parser_format.add_argument('--clang-format-path', default='clang-format',
                               help="path of clang-format used to check source")

    parser_check = subparsers.add_parser(
        'check',
        description="Check or lint source code",
        help="Check or lint source code")
    parser_check.set_defaults(func=parser_check.print_help)
    parser_check_subparsers = parser_check.add_subparsers()
    parser_check_format = parser_check_subparsers.add_parser(
        'format',
        description="Check source format by clang-format",
        help="Check source format by clang-format")
    parser_check_format.set_defaults(func=lambda **args: clang_format(**args, fix=False))
    parser_check_format.add_argument('--clang-format-path', default='clang-format',
                                     help="path of clang-format used to check source")
    parser_check_tidy = parser_check_subparsers.add_parser(
        'tidy',
        description="Check code with clang-tidy",
        help="Check code with clang-tidy",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser_check_tidy.set_defaults(func=clang_tidy)
    parser_check_tidy.add_argument('dir', metavar='BUILD_DIR', nargs='?', default='cmake-build-debug',
                                   help="directory to store cmake-generated and build files")
    parser_check_tidy.add_argument('-j', '--jobs', metavar='N', help='execute N build jobs concurrently')
    parser_check_tidy.add_argument('--clang-tidy-path', default='clang-tidy',
                                   help="path of clang-tidy used to check source")
    parser_check_tidy.add_argument('--run-clang-tidy-path', default='run-clang-tidy',
                                   help="path of run-clang-tidy used to check source")
    parser_check_tidy.add_argument('--fix', default=False, action='store_true',
                                   help='automatically fix codebase via clang-tidy suggested changes')

    args = parser.parse_args()

    arg_dict = dict(vars(args))
    del arg_dict['func']
    args.func(**arg_dict)
