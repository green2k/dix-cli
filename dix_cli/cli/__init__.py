import argparse

from dix_cli.cli import commands

LOCAL_SRC_DIR = 'src'
REMOTE_GIT_DIR = 'git'


def run():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        dest='command',
        help='Command to be executed',
        required=True,
    )

    # Register all command-parsers
    for cmd in commands.all_commands:
        cmd_parser = subparsers.add_parser(cmd.get_command(), help=cmd.get_help())
        cmd.add_arguments_to_parser(cmd_parser)

    # Run command
    args = parser.parse_args()
    commands.run(args.command, args)
