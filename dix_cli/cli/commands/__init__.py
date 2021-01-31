import argparse
import logging
import os
from typing import List, Type

from dix_cli.cli.commands.abstract_command import AbstractCommand
from dix_cli.cli.commands.pull import PullCommand
from dix_cli.cli.commands.push import PushCommand

# Register all commands
all_commands: List[Type[AbstractCommand]] = [
    PullCommand,
    PushCommand,
]

# Build a map of commands
commands_map = {}
for command in all_commands:
    commands_map[command.get_command()] = command


def run(command: str, args: argparse.Namespace) -> None:
    """Runs specific command with input arguments"""
    logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO'), format='%(asctime)s %(name)s %(levelname)s: %(message)s')

    commands_map[command].run(args)
