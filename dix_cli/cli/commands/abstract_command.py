import argparse
from abc import ABC


class AbstractCommand(ABC):
    @staticmethod
    def get_command() -> str:
        """TODO: docstring"""

    @staticmethod
    def get_help() -> str:
        """TODO: docstring"""

    @staticmethod
    def add_arguments_to_parser(parser: argparse.ArgumentParser) -> None:
        """WARNING: Not a pure function!"""

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        """TODO: docstring"""
