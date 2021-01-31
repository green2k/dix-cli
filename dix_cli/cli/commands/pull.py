import argparse
import logging
import os
import posixpath
import time

from tqdm import tqdm

from dix_cli.cli.commands.abstract_command import AbstractCommand
from dix_cli.util import databricks, git, sync
from dix_cli.util.databricks import workspace
from pqdm.threads import pqdm

logger = logging.getLogger(__name__)


class PullCommand(AbstractCommand):
    @staticmethod
    def get_command() -> str:
        return 'pull'

    @staticmethod
    def get_help() -> str:
        return 'Synchronizes remote Databricks workspace into a local workspace'

    @staticmethod
    def add_arguments_to_parser(parser: argparse.ArgumentParser) -> None:
        """WARNING: Not a pure function!"""
        parser.add_argument('-f', action='store_true', help='Rewrites all changes in local workspace', required=True)

    @staticmethod
    def __download_notebook(notebook: workspace.DatabricksWorkspaceObject, local_path: str) -> None:
        """Downloads notebook to a local filesystem"""
        os.makedirs(os.path.split(local_path)[0], exist_ok=True)
        with open(local_path, 'w') as f:
            f.write(notebook.download_source())

    @staticmethod
    def __delete_local_file(path: str) -> None:
        """Deletes specific path (file/folder) from a local filesystem"""
        if os.path.isdir(path):  # Path is a folder
            os.rmdir(path)
            return

        # File must have supported extension
        if os.path.splitext(path)[1][1:] not in workspace.SUPPORTED_EXTENSIONS:
            raise ValueError('Can\'t delete {} - unsupported file extension'.format(path))

        # Delete file
        os.remove(path)

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        # Local/Remote base path
        local_base_path = databricks.LOCAL_WORKSPACE_BASE_DIR
        remote_base_path = posixpath.join(
            databricks.REMOTE_WORKSPACE_BASE_DIR,
            git.get_project_name(git.get_remote_url()),
            'branch',
            git.get_current_branch_name(),
        )

        # List remote workspace
        logger.info('Indexing remote workspace')
        remote_objects = list(workspace.list_objects(remote_base_path, recursive=True))

        # Fetch remote notebooks
        def download_object(obj: workspace.DatabricksWorkspaceObject) -> None:
            if obj.get_object_type() == 'NOTEBOOK':
                obj.download_source()
        logger.info('Fetching %s notebooks from remote workspace', len(remote_objects))
        pqdm(remote_objects, download_object, n_jobs=100)

        logger.info('Processing local/remote indexes')
        compared_local_new, compared_updated, compared_remote_new = sync.compare(remote_objects, remote_base_path, local_base_path)

        logger.info('Processing fetched notebooks')
        for remote_object in sorted(list(compared_remote_new.union(compared_updated))):
            object_local_path = sync.rebase_path(remote_object.get_path(include_extension=True), remote_base_path, local_base_path)
            logger.info('[SYNC] databricks:/%s -> %s', remote_object.get_path(include_extension=True), object_local_path)

            # Create local directory
            if remote_object.get_object_type() == 'DIRECTORY':
                os.mkdir(object_local_path)
                continue

            # Sync notebook to a local workspace
            PullCommand.__download_notebook(remote_object, object_local_path)

        # Delete local files
        if len(compared_local_new) > 0:
            for local_path_to_delete_scoped in tqdm(sorted(compared_local_new, reverse=True), mininterval=1):
                local_path_to_delete = os.path.join(local_base_path, local_path_to_delete_scoped)
                logger.info('[DELETE] %s', local_path_to_delete)
                PullCommand.__delete_local_file(local_path_to_delete)
                time.sleep(0.2)
        else:
            logger.info('Nothing to delete in local workspace')

        logger.info('Pull completed!')
