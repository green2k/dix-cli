import argparse
import logging
import os
import posixpath

from dix_cli.cli.commands.abstract_command import AbstractCommand
from dix_cli.util import databricks, git, sync
from dix_cli.util.databricks import workspace
from pqdm.threads import pqdm

logger = logging.getLogger(__name__)


class PushCommand(AbstractCommand):
    @staticmethod
    def get_command() -> str:
        return 'push'

    @staticmethod
    def get_help() -> str:
        return 'Synchronizes local workspace to a remote Databricks workspace'

    @staticmethod
    def add_arguments_to_parser(parser: argparse.ArgumentParser) -> None:
        """WARNING: Not a pure function!"""
        parser.add_argument('-f', action='store_true', help='Rewrites all changes in remote workspace', required=True)

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

        # Build list of updated local files (scoped)
        compared_local_updated_scoped = set()
        for o in compared_updated:
            compared_local_updated_scoped.add(sync.rebase_path(o.get_path(include_extension=True), remote_base_path))

        # Upsert objects to Databricks
        for local_path_scoped in sorted(list(compared_local_new.union(compared_local_updated_scoped))):
            local_path = os.path.join(local_base_path, local_path_scoped)
            remote_path = os.path.join(remote_base_path, local_path_scoped)
            logger.info('[SYNC] %s -> databricks:/%s', local_path, remote_path)

            # Create new directory in Databricks workspace
            if os.path.isdir(local_path):
                workspace.make_dir(remote_path)
                continue

            # Sync notebook to Databricks
            with open(local_path, 'r') as f:
                workspace.import_object(f.read(), remote_path, overwrite=True)

        # Delete objects from Databricks
        if len(compared_remote_new) > 0:
            for remote_object in compared_remote_new:
                logger.info('[DELETE] %s', remote_object.get_path())
                workspace.delete_object(remote_object.get_path())  # TODO: Implement remote_object.delete()
        else:
            logger.info('Nothing to delete in Databricks workspace')

        logger.info('Push completed!')
