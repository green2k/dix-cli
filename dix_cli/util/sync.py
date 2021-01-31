import os
from typing import Iterator, List, Set, Tuple

from dix_cli.util.databricks import workspace


def list_local_files(base_path: str, include_folders: bool = False) -> Iterator[str]:
    """Yields all paths with respect to the input base_path"""
    for dirpath, dirnames, filenames in os.walk(base_path):
        iter_filenames = (filenames + dirnames) if include_folders else filenames
        for filename in iter_filenames:
            yield os.path.join(dirpath, filename)


def rebase_path(path: str, base_path_old: str, base_path_new: str = '') -> str:
    """
    Returns input path with a modified base_path according to base_path_old & base_path_new input args.
    """
    return os.path.join(
        base_path_new,
        os.path.relpath(path, base_path_old),
    )


def local_file_equals(file_path: str, expected_content: str) -> bool:
    """Returns True, if content of file_path is the same as expected_content"""
    if not os.path.exists(file_path):
        return False

    with open(file_path, 'r') as f:
        return f.read() == expected_content


def compare(
    remote_objects: List[workspace.DatabricksWorkspaceObject],
    remote_base_path: str,
    local_base_path: str,
) -> Tuple[Set[str], Set[workspace.DatabricksWorkspaceObject], Set[workspace.DatabricksWorkspaceObject]]:
    """
    Returns a tuple, which consists of:
        - Set of new local files (relative & scoped)
        - Set of new Databricks objects
        - Set of modified Databricks objects
    """
    set_updated = set()
    set_remote_new = set()

    local_files_scoped = set()
    remote_files_scoped = set()

    # Build set of local files (relative & scoped)
    for local_file in list_local_files(local_base_path, include_folders=True):
        local_files_scoped.add(rebase_path(local_file, local_base_path))

    for remote_object in remote_objects:
        remote_object_path_scoped = rebase_path(remote_object.get_path(include_extension=True), remote_base_path)
        remote_files_scoped.add(remote_object_path_scoped)  # Build set of remote files (relative & scoped)

        # Remote file is new
        if remote_object_path_scoped not in local_files_scoped:
            set_remote_new.add(remote_object)
            continue

        # Need to compare content of remote & local file
        if remote_object.get_object_type() == 'NOTEBOOK' and remote_object_path_scoped in local_files_scoped:
            if not local_file_equals(os.path.join(local_base_path, remote_object_path_scoped), remote_object.download_source()):
                set_updated.add(remote_object)
            continue

    return local_files_scoped - remote_files_scoped, set_updated, set_remote_new
