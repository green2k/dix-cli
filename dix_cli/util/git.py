import configparser
import logging
import os
import posixpath
import re

PATTERN_GIT_URL = re.compile(r'^(https|git)(:\/\/|@)([^\/:]+)[\/:]([^\/:]+)\/(.+).git$')

logger = logging.getLogger(__name__)


def get_remote_url() -> str:
    """
    Returns remote git repository url (with respect to current directory)
    """
    config = configparser.ConfigParser()
    config.read('.git/config')
    for section in config.sections():
        url = config[section].get('url')
        if url:
            return url

    raise Exception('No remote git url found!')


def get_project_name(url: str) -> str:
    """
    Returns project name for given git url
    """
    extract = PATTERN_GIT_URL.search(url)
    if extract:
        return posixpath.join(
            extract.group(4),
            extract.group(5),
        )

    raise Exception(f'Git project name not found for given url ({url})')


def get_current_branch_name() -> str:
    """
    Returns currently checked-out GIT branch
    """
    with open(os.path.join('.git', 'HEAD'), 'r') as fp:
        f_content = fp.readlines()[0]
        branch_name = f_content[f_content.index('/heads/') + 7:].strip(' \n\t')
    return branch_name
