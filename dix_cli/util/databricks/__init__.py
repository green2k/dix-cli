import logging
from typing import Optional
from urllib.parse import urljoin

import requests

from dix_cli.util.databricks.auth import get_auth_config

logger = logging.getLogger(__name__)
LOCAL_WORKSPACE_BASE_DIR = 'src'  # Relative path
REMOTE_WORKSPACE_BASE_DIR = '/git'  # Absolute path


def call(path: str, params: Optional[dict] = None, method: str = 'GET', data: Optional[str] = None) -> dict:
    """
    Returns a Response based on a Databricks API request
    Do all the authentication necessary.
    """
    config = get_auth_config()
    host = config.get_host()
    token = config.get_token()

    url = urljoin(host, path)
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    params = params or {}

    # Log API request
    headers_masked = {**headers, 'Authorization': headers['Authorization'][:15] + '...'}
    logger.debug('Calling API %s (headers=%s, params=%s)', url, headers_masked, params)

    # Call Databricks API
    if method == 'GET':
        response = requests.get(url, headers=headers, params=params)
    elif method == 'POST':
        response = requests.post(url, headers=headers, params=params, data=data)
    response.raise_for_status()
    return response.json()
