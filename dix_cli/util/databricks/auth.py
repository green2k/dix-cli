import configparser
import os


class DatabricksAuthConfig:
    def __init__(self, host: str, token: str):
        self.__host = host
        self.__token = token

    def get_host(self) -> str:
        return self.__host

    def get_token(self) -> str:
        return self.__token


def get_auth_config(config_path=None) -> DatabricksAuthConfig:
    """
    Loads auth config from ~/.databrickscfg
    """
    host = 'n/a'
    token = 'n/a'

    if not config_path:
        config_path = os.path.join(
            os.path.expanduser('~'),
            '.databrickscfg'
        )

    # Load config
    config = configparser.ConfigParser()
    config.read(config_path)

    # Process config
    host = config['DEFAULT']['host']
    token = config['DEFAULT']['token']

    return DatabricksAuthConfig(host, token)
