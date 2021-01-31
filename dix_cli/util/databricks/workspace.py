from __future__ import annotations

import base64
import json
import os
from typing import Dict, Iterator, Optional, Type

from dix_cli.util import databricks

LANGUAGE_EXTENSION_MAP = {
    'PYTHON': 'py',
    'R': 'r',
    'SCALA': 'scala',
    'SQL': 'sql',
}
EXTENSION_LANGUAGE_MAP = {v: k for k, v in LANGUAGE_EXTENSION_MAP.items()}
SUPPORTED_EXTENSIONS = set(LANGUAGE_EXTENSION_MAP.values())


class DatabricksWorkspaceObject:
    __cache: Optional[str] = None

    def __init__(self, object_id: int, path: str, object_type: str, language: Optional[str] = None):
        self.__object_id = object_id
        self.__path = path
        self.__object_type = object_type
        self.__language = language

    def __eq__(self, other: object):
        if not isinstance(other, DatabricksWorkspaceObject):
            raise NotImplementedError

        return self.get_path(include_extension=True) == other.get_path(include_extension=True)

    def __lt__(self, other: object):
        if not isinstance(other, DatabricksWorkspaceObject):
            raise NotImplementedError

        return self.get_path(include_extension=True) < other.get_path(include_extension=True)

    def get_object_id(self) -> int:
        return self.__object_id

    def get_object_extension(self) -> str:
        if not self.__language:
            raise ValueError('Extension not found')

        return LANGUAGE_EXTENSION_MAP[self.__language]

    def get_path(self, include_extension: bool = False) -> str:
        """
        Returns path to current object on remote workspace
        """
        if include_extension and self.get_object_type() == 'NOTEBOOK':
            return f'{self.__path}.{self.get_object_extension()}'

        return self.__path

    def get_object_type(self) -> str:
        return self.__object_type

    def download_source(self, use_cache: bool = True) -> str:
        """Downloads and returns raw source of this object"""
        if use_cache and self.__cache:
            return self.__cache

        res = databricks.call('api/2.0/workspace/export', {'path': self.get_path(), 'format': 'SOURCE'})
        self.__cache = base64.b64decode(res['content']).decode('utf-8')
        return self.__cache

    def __repr__(self) -> str:
        return f'{self.__path} (id={self.__object_id}, path={self.__path}, type={self.__object_type})'

    @classmethod
    def from_dict(cls: Type[DatabricksWorkspaceObject], input_dict: Dict[str, str]) -> DatabricksWorkspaceObject:
        return DatabricksWorkspaceObject(
            int(str(input_dict.get('object_id'))),
            str(input_dict.get('path')),
            str(input_dict.get('object_type')),
            language=input_dict.get('language'),
        )


def list_objects(path: str, recursive: bool = False) -> Iterator[DatabricksWorkspaceObject]:
    """Returns a list of DatabricksWorkspaceObjects (for a given path)"""
    res = databricks.call('api/2.0/workspace/list', {'path': path})
    remote_objects = map(
        DatabricksWorkspaceObject.from_dict,
        res.get('objects', [])
    )

    for remote_object in remote_objects:
        if recursive and remote_object.get_object_type() == 'DIRECTORY':
            yield from list_objects(remote_object.get_path(), recursive=recursive)
        yield remote_object


def import_object(content: str, path: str, overwrite: bool = False, object_format: str = 'SOURCE') -> None:
    path_object, path_extension = os.path.splitext(path)

    databricks.call('api/2.0/workspace/import', method='POST', data=json.dumps({
        'content': base64.b64encode(content.encode('utf-8')).decode('utf-8'),
        'path': path_object,
        'format': object_format,
        'overwrite': overwrite,
        'language': EXTENSION_LANGUAGE_MAP[path_extension[1:]],
    }))


def delete_object(path: str, recursive: bool = False) -> None:
    databricks.call('api/2.0/workspace/delete', method='POST', data=json.dumps({
        'recursive': recursive,
        'path': path,
    }))


def make_dir(path: str) -> None:
    databricks.call('api/2.0/workspace/mkdirs', method='POST', data=json.dumps({
        'path': path,
    }))
