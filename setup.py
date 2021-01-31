from setuptools import find_packages, setup

from dix_cli.__version__ import VERSION

setup(
    name='dix-cli',
    version=VERSION,
    description='TODO: Description',
    url='TODO: url',
    author='github/green2k',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pqdm',
    ],
    extras_require={
        'dev': [
            'isort',
            'mypy',
            'prospector',
        ],
    },
    entry_points='''
        [console_scripts]
        dix=dix_cli.cli:run
    ''',
    platforms=['any'],
    zip_safe=False
)
