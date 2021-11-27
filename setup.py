import os
import re
import setuptools
import sys

py_version = sys.version_info[:2]

_file_content = open(os.path.join(os.path.dirname(__file__), 'aioamqp', 'version.py')).read()

rex = re.compile(r"""version__ = '(.*)'.*__packagename__ = '(.*)'""", re.MULTILINE | re.DOTALL)
VERSION, PACKAGE_NAME = rex.search(_file_content).groups()
description = 'AMQP implementation using asyncio'

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author="Polyconseil dev' team",
    author_email='opensource+aioamqp@polyconseil.fr',
    url='https://github.com/polyconseil/aioamqp',
    description=description,
    long_description=open('README.rst').read(),
    keywords=['asyncio', 'amqp', 'rabbitmq', 'aio'],
    download_url='https://pypi.python.org/pypi/aioamqp',
    packages=[
        'aioamqp',
    ],
    install_requires=[
        'pamqp>=3.1.0',
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    platforms='all',
    license='BSD'
)
