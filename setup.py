import setuptools
import sys

py_version = sys.version_info[:2]


version = '0.0.1'
description = 'AMQP implementation using asyncio'

setuptools.setup(
    name='aioamqp',
    version=version,
    author='Benoît Calvez',
    author_email='benoit.calvez@polyconseil.fr',
    url='https://github.com/polyconseil/aioamqp',
    description=description,
    long_description=description,
    download_url='https://pypi.python.org/pypi/aioamqp',
    packages=[
        'aioamqp',
    ],
    install_requires=['asyncio'] if py_version == (3, 3) else [],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
    ],
    platforms='all',
    license='BSD'
)
