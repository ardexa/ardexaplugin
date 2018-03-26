"""A suite of common tools for developing Ardexa Plugins

See:
https://github.com/ardexa
https://app.ardexa.com
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ardexaplugin',
    version='0.1.0',
    description='A suite of common tools for developing Ardexa Plugins',
    long_description=long_description,
    url='https://github.com/ardexa/ardexaplugin',
    author='Ardexa Pty Limited',
    author_email='support@ardexa.com',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='development iot ardexa',

    py_modules=["ardexaplugin"],

    project_urls={
        'Bug Reports': 'https://github.com/ardexa/ardexaplugin/issues',
        'Source': 'https://github.com/ardexa/ardexaplugin/',
    },
)
