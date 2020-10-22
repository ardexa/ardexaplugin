"""A suite of common tools for developing Ardexa Plugins

See:
https://github.com/ardexa
https://app.ardexa.com
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

# Get the long description from the README file
with open('README.md') as f:
    long_description = f.read()

setup(
    name='ardexaplugin',
    version='2.2.0',
    description='A suite of common tools for developing Ardexa Plugins',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ardexa/ardexaplugin',
    author='Ardexa Pty Limited',
    author_email='support@ardexa.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],

    keywords='development iot ardexa',

    packages=["ardexaplugin"],
    install_requires=[
        'psutil',
    ],
    package_data={
        'data': ['data/plugin.service']
    },
    include_package_data=True,

    project_urls={
        'Bug Reports': 'https://github.com/ardexa/ardexaplugin/issues',
        'Source': 'https://github.com/ardexa/ardexaplugin/',
    },
)
