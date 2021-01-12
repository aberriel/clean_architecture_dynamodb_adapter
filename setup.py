#!/usr/bin/env python

"""The setup script."""
from requirements import *
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

setup(
    author="Anselmo Lira",
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Implementação concreta de adapter para DynamoDB",
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='clean_architecture_dynamodb_adapter',
    name='clean_architecture_dynamodb_adapter',
    packages=find_packages(include=['clean_architecture_dynamodb_adapter', 'clean_architecture_dynamodb_adapter.*']),
    test_suite='tests',
    url='https://github.com/aberriel/clean_architecture_dynamodb_adapter',
    version='0.1.0',
    zip_safe=False,
)
