#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup


def get_long_description():
    with open("README.md", encoding="utf8") as f:
        return f.read()


def get_packages(package):
    return [
        dirpath
        for dirpath, dirnames, filenames in os.walk(package)
        if os.path.exists(os.path.join(dirpath, "__init__.py"))
    ]


setup(
    name="adc-aiopg",
    version="0.1.0",
    url="https://github.com/ascet-dev/adc-aiopg",
    python_requires=">=3.8",
    install_requires=[
        "pydantic>=2.0.0",
        "asyncpg>=0.27.0",
        "alembic>=1.11.0",
        "sqlalchemy>=2.0.0",
        "sqlmodel>=0.0.8",
        "ujson>=5.10.0",
        "psycopg2-binary>=2.9.0",
        "sqlalchemy-utils>=0.41.0",
    ],
    license="MIT",
    description="Async PostgreSQL client with connection pooling",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    packages=get_packages("adc_aiopg"),
    include_package_data=True,
    data_files=[("", [])],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    zip_safe=False,
)
