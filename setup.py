#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from setuptools import setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


setup(
    name="coscli",
    version=get_version("coscli"),
    url="https://github.com/SerhoLiu/coscli",
    description="Coscli is simple command line tool for qcloud COS.",
    long_description=open('README.md').read(),
    author="Serho Liu",
    author_email="serholiu@gmail.com",
    packages=["coscli"],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "click>=2.0",
        "qcloud_cos_v4>=0.0.12",
    ],
    entry_points={
        "console_scripts": [
            "coscli=coscli.__main__:cli"
        ]
    },
    license="MIT License",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: Chinese (Simplified)",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 2 :: Only",
    ]
)
