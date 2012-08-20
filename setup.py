#!/usr/bin/python
# Copyright (c) 2011 Yun Mao.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = '0.4.0'

setup(
    name='evzookeeper',
    version=version,
    description="zookeeper binding for eventlet",
    license='Apache License (2.0)',
    classifiers=["Programming Language :: Python"],
    keywords='zookeeper eventlet',
    author='Yun Mao',
    author_email='yunmao@gmail.com',
    url='https://github.com/maoy/python-evzookeeper',
    packages=["evzookeeper"],
    )
