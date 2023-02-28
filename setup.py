# -*- encoding: utf-8 -*-
import os
thelibFolder = os.path.dirname(os.path.realpath(__file__))
requirementPath = thelibFolder + '/requirements.txt'
install_requires = ['swift']
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()

name = 'swifthlm'
entry_point = '%s.middleware:filter_factory' % (name)
version = '1.0.1'

from setuptools import setup, find_packages

setup(
    name=name,
    version=version,
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'paste.filter_factory': ['%s=%s' % (name, entry_point)]
    },
)
