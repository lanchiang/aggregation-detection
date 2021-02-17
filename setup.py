# Created by lan at 2021/1/2
from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='aggrdet',
    version='0.1.0',
    description='Aggregation Detection in Verbose CSV Files.',
    long_description=readme,
    author='Lan Jiang',
    author_email='lan.jiang@hpi.de',
    url='https://github.com/lanchiang',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)