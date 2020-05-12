from setuptools import setup, find_packages
<<<<<<< HEAD
=======
import os
import re

packages = find_packages()

reqs = []
with open("requirements.txt") as f:
    for line in f:
        if line.startswith('git+https'):
            continue
        reqs.append(line.strip())

>>>>>>> f168c682da7e9ab344ffd369b43dd54adeccf636

__version__ = '0.0.1'

packages = find_packages()

reqs = []
with open("requirements.txt") as f:
    for line in f:
        if line.startswith('git+https'):
            continue
        reqs.append(line.strip())

setup_args = dict(
    name='atlan_lite',
    version='0.0.1',
    description='Plugin to push airflow lineage to Atlan',
    packages=packages,
    include_package_data=True,
    install_requires=reqs
)


if __name__ == '__main__':
    setup(**setup_args)
