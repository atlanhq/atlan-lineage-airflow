from setuptools import setup, find_packages
import os
import re

packages = find_packages()

reqs = []
with open("requirements.txt") as f:
    for line in f:
        if line.startswith('git+https'):
            continue
        reqs.append(line.strip())

__version__ = '0.0.1'

packages = find_packages()

reqs = []
with open("requirements.txt") as f:
    for line in f:
        if line.startswith('git+https'):
            continue
        reqs.append(line.strip())

setup_args = dict(
    name='atlan',
    version=__version__,
    description='Plugin to push airflow lineage metadata to Atlan',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins'
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators'
      ],
    keywords='data lineage airflow data governance data democratization atlan atlas',
    url='https://github.com/atlanhq/atlan-airflow-lineage-plugin',
    author='Atlan',
    author_email='support@atlan.com',
    license='Apache License 2.0',
    packages=packages,
    include_package_data=True,
    install_requires=reqs
)

if __name__ == '__main__':
    setup(**setup_args)