# Copyright 2020 Peeply Technologies Private Limited
#
# Licensed under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in
# compliance with the License. You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
from setuptools import setup, find_packages

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
    name='atlan-lineage-airflow',
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
    keywords='data lineage airflow data governance \
                data democratization atlan atlas',
    url='https://github.com/atlanhq/atlan-airflow-lineage-plugin',
    author='Atlan',
    author_email='support@atlan.com',
    license='Apache License 2.0',
    packages=packages,
    include_package_data=True,
    install_requires=reqs,
    entry_points={
        'airflow.plugins': [
            'atlan.api = atlan.api:AtlanRESTApiPlugin'
        ]
    }
)

if __name__ == '__main__':
    setup(**setup_args)
