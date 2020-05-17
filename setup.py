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
    name='atlan_lite',
    version='0.0.1',
    description='Plugin to push airflow lineage to Atlan',
    packages=packages,
    include_package_data=True,
    install_requires=reqs
)

if __name__ == '__main__':
    setup(**setup_args)
from setuptools import setup, find_packages

setup(name='funniest',
      version='0.1',
      description='The funniest joke in the world',
      long_description='Really, the funniest around.',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Text Processing :: Linguistic',
      ],
      keywords='funniest joke comedy flying circus',
      url='http://github.com/storborg/funniest',
      author='Flying Circus',
      author_email='flyingcircus@example.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'markdown',
      ],
      include_package_data=True,
      zip_safe=False)