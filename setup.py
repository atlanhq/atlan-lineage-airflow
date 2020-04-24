from setuptools import setup, find_packages
__version__ = '0.0.1'
setup_args = dict(
    name='atlan_lite',
    version='0.0.1',
    description='Plugin to push airflow lineage to Atlan'
)


if __name__ == '__main__':
    setup(**setup_args)