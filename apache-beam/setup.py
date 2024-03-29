from setuptools import setup, find_packages
setup(
    name='etl',
    version='1.0.1',
    install_requires=[
        'PyYAML',
        # 'unicodecsv==0.14.1'
    ],
    description='dataflow',
    packages = find_packages(),
    include_package_data=True,
)