"""Setup tool for dataflow GCP"""


from setuptools import setup, find_packages
setup(
    name='arvo',
    version='0.0.1',
    install_requires=[
        'PyYAML',
        # 'unicodecsv==0.14.1'
        # "pip install 'apache-beam[gcp]'"
        "pyfiglet",
        "avro==1.11.3",

    ],
    description='dataflow',
    packages = find_packages(),
    include_package_data=True,
)