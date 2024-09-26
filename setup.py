"""Setup tool for dataflow GCP"""


from setuptools import setup, find_packages
setup(
    name='datalake streaming',
    version='0.0.2',
    install_requires=[
        'PyYAML',
        # 'unicodecsv==0.14.1'
        # "pip install 'apache-beam[gcp]'"
        "pyfiglet",
        "avro==1.11.3",
        "google-cloud-storage==2.18.2"  

    ],
    description='dataflow',
    packages = find_packages(),
    include_package_data=True,
    author="My name",
    author_email="my@email.com",
    url="an url example"
)