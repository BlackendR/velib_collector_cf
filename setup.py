from setuptools import setup, find_packages

setup(
    name="velib_collector",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery==3.11.4",
        "requests==2.31.0",
        "python-dotenv==1.0.0",
    ],
) 