from setuptools import find_packages, setup

setup(
    name="lightning_containers",
    version="0.0.1",
    packages=find_packages(exclude=["lightning_containers_tests"]),
    install_requires=[
        ## backend dependencies
        "prefect==2.20.9",
        "netCDF4==1.6.5",
        "pandas==2.0",
        "boto3==1.28.44",
        "botocore==1.31.44",
        "scikit-learn==1.3.0",
        "SQLAlchemy==2.0.25",
        "tqdm==4.66.1",
        # frontend dependencies
        "streamlit==1.31.0",
        "plotly==5.18.0",
    ],
    author="Bayo Adejare",
    author_email="4624500+BayoAdejare@users.noreply.github.com",
    extras_requires={"dev": ["pytest==7.4.2"]},
)
