from setuptools import find_packages, setup

setup(
    name="lightning_containers",
    version="0.0.1",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "importlib_metadata>=4.6.0",
        "setuptools>=75.1.0",
        ## backend dependencies
        "prefect==2.20.17",
        "netCDF4==1.6.5",
        "boto3==1.28.44",
        "botocore==1.31.44",
        "SQLAlchemy==2.0.25",
        "tqdm==4.66.3",
        "numpy==1.26.3",  # Add numpy with a specific version
        "scipy==1.11.4",
        "pandas==2.0.3",
        "scikit-learn==1.5.0",
        # frontend dependencies
        "streamlit==1.37.0",
        "plotly==5.18.0",
        # "folium==0.17.0",
    ],
    author="Bayo Adejare",
    author_email="4624500+BayoAdejare@users.noreply.github.com",
    extras_require={"dev": ["pytest==7.4.2", "pydantic>=1.8.0", "pyarrow>=6.0.0"]},
)
