from setuptools import find_packages, setup

# Core dependencies that must be installed first
core_requires = [
    "numpy==2.2.4",  # Pinned first for build dependencies
    "setuptools>=75.1.0",
    "importlib_metadata>=4.6.0",
]

backend_requires = [
    "prefect==3.2.15",
    "pendulum==3.0.0", 
    "pandas==2.2.3",
    "pyarrow==19.0.1",
    "netCDF4==1.7.2",
    "boto3==1.28.44",
    "botocore==1.31.44",
    "duckdb==1.2.2",
    "SQLAlchemy==2.0.40",
    "tqdm==4.66.3",
    "scikit-learn==1.5.0",
    "pydantic==2.11.1",  # Required for Prefect 3.x
    "prefect-aws==0.5.8",
    "prefect-sqlalchemy==0.5.2",
]

frontend_requires = [
    "streamlit==1.37.0",
    "plotly==5.18.0",
    "streamlit-folium==0.24.0",
    "pydantic==2.11.1",  # Shared dependency
]

test_requires = [
    "pytest==7.4.2",
    "pytest-mock==3.12.0",
    "pytest-cov==4.1.0",
]

setup(
    name="lightning_containers",
    version="0.0.3",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.10",
    install_requires=core_requires,
    extras_require={
        "backend": backend_requires,
        "frontend": frontend_requires,
        "dev": test_requires + ["mypy==1.9.0", "ruff==0.3.4"],
        "full": backend_requires + frontend_requires
    },
    author="Bayo Adejare",
    author_email="4624500+BayoAdejare@users.noreply.github.com",
    description="Streamlit frontend with Prefect backend for data processing pipelines",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/BayoAdejare/lightning-containers",
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache 2.0 License",
        "Operating System :: OS Independent",
    ],
    project_urls={
        "Documentation": "https://github.com/BayoAdejare/lightning-containers/docs",
        "Source": "https://github.com/BayoAdejare/lightning-containers",
    },
)