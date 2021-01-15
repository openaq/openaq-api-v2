from distutils.core import setup

setup(
    name="OpenAQ-FastAPI",
    version="0.0.1",
    author="David Bitner",
    author_email="david@developmentseed.org",
    packages=["openaq_fastapi"],
    url="http://openaq.org/",
    license="LICENSE.txt",
    description="FastAPI API For OpenAQ",
    long_description=open("README.md").read(),
    install_requires=[
        "fastapi",
        "mangum>=0.1.0",
        "fastapi-utils",
        "wheel",
        "pypika",
        "asyncpg",
        "pydantic[dotenv]",
        "buildpg",
        "aiocache",
        "jq",
        "orjson",
        "uvicorn",
        "msgpack",
        "asyncpg",
        "uvicorn",
        "jinja2",
        "typer",
        "markdown",
        "psycopg2-binary",
        "boto3",
        "pytz",
        "dateparser",
        "pyhumps",
    ],
    extras_require={
        "dev": [
            "black",
            "flake8",
            "pytest",
            "requests",
            "schemathesis",
            "hypothesis<6.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "openaqapi=openaq_fastapi.main:run",
            "openaqfetch=openaq_fastapi.ingest.fetch:app",
        ]
    },
    include_package_data=True,
    package_data={
        "": ["*.sql"],
    },
)
