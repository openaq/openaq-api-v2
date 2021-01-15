import pathlib
from pathlib import Path

import docker
from aws_cdk import aws_lambda, aws_s3, core
from aws_cdk.aws_apigatewayv2 import HttpApi, HttpMethod
from aws_cdk.aws_apigatewayv2_integrations import LambdaProxyIntegration
from pydantic import BaseSettings

code_dir = pathlib.Path(__file__).parent.absolute()
parent = code_dir.parent.absolute()
env_file = Path.joinpath(parent, ".env")


class Settings(BaseSettings):
    DATABASE_URL: str
    DATABASE_WRITE_URL: str
    OPENAQ_ENV: str = "staging"
    OPENAQ_FASTAPI_URL: str
    TESTLOCAL: bool = True
    OPENAQ_FETCH_BUCKET: str
    OPENAQ_ETL_BUCKET: str

    class Config:
        env_file = env_file


settings = Settings()
OPENAQ_FETCH_BUCKET = "openaq-fetches"


code_dir = pathlib.Path(__file__).parent.absolute()
docker_dir = code_dir.parent.absolute()


def dictstr(item):
    return item[0], str(item[1])


env = dict(map(dictstr, settings.dict().items()))
print(env)

# create package using docker
client = docker.from_env()
client.images.build(
    path=str(docker_dir),
    dockerfile="Dockerfile",
    tag="openaqfastapi",
    nocache=False,
)
client.containers.run(
    image="openaqfastapi",
    command="/bin/sh -c 'cp /tmp/package.zip /local/package.zip'",
    remove=True,
    volumes={str(code_dir): {"bind": "/local/", "mode": "rw"}},
    user=0,
)

stagingpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)
prodpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)
ingestpackage = aws_lambda.Code.asset(
    str(pathlib.Path.joinpath(code_dir, "package.zip"))
)


class LambdaApiStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        package,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        openaq_api = aws_lambda.Function(
            self,
            f"{id}-lambda",
            code=package,
            handler="openaq_fastapi.main.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=core.Duration.seconds(30),
            environment=env,
        )

        api = HttpApi(
            self,
            f"{id}-endpoint",
            default_integration=LambdaProxyIntegration(handler=openaq_api),
            cors_preflight={
                "allow_headers": [
                    "Authorization",
                    "*",
                ],
                "allow_methods": [
                    HttpMethod.GET,
                    HttpMethod.HEAD,
                    HttpMethod.OPTIONS,
                    HttpMethod.POST,
                ],
                "allow_origins": ["*"],
                "max_age": core.Duration.days(10),
            },
        )

        core.CfnOutput(self, "Endpoint", value=api.url)


class LambdaIngestStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        package,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, *kwargs)

        ingest_function = aws_lambda.Function(
            self,
            f"{id}-ingestlambda",
            code=package,
            handler="openaq_fastapi.ingest.handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            allow_public_subnet=True,
            memory_size=1512,
            timeout=core.Duration.seconds(900),
            environment=env,
        )

        openaq_fetch_bucket = aws_s3.Bucket.from_bucket_name(
            self, "{id}-OPENAQ-FETCH-BUCKET", OPENAQ_FETCH_BUCKET
        )

        openaq_fetch_bucket.grant_read(ingest_function)


app = core.App()
print(f"openaq-lcs-api{settings.OPENAQ_ENV}")
staging = LambdaApiStack(app, "openaq-lcs-apistaging", package=stagingpackage)
prod = LambdaApiStack(app, "openaq-lcs-api", package=prodpackage)
ingest = LambdaIngestStack(
    app, f"openaq-lcs-ingest{settings.OPENAQ_ENV}", package=ingestpackage
)
core.Tags.of(staging).add("devseed", "true")
core.Tags.of(staging).add("lcs", "true")
core.Tags.of(ingest).add("devseed", "true")
core.Tags.of(ingest).add("lcs", "true")
core.Tags.of(prod).add("devseed", "true")
core.Tags.of(prod).add("lcs", "true")
app.synth()
