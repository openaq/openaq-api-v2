from os import environ
from pathlib import Path
import subprocess
import docker
import shutil
import os
import pathlib
from aws_cdk import aws_lambda


def dictstr(item):
    return item[0], str(item[1])


def stringify_settings(data: dict):
    return dict(map(dictstr, data.model_dump(exclude_unset=True).items()))


def create_dependencies_layer(
    self,
    env_name: str,
    function_name: str,
    python_version,
) -> aws_lambda.LayerVersion:
    output_dir = f"../.build/{function_name}"
    layer_id = f"openaq-{function_name}-{env_name}-dependencies"

    if not environ.get("SKIP_BUILD"):
        print(f'Building {layer_id} into {output_dir}')
        ## migrate to the package/function directory to export and install
        subprocess.run(
            f"""
             cd ../{function_name} && poetry export --only main -o requirements.txt --without-hashes && \
             poetry run python -m pip install -qq -r requirements.txt \
             -t {output_dir}/python && \
             cd {output_dir} && \
             find . -type f -name '*.pyc' | \
             while read f; do n=$(echo $f | \
             sed 's/__pycache__\///' | \
             sed 's/.cpython-[2-3][0-9]+//'); \
             cp $f $n; \
             done \
             && find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf \
             && find . -type d -a -name 'tests' -print0 | xargs -0 rm -rf \
             && find . -type d -a -name '*.dist-info' -print0 | xargs -0 rm -rf \
             && find . -type f -a -name '*.so' -print0 | xargs -0 strip --strip-unneeded
             """,
            shell=True,
            )

    layer_code = aws_lambda.Code.from_asset(output_dir)

    return aws_lambda.LayerVersion(
        self,
        layer_id,
        code=layer_code,
        compatible_architectures=[aws_lambda.Architecture.X86_64],
        compatible_runtimes=[python_version],
    )
