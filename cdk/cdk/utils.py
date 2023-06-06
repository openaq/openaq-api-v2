from os import environ
from pathlib import Path
import subprocess

from aws_cdk import aws_lambda


def dictstr(item):
    return item[0], str(item[1])


def stringify_settings(data):
    return dict(map(dictstr, data.dict().items()))


def create_dependencies_layer(
    self, env_name: str, function_name: str, requirements_path: Path
) -> aws_lambda.LayerVersion:
    requirements_file = str(requirements_path.resolve())
    output_dir = f"../.build/{function_name}"
    layer_id = f"openaq-{function_name}-{env_name}-dependencies"

    if not environ.get("SKIP_BUILD"):
        print(f"Building {layer_id} from {requirements_file} into {output_dir}")
        subprocess.run(
            f"""/usr/bin/python3.9 -m pip install -qq -r {requirements_file} \
            -t {output_dir}/python && \
            cd {output_dir}/python && \
            find . -type f -name '*.pyc' | \
              while read f; do n=$(echo $f | \
              sed 's/__pycache__\///' | \
              sed 's/.cpython-[2-3] [0-9]//'); \
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
        compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_9],
    )
