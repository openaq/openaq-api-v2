import setuptools

aws_cdk_version = "1.79.0"
aws_cdk_reqs = [
    "core",
    "aws-lambda",
    "aws-apigatewayv2",
    "aws-apigatewayv2-integrations",
    "aws-lambda-python",
    "aws-lambda-nodejs",
    "aws-s3",
    "aws-lambda-event-sources",
    "aws_s3_notifications",
]

install_requires = ["docker"]
install_requires.append([f"aws_cdk.{x}=={aws_cdk_version}" for x in aws_cdk_reqs])

setuptools.setup(
    name="openaq-fastapi",
    version="0.0.1",
    description="An empty CDK Python app",
    long_description="hi",
    long_description_content_type="text/markdown",
    author="author",
    # package_dir={"": "../openaq_api"},
    # packages=setuptools.find_packages(where="openaq_api"),
    install_requires=install_requires,
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
