
# OpenAQ Version 2 API CDK Deployment

This is an [AWS CDK](https://aws.amazon.com/cdk/) project that can be used to deploy Lambda Functions for data ingest and the FastAPI based OpenAQ Version 2 API.

This code will package the API code and all dependencies into a package.zip file using Docker. CDK can then be used to deploy the code as an AWS CloudFormation Stack.

It is recommended to install this code in a virtual environment.

Before install the code, you must have Docker and CDK installed on the system.

CDK can be installed using the node package manager:

`
npm install -g aws-cdk
`

Install the cdk project:

`
pip install -e .
`

You must have your environment set up ([Setting up your Environment](../README.md)) prior to deploying.

There are three targets for building:
- openaq-lcs-apistaging (Staging API)
- openaq-lcs-api (Production API)
- openaq-lcs-ingeststaging (Ingest Lambda)

To build the project:

`
cdk synth [target]
`

To see what changes will be made in a deploy:

`
cdk diff [target]
`

To deploy:

`
cdk deploy [target]
`

**Note that you do not need to run the synth and diff separately to deploy.**

`cdk deploy`  will return the URL that each resource is available through.