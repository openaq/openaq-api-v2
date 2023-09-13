FROM public.ecr.aws/sam/build-python3.11:1.97.0-20230905191328-x86_64

WORKDIR /var/task

COPY requirements.docker.txt /var/task/requirements.txt

RUN pip install --upgrade pip

RUN mkdir -p /var/task/python

RUN pip install -t /var/task/python -r /var/task/requirements.txt

# Reduce package size and remove useless files
RUN \
    cd python \
    find . -type f -name '*.pyc' | \
        while read f; do n=$(echo $f | \
        sed 's/__pycache__\///' | \
        sed 's/.cpython-[2-3] [0-9]//'); \
        cp $f $n; \
        done \
    && find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf \
    && find . -type d -a -name 'tests' -print0 | xargs -0 rm -rf \
    && find . -type d -a -name '*.dist-info' -print0 | xargs -0 rm -rf \
    && find . -type f -a -name '*.so' -exec strip "{}" \;

CMD cp -r /var/task/python /tmp/

