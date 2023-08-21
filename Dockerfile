FROM lambci/lambda:build-python3.10

WORKDIR /var/task

COPY openaq_fastapi/ /var/task

RUN pip install . -t /var/task

# Reduce package size and remove useless files
RUN \
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

RUN zip --symlinks -r9q /tmp/package.zip *