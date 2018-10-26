FROM node
MAINTAINER Jeremy Magland

ADD . /src
ADD docker/scripts_inside_docker /scripts
RUN cd /src && npm install .
