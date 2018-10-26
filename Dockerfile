FROM node

RUN npm install -g @magland/kbucket@0.11.1
#ADD scripts_inside_docker /scripts

ADD . /src
ADD docker/scripts_inside_docker /scripts
RUN cd /src && npm install .
