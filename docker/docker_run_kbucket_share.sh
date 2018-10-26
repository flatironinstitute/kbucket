#!/bin/bash

docker run -v $1:/share -it magland/kbucket bash /scripts/kbucket_share.sh
