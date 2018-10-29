#!/bin/bash

export PORT=24341
export CAS_UPLOAD_DIR=/share

cd /share
/src/bin/casuploadserver
