#!/bin/bash

# Need to be in directory that has the package.json
conda build -c flatiron -c conda-forge devel/conda.recipe
