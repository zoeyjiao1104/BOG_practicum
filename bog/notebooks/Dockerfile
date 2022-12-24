# Pull base Docker image
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-scipy-notebook
FROM jupyter/scipy-notebook:python-3.9

# Create working directory
ARG PROJECTDIR=/usr/src/notebooks
WORKDIR ${PROJECTDIR}

# Install Python 3 packages
COPY requirements.txt .
RUN mamba install --quiet --yes --file requirements.txt -c conda-forge

# Install select directories as packages to facilitate imports
COPY . .
RUN pip install -e .