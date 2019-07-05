FROM nvidia/cuda:9.0-devel-ubuntu16.04

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

WORKDIR /app

COPY environment.yaml install/miniconda.sh /app/

RUN ./miniconda.sh -p $HOME/miniconda -b -f \
    && echo ". /root/miniconda/etc/profile.d/conda.sh" >> /root/.bashrc \
    && /root/miniconda/bin/conda env create -f environment.yaml \
    && echo "conda activate vec_engine" >> /root/.bashrc \
    && rm /app/*

COPY . /app
