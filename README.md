# Vecwise Engine Dev Guide

## Install via Conda
1.  Install Miniconda first
    - `bash vecwise_engine/install/miniconda.sh`

2.  Create environment
    - `conda env create -f vecwise_engine/environment.yaml`

3.  Test your installation

## Install via Docker

1.  Install nvidia-docker

2.  `docker build -t cuda9.0/VecEngine .`

3.  `docker run -it cuda9.0/VecEngine bash`


## Create Database
1. Install MySQL
    - `sudo apt-get update`
    - `sudo apt-get install mariadb-server`

2. Create user and database:
    - `create user vecwise;`
    - `create database vecdata;`
    - `grant all privileges on vecdata.* to 'vecwise'@'%';`
    - `flush privileges;`

3. Create table:
    - `cd vecwise_engine/pyengine && python manager.py create_all`