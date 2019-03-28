From continuumio/miniconda
RUN conda update -y conda
RUN conda create --name vec_engine python=3.6
RUN echo "source activate vec_engine" > ~/.bashrc
ENV PATH /opt/conda/envs/env/bin:$PATH
#RUN conda install -y faiss-gpu cuda90 -c pytorch
#RUN pip install flask flask-restful flask_sqlalchemy flask_script pymysql environs
WORKDIR /root/front-source
EXPOSE 5000
