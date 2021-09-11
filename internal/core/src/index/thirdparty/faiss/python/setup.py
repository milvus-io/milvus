from __future__ import print_function
from setuptools import setup, find_packages
import os
import shutil

here = os.path.abspath(os.path.dirname(__file__))

check_fpath = os.path.join("_swigfaiss.so")
if not os.path.exists(check_fpath):
    print("Could not find {}".format(check_fpath))
    print("Have you run `make` and `make -C python`?")

# make the faiss python package dir
shutil.rmtree("faiss", ignore_errors=True)
os.mkdir("faiss")
shutil.copyfile("faiss.py", "faiss/__init__.py")
shutil.copyfile("swigfaiss.py", "faiss/swigfaiss.py")
shutil.copyfile("_swigfaiss.so", "faiss/_swigfaiss.so")
try:
    shutil.copyfile("swigfaiss_avx2.py", "faiss/swigfaiss_avx2.py")
    shutil.copyfile("_swigfaiss_avx2.so", "faiss/_swigfaiss_avx2.so")
except:
    pass

long_description="""
Faiss is a library for efficient similarity search and clustering of dense
vectors. It contains algorithms that search in sets of vectors of any size,
 up to ones that possibly do not fit in RAM. It also contains supporting
code for evaluation and parameter tuning. Faiss is written in C++ with
complete wrappers for Python/numpy. Some of the most useful algorithms
are implemented on the GPU. It is developed by Facebook AI Research.
"""
setup(
    name='faiss',
    version='1.6.3',
    description='A library for efficient similarity search and clustering of dense vectors',
    long_description=long_description,
    url='https://github.com/facebookresearch/faiss',
    author='Matthijs Douze, Jeff Johnson, Herve Jegou, Lucas Hosseini',
    author_email='matthijs@fb.com',
    license='MIT',
    keywords='search nearest neighbors',

    install_requires=['numpy'],
    packages=['faiss'],
    package_data={
        'faiss': ['*.so'],
    },

)
