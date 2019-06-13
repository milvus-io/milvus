import setuptools

long_description = ''

setuptools.setup(
    name="Milvus",
    version="0.1.0",
    author="XuanYang",
    author_email="xuan.yang@zilliz.com",
    description="Python Sdk for Milvus",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],


    python_requires='>=3.4'
)