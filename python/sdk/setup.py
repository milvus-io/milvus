import setuptools

long_description = ''

setuptools.setup(
    name="MegaSearch",
    version="0.0.1",
    author="XuanYang",
    author_email="xuan.yang@zilliz.com",
    description="Sdk for using MegaSearch",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],


    python_requires='>=3.4'
)