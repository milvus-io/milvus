# Requirements
* python 3.6.8+
* pip install -r requirements.txt

# How to use this Test Project
```shell
pytest . --level=1
```
or test connect function only

```shell
pytest test_connect.py --level=1
```

collect cases
```shell
pytest --day-run -qq
```
collect cases with docstring
```shell
pytest --collect-only -qq
```

with allure test report

 ```shell
pytest --alluredir=test_out . -q -v
allure serve test_out
 ```
# Contribution getting started
* Follow PEP-8 for naming and black for formatting.

