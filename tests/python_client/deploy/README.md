

## Overview
To test deployment by docker-compose(Both standalone and cluster)

* re-install milvus to check data persistence
    1. Deploy Milvus
    2. Insert data
    3. Build index 
    4. Search
    5. Stop Milvus
    6. Repeat from step #1
* upgrade milvus to check data compatibility
    1. Deploy Milvus （Previous RC)
    2. Insert data
    3. Search
    4. Stop Milvus
    5. Deploy Milvus  (Latest RC)
    6. Build index
    7. Search

## Project structure
```
.
├── README.md
├── cluster # dir to deploy cluster
│   ├── logs # dir to save logs
│   └──docker-compose.yml
├── standalone # dir to deploy standalone
│   ├── logs # dir to save logs
│   └──docker-compose.yml
├── scripts
│   ├── action_after_upgrade.py
│   ├── action_before_upgrade.py
│   ├── action_reinstall.py
│   └── utils.py
├── test.sh # script to run a single task
└── run.sh # script to run all tasks
```

## Usage
Make sure you have installed `docker`,`docker-compose` and `pymilvus`!
For different version, you should modify the value of `latest_tag`, `latest_rc_tag` and `Release`. Password of root is needed for deleting volumes dir.

single test task

```bash
$ bash test.sh -m ${Mode} -t ${Task} -p ${Password}
# Mode, the mode of milvus deploy. standalone or cluster"
# Task, the task type of test. reinstall or upgrade
# Password, the password of root"
```

run all tasks
```bash
$ bash run.sh -p ${Password}
# Password, the password of root"
```

## Integrate deploy test into CI
Provides a way to periodically run docker-compose deployment tests through GitHub action：[deploy-test](https://github.com/milvus-io/milvus/blob/master/.github/workflows/deploy-test.yaml)

- [x] Parallel testing for four deployment scenarios
- [x] Upload logs to artifacts for further debug
- [x] Email notification for test failure
- [ ] Support helm deployment tests
- [ ] Cover more detail information in email notification
