# Milvus分布式部署到多台Docker Host
本篇文档将介绍如何创建Milvus分布式部署，并且提供Ansible Playbook创建所需的Docker Host，和Container来运行分布式Milvus。
### 前置条件：
1. 准备3台VM做Docker Host，单台需要资源4cpu，8GB RAM，用户可以根据需要增加。
2. 虚拟机操作系统，Ubuntu 20.04 LTS。
3. 设置Ansible admin controller，如果新建Ansible controller，建议选择Ubuntu操作系统，系统资源保证能够运行Ansible任务。
4. 下载ansible-milvus-node-deployment playbook。
```
git clone https://github.com/john-h-luo/ansible-milvus-node-deployment.git
```
### 开始安装Docker
#### Ansible Inventory
Ansible Inventory可以对Host分组，在执行相同任务时可以按组分配。
```
[dockernodes] #方括号表示组名
dockernode01 #根据实际Hostname替换此处默认值
dockernode02
dockernode03

[admin]
ansible-controller

[coords]
dockernode01

[nodes]
dockernode02

[dependencies]
dockernode03

[docker:children] #定义"docker"组
dockernodes
coords
nodes
dependencies

[docker:vars] #定义组变量
ansible_python_interpreter=/usr/bin/python3
StrictHostKeyChecking=no
```
#### Ansible.cfg
Ansible配置文件可以控制Playbook中的行为，例如ssh key和其它设置，方便Ansible运行Playbook。
```
[defaults]
host_key_checking = False
inventory = inventory.ini #定义Inventory引用文件
private_key_file=~/.my_ssh_keys/gpc_sshkey #Ansible访问Docker主机的ssh key
```
#### Deploy-docker.yml
Playbook中详细定义了安装Docker的任务，具体请参考内置的注释。
```
- name: setup pre-requisites #安装前置条件
  hosts: all 
  become: yes
  become_user: root
  roles:
    - install-modules
    - configure-hosts-file

- name: install docker
  become: yes
  become_user: root
  hosts: dockernodes
  roles:
    - docker-installation
```
#### 测试Ansible connectivity
在terminal中进入脚本的目录下，运行ansible all -m ping，如果未在ansible.cfg中指定inventory，则需要加入"-i"并指定路径，否则ansible将引用/etc/ansible/hosts的主机地址。  
返回的结果如下:
```
dockernode01 | SUCCESS => {
"changed": false,
"ping": "pong"
}
ansible-controller | SUCCESS => {
    "ansible_facts": {
        "discovered_interpreter_python": "/usr/bin/python3"
    },
    "changed": false,
    "ping": "pong"
}
dockernode03 | SUCCESS => {
    "changed": false,
    "ping": "pong"
}
dockernode02 | SUCCESS => {
    "changed": false,
    "ping": "pong"
}
```
#### 检查Playbook语法
运行ansible-playbook deploy-docker.yml --syntax-check检查脚本是否有语法错误。  
返回的正常结果如下：
```
playbook: deploy-docker.yml
```
#### 安装Docker
运行ansible-playbook deploy-docker.yml
部分返回结果看起来如下图：
```
TASK [docker-installation : Install Docker-CE] *******************************************************************
ok: [dockernode01]
ok: [dockernode03]
ok: [dockernode02]

TASK [docker-installation : Install python3-docker] **************************************************************
ok: [dockernode01]
ok: [dockernode02]
ok: [dockernode03]

TASK [docker-installation : Install docker-compose python3 library] **********************************************
changed: [dockernode01]
changed: [dockernode03]
changed: [dockernode02]

PLAY RECAP *******************************************************************************************************
ansible-controller         : ok=3    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
dockernode01               : ok=10   changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
dockernode02               : ok=10   changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
dockernode03               : ok=10   changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
```
到这里Docker就已经成功地安装到了3台主机上，接下来我们检查Docker安装是否成功。
#### 检查Docker是否安装成功
SSH分别登录到3台主机，运行docker -v，root以外的帐户运行sudo docker -v。  
返回结果如下：
```
root@swarm-manager:~$ docker -v
Docker version 20.10.14, build a224086
```
运行docker ps，初始状态下，返回结果没有运行的container。
### 创建Milvus
#### 检查deploy-milvus.yml
在进入目录后运行ansible-playbook deploy-milvus.yml --syntax-check来检查语法错误。  
正常的返回结果为：
```
playbook: deploy-milvus.yml
```
#### 创建Milvus Container
运行 ansible-playbook deploy-milvus.yml，创建Milvus的任务已在deploy-milvus.yml中定义，在脚本中有详细说明。  
返回结果如下：
```
PLAY [Create milvus-etcd, minio, pulsar, network] *****************************************************************

TASK [Gathering Facts] ********************************************************************************************
ok: [dockernode03]

TASK [etcd] *******************************************************************************************************
changed: [dockernode03]

TASK [pulsar] *****************************************************************************************************
changed: [dockernode03]

TASK [minio] ******************************************************************************************************
changed: [dockernode03]

PLAY [Create milvus nodes] ****************************************************************************************

TASK [Gathering Facts] ********************************************************************************************
ok: [dockernode02]

TASK [querynode] **************************************************************************************************
changed: [dockernode02]

TASK [datanode] ***************************************************************************************************
changed: [dockernode02]

TASK [indexnode] **************************************************************************************************
changed: [dockernode02]

PLAY [Create milvus coords] ***************************************************************************************

TASK [Gathering Facts] ********************************************************************************************
ok: [dockernode01]

TASK [rootcoord] **************************************************************************************************
changed: [dockernode01]

TASK [datacoord] **************************************************************************************************
changed: [dockernode01]

TASK [querycoord] *************************************************************************************************
changed: [dockernode01]

TASK [indexcoord] *************************************************************************************************
changed: [dockernode01]

TASK [proxy] ******************************************************************************************************
changed: [dockernode01]

PLAY RECAP ********************************************************************************************************
dockernode01               : ok=6    changed=5    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
dockernode02               : ok=4    changed=3    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
dockernode03               : ok=4    changed=3    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
```
到这里Milvus已部署到3台Docker主机上，接下来可以参考[Hello Milvus](https://milvus.io/docs/v2.0.x/example_code.md)进行一个hello_milvus.py的测试。