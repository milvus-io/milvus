# Vecwise Engine

### Geting started

- Install Miniconda first
    - `conda create --name vec_engine python=3.6`
    - `conda activate vec_engine`
    - `conda install faiss-gpu cuda90 -c pytorch # For CUDA9.0`
    - `conda install flask`
    - `pip install flask-restful flask_sqlalchemy`

### Create Database

- Install MySQL
    - `sudo apt-get update`
    - `sudo apt-get install mariadb-server`

- Create user and database:
    - `create user vecwise;`
    - `create database vecdata;`
    - `grant all privileges on vecdata.* to 'vecwise'@'%';`
    - `flush privileges;`

- Create table:
    - `python` # enter python3 interaction environment
    - `from engine import db`
    - `db.create_all()`

- table desc
group_table
+-------------+--------------+------+-----+---------+----------------+
| Field       | Type         | Null | Key | Default | Extra          |
+-------------+--------------+------+-----+---------+----------------+
| id          | int(11)      | NO   | PRI | NULL    | auto_increment |
| group_name  | varchar(100) | YES  |     | NULL    |                |
| file_number | int(11)      | YES  |     | NULL    |                |
+-------------+--------------+------+-----+---------+----------------+

file_table
+------------+--------------+------+-----+---------+----------------+
| Field      | Type         | Null | Key | Default | Extra          |
+------------+--------------+------+-----+---------+----------------+
| id         | int(11)      | NO   | PRI | NULL    | auto_increment |
| group_name | varchar(100) | YES  |     | NULL    |                |
| filename   | varchar(100) | YES  |     | NULL    |                |
| row_number | int(11)      | YES  |     | NULL    |                |
+------------+--------------+------+-----+---------+----------------+
