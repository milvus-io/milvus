# -*- coding: utf-8 -*-
from engine import settings
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_object(settings)

#创建数据库对象
print ("Create database instance")
db = SQLAlchemy(app)

from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable

from engine.controller import views
