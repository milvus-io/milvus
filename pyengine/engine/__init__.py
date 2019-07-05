# -*- coding: utf-8 -*-
from engine import settings
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import flask_profiler

app = Flask(__name__)
app.config.from_object(settings)
app.config['flask_profiler'] = settings.FLASK_PROFILER_CONFIG

#创建数据库对象
print ("Create database instance")
db = SQLAlchemy(app)

from engine.model.group_table import GroupTable
from engine.model.file_table import FileTable

from engine.controller import views

flask_profiler.init_app(app)
