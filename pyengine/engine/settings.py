from environs import Env

env = Env()
env.read_env()

DEBUG = env.bool('DEBUG', default=False)
SQLALCHEMY_TRACK_MODIFICATIONS = env.bool('DEBUG', default=False)
SECRET_KEY = env.str('SECRET_KEY', 'test')
SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_URI')

ROW_LIMIT = env.int('ROW_LIMIT')
DATABASE_DIRECTORY = env.str('DATABASE_DIRECTORY')
