from environs import Env

env = Env()
env.read_env()

DEBUG = env.bool('DEBUG', default=False)
SQLALCHEMY_TRACK_MODIFICATIONS = env.bool('DEBUG', default=False)
SECRET_KEY = env.str('SECRET_KEY', 'test')
SQLALCHEMY_DATABASE_URI = env.str('SQLALCHEMY_DATABASE_URI')
SQLALCHEMY_POOL_SIZE = env.int('SQLALCHEMY_POOL_SIZE', 50)

ROW_LIMIT = env.int('ROW_LIMIT')
DATABASE_DIRECTORY = env.str('DATABASE_DIRECTORY')

FLASK_PROFILER_CONFIG = {
    "enabled": DEBUG,
    "storage": {
        "engine": "sqlalchemy",
        "db_url": env.str("PROFILER_STORAGE_DB_URL")
    },
    "basicAuth": {
        "enabled": True,
        "username": env.str("PROFILER_BASIC_AUTH_USERNAME", "admin"),
        "password": env.str("PROFILER_BASIC_AUTH_PASSWORD", "admin"),
    }
}
