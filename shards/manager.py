import fire
from mishards import db, settings


class DBHandler:
    @classmethod
    def create_all(cls):
        db.create_all()

    @classmethod
    def drop_all(cls):
        db.drop_all()


if __name__ == '__main__':
    db.init_db(settings.DefaultConfig.SQLALCHEMY_DATABASE_URI)
    from mishards import models
    fire.Fire(DBHandler)
