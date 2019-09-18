import fire
from mishards import db

class DBHandler:
    @classmethod
    def create_all(cls):
        db.create_all()

    @classmethod
    def drop_all(cls):
        db.drop_all()

if __name__ == '__main__':
    fire.Fire(DBHandler)
