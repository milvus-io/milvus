import fire
from mishards import db
from sqlalchemy import and_


class DBHandler:
    @classmethod
    def create_all(cls):
        db.create_all()

    @classmethod
    def drop_all(cls):
        db.drop_all()

    @classmethod
    def fun(cls, tid):
        from mishards.factories import TablesFactory, TableFilesFactory, Tables
        f = db.Session.query(Tables).filter(and_(
            Tables.table_id == tid,
            Tables.state != Tables.TO_DELETE)
        ).first()
        print(f)

        # f1 = TableFilesFactory()


if __name__ == '__main__':
    fire.Fire(DBHandler)
