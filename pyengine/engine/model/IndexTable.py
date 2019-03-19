from engine import db

class IndexTable(db.Model):
    __tablename__ = 'index_table'
    id = db.Column(db.Integer, primary_key=True)
    tablename = db.Column(db.String(100))
    filename = db.Column(db.String(100))
    type = (db.Integer)

    def __init__(self, tablename, filename, type):
        self.tablename = tablename
        self.filename = filename
        self.type = type

    def __repr__(self):
        return '<IndexTable $r>' % self.tablename