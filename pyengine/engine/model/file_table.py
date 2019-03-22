from engine import db

class FileTable(db.Model):
    __tablename__ = 'file_table'
    id = db.Column(db.Integer, primary_key=True)
    group_name = db.Column(db.String(100))
    filename = db.Column(db.String(100))
    type = db.Column(db.String(100))
    row_number = db.Column(db.Integer)
    seq_no = db.Column(db.Integer)


    def __init__(self, group_name, filename, type, row_number):
        self.group_name = group_name
        self.filename = filename
        self.type = type
        self.row_number = row_number
        self.type = type
        self.seq_no = 0


    def __repr__(self):
        return '<FileTable $r>' % self.tablename

