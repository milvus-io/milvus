from flask_script import Manager

from engine import db, app

manager = Manager(app)

@manager.command
def create_all():
    db.create_all()

@manager.command
def drop_all():
    db.drop_all()

@manager.command
def recreate_all():
    db.drop_all()
    db.create_all()

if __name__ == '__main__':
    manager.run()
