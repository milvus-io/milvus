from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from engine import app, db
from engine.model.GroupTable import GroupTable

# app = Flask(__name__)
api = Api(app)


from flask_restful import reqparse
from flask_restful import request
class Vector(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=float, action='append', location=['json'])

    def post(self, groupid):
        args = self.__parser.parse_args()
        vector = args['vector']
        # add vector into file
        print("vector: ", vector)
        return "vector post"


class VectorSearch(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=float, action='append', location=['json'])

    def post(self, groupid):
        args = self.__parser.parse_args()
        print('vector: ', args['vector'])
        # go to search every thing
        return "vectorSearch post"


class Index(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)

    def post(self):
        # go to create index for specific group
        return "index post"


class Group(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)

    def post(self, groupid):
        args = self.__parser.parse_args()
        group = GroupTable.query.filter(GroupTable.group_name==groupid).first()
        if(group):
            return jsonify({'code': 1, 'group_name': groupid, 'file_number': group.file_number})
        else:
            new_group = GroupTable(groupid)
            db.session.add(new_group)
            db.session.commit()
            return jsonify({'code': 0, 'group_name': groupid, 'file_number': 0})


    def get(self, groupid):
        args = self.__parser.parse_args()
        group = GroupTable.query.filter(GroupTable.group_name==groupid).first()
        if(group):
            return jsonify({'code': 0, 'group_name': groupid, 'file_number': group.file_number})
        else:
            return jsonify({'code': 1, 'group_name': groupid, 'file_number': 0}) # not found

    def delete(self, groupid):
        args = self.__parser.parse_args()
        group = GroupTable.query.filter(GroupTable.group_name==groupid).first()
        if(group):
            # old_group = GroupTable(groupid)
            db.session.delete(group)
            db.session.commit()
            return jsonify({'code': 0, 'group_name': groupid, 'file_number': group.file_number})
        else:
            return jsonify({'code': 0, 'group_name': groupid, 'file_number': 0})


class GroupList(Resource):
    def get(self):
        group = GroupTable.query.all()
        group_list = []
        for group_tuple in group:
            group_item = {}
            group_item['group_name'] = group_tuple.group_name
            group_item['file_number'] = group_tuple.file_number
            group_list.append(group_item)

        print(group_list)
        return jsonify(results = group_list)


api.add_resource(Vector, '/vector/add/<groupid>')
api.add_resource(Group, '/vector/group/<groupid>')
api.add_resource(GroupList, '/vector/group')
api.add_resource(Index, '/vector/index')
api.add_resource(VectorSearch, '/vector/search/<groupid>')


# if __name__ == '__main__':
#     app.run()
