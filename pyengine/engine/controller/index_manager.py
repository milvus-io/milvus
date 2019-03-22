from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from engine import app, db
from engine.model.group_table import GroupTable
from engine.controller.vector_engine import VectorEngine

# app = Flask(__name__)
api = Api(app)


from flask_restful import reqparse
from flask_restful import request
class Vector(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=float, action='append', location=['json'])

    def post(self, group_id):
        args = self.__parser.parse_args()
        vector = args['vector']
        return VectorEngine.AddVector(group_id, vector)


class VectorSearch(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=float, action='append', location=['json'])
        self.__parser.add_argument('limit', type=int, action='append', location=['json'])

    def post(self, group_id):
        args = self.__parser.parse_args()
        print('vector: ', args['vector'])
        # go to search every thing
        return VectorEngine.SearchVector(group_id, args['vector'], args['limit'])


class Index(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        # self.__parser.add_argument('group_id', type=str)

    def post(self, group_id):
        return VectorEngine.CreateIndex(group_id)


class Group(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('group_id', type=str)
        self.__parser.add_argument('dimension', type=int, action='append', location=['json'])

    def post(self, group_id):
        args = self.__parser.parse_args()
        dimension = args['dimension']
        return VectorEngine.AddGroup(group_id, dimension)

    def get(self, group_id):
        return VectorEngine.GetGroup(group_id)

    def delete(self, group_id):
        return VectorEngine.DeleteGroup(group_id)


class GroupList(Resource):
    def get(self):
        return VectorEngine.GetGroupList()


api.add_resource(Vector, '/vector/add/<group_id>')
api.add_resource(Group, '/vector/group/<group_id>')
api.add_resource(GroupList, '/vector/group')
api.add_resource(Index, '/vector/index/<group_id>')
api.add_resource(VectorSearch, '/vector/search/<group_id>')


# if __name__ == '__main__':
#     app.run()
