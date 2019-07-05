from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from engine import app, db
from engine.model.group_table import GroupTable
from engine.controller.vector_engine import VectorEngine
import json

# app = Flask(__name__)
api = Api(app)


from flask_restful import reqparse
from flask_restful import request
class Vector(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=list, action='append', location=['json'])

    def post(self, group_id):
        args = self.__parser.parse_args()
        vector = args['vector']
        code, vector_id = VectorEngine.AddVector(group_id, vector)
        return jsonify({'code': code, 'vector_id': vector_id})


class VectorSearch(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('vector', type=float, action='append', location=['json'])
        self.__parser.add_argument('limit', type=int, location=['json'])

    def get(self, group_id):
        args = self.__parser.parse_args()
        print('VectorSearch vector: ', args['vector'])
        print('limit: ', args['limit'])
        # go to search every thing
        code, vector_id = VectorEngine.SearchVector(group_id, args['vector'], args['limit'])
        print('vector_id: ', vector_id)
        return jsonify({'code': code, 'vector_id': vector_id})
        #return jsonify(})


class Index(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        # self.__parser.add_argument('group_id', type=str)

    def post(self, group_id):
        code = VectorEngine.CreateIndex(group_id)
        return jsonify({'code': code})


class Group(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('group_id', type=str)
        self.__parser.add_argument('dimension', type=int, location=['json'])

    def post(self, group_id):
        args = self.__parser.parse_args()
        dimension = args['dimension']
        code, group_id = VectorEngine.AddGroup(group_id, dimension)
        return jsonify({'code': code, 'group': group_id, 'filenumber': 0})

    def get(self, group_id):
        code, group_id = VectorEngine.GetGroup(group_id)
        return jsonify({'code': code, 'group': group_id, 'filenumber': 0})

    def delete(self, group_id):
        code, group_id = VectorEngine.DeleteGroup(group_id)
        return jsonify({'code': code, 'group': group_id, 'filenumber': 0})


class GroupList(Resource):
    def get(self):
        code, group_list = VectorEngine.GetGroupList()
        return jsonify({'code': code, 'group_list': group_list})


api.add_resource(Vector, '/vector/add/<group_id>')
api.add_resource(Group, '/vector/group/<group_id>')
api.add_resource(GroupList, '/vector/group')
api.add_resource(Index, '/vector/index/<group_id>')
api.add_resource(VectorSearch, '/vector/search/<group_id>')


# if __name__ == '__main__':
#     app.run()
