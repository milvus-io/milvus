from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


from flask_restful import reqparse
class Vector(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)
        self.__parser.add_argument('vec', type=str)

    def post(self):
        # args = self.__parser.parse_args()
        # vec = args['vec']
        # groupid = args['groupid']
        return "vector post"


class VectorSearch(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)

    def post(self):
        return "vectorSearch post"


class Index(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)

    def post(self):
        return "index post"


class Group(Resource):
    def __init__(self):
        self.__parser = reqparse.RequestParser()
        self.__parser.add_argument('groupid', type=str)

    def post(self, groupid):
        return "group post"

    def get(self, groupid):
        return "group get"

    def delete(self, groupid):
        return "group delete"


class GroupList(Resource):
    def get(self):
        return "grouplist get"


api.add_resource(Vector, '/vector')
api.add_resource(Group, '/vector/group/<groupid>')
api.add_resource(GroupList, '/vector/group')
api.add_resource(Index, '/vector/index')
api.add_resource(VectorSearch, '/vector/search')


if __name__ == '__main__':
    app.run(debug=True)
