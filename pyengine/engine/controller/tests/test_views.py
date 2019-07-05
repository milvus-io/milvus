from engine.controller.vector_engine import VectorEngine
from engine.settings import DATABASE_DIRECTORY
from engine import app
from flask import jsonify
import pytest
import os
import logging
import json

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestViews:
    HEADERS = {'Content-Type': 'application/json'}

    def loads(self, resp):
        return json.loads(resp.data.decode())

    def test_group(self, test_client):
        data = {"dimension": 10}

        resp = test_client.get('/vector/group/6', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 1

        resp = test_client.post('/vector/group/6', data=json.dumps(data), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        resp = test_client.get('/vector/group/6', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        # GroupList
        resp = test_client.get('/vector/group', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0
        assert self.loads(resp)['group_list'] == [{'file_number': 0, 'group_name': '6'}]

        resp = test_client.delete('/vector/group/6', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0
    
    
    def test_vector(self, test_client):
        dimension = {"dimension": 8}
        resp = test_client.post('/vector/group/6', data=json.dumps(dimension), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0
        
        vector = {"vector": [[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]]}
        resp = test_client.post('/vector/add/6', data=json.dumps(vector), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        vector = {"vector": [[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]]}
        resp = test_client.post('/vector/add/6', data=json.dumps(vector), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        vector = {"vector": [[1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8], [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8], [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8]]}
        resp = test_client.post('/vector/add/6', data=json.dumps(vector), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        resp = test_client.post('/vector/index/6', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0

        limit = {"vector": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8], "limit": 1}
        resp = test_client.get('/vector/search/6', data=json.dumps(limit), headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0
        assert self.loads(resp)['vector_id'] == ['6.0']

        resp = test_client.delete('/vector/group/6', headers = TestViews.HEADERS)
        assert resp.status_code == 200
        assert self.loads(resp)['code'] == 0




