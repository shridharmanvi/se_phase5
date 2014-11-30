# flask web ref: http://flask.pocoo.org/docs/0.10/
# great flask walk through: http://blog.luisrei.com/articles/flaskrest.html
# RESTful practices: https://bourgeois.me/rest/

import json

from flask import Flask, jsonify, url_for, request
from flask import Response
#from flask.ext.pymongo import PyMongo
from flask.ext.mongoengine import MongoEngine
#app.config.from_pyfile('the-config.cfg')
db = MongoEngine()
app=Flask(__name__)
db.init_app(app)



app.config['MONGODB_SETTINGS'] = {
    'db': 'test',
    'username':'',
    'password':''
}


from bson import json_util

app = Flask(__name__)
# to hold the current Version
version = u'v1'
# this will configure the MONGO_DBNAME
# see: https://flask-pymongo.readthedocs.org/en/latest/
app.config['MONGO_DBNAME'] = 'test'
mongo = MongoEngine(app)
db = MongoEngine(app)


# our base test
# {root}/
@app.route('/')
def hello_world():
    return 'Hello World! How are you???'


# {root}/api/v1/loans/
# returns: all loans in the database
@app.route('/try', methods=['GET'])
def loan():
    if request.content_type == 'application/json' or request.content_type == 'application/xml' in request.content_type:
        if request.method == 'GET':
            fed_loans = mongo.db.RxTerms.find()
            #fed_loans_json = [json.dumps(doc, default=json_util.default) for doc in fed_loans]
            js=json.dumps(fed_loans)
            resp = Response(js, status=200, mimetype='application/json')
            return resp
    else:
        return "Unsupported MimeType"


if __name__ == '__main__':
    app.debug = True
    app.run()
