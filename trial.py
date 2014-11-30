import json

from flask import Flask, jsonify, url_for, request
from flask.ext.pymongo import PyMongo
from bson import json_util


app = Flask(__name__)
# to hold the current Version
version = u'v1'
# this will configure the MONGO_DBNAME
# see: https://flask-pymongo.readthedocs.org/en/latest/
app.config['MONGO_DBNAME'] = 'test'
mongo = PyMongo(app)


#db = MongoEngine(app)


# our base test
# {root}/
@app.route('/')
def hello_world():
    return 'Hello World! How are you???'


# {root}/api/v1/loans/
# returns: all loans in the database
@app.route('/trial', methods=['GET'])
def trial():
    if request.method == 'GET':
        fed_loans = mongo.db.Rx.find()
        fed_loans_json = [json.dumps(doc, default=json_util.default) for doc in fed_loans]
        return jsonify(result=fed_loans_json)
    else:
        return "Unsupported MimeType"


if __name__ == '__main__':
    app.debug = True
    app.run()
