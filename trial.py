import json

from flask import Flask, jsonify, url_for, request
from flask.ext.pymongo import PyMongo
from bson import json_util


app = Flask(__name__)
# to hold the current Version
version = u'v1'
# this will configure the MONGO_DBNAME
# see: https://flask-pymongo.readthedocs.org/en/latest/
app.config['MONGO_DBNAME'] = 'sphv'
app.config['MONGO_USERNAME'] = 'sphv'
app.config['MONGO_PASSWORD'] = '2s3WW6ut'
app.config['MONGO_HOST'] = '50.84.62.186'
app.config['MONGO_PORT'] = '27017'

mongo = PyMongo(app)

#print app.config
#db = MongoEngine(app)


# our base test
# {root}/
@app.route('/')
def hello_world():
    return 'World! How are you???'


# {root}/api/v1/loans/
# returns: all loans in the database
@app.route('/trial/<drugname>', methods=['GET'])
def trial(drugname):
    if request.method == 'GET':
        print '1'
        print drugname
        rx = mongo.db.RxTerms.find({"FULL_GENERIC_NAME":drugname})
        fda = mongo.db.RxTerms.find({"DRUGNAME":drugname})
        print "drugname"
        print '2'
        k_json = []
        #k_json = [json.dumps(doc, default=json_util.default) for doc in k]
        #k_json = [doc for doc in k]
        try:
            for doc in rx:
                json_dump = json.dumps(doc, default=json_util.default)
                k_json.append(json_dump)
        except:
            import pdb
            pdb.set_trace()
        print '3'
        
        return jsonify(result=k_json)
    else:
        return "Unsupported MimeType"


if __name__ == '__main__':
    app.debug = True
    app.run()
