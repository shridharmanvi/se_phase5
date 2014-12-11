import json
import re
from flask import Flask, jsonify, url_for, request
from flask.ext.pymongo import PyMongo
from bson import json_util


app = Flask(__name__)
app.config['MONGO_DBNAME'] = 'sphv'
app.config['MONGO_USERNAME'] = 'sphv'
app.config['MONGO_PASSWORD'] = '2s3WW6ut'
app.config['MONGO_HOST'] = '50.84.62.186'
app.config['MONGO_PORT'] = '27017'

mongo = PyMongo(app)

#(r'.*hey.*')

@app.route('/')
def hello_world():
    return 'World! How are you???'


@app.route('/cse5320/<drugname>', methods=['GET'])
def trial(drugname):
    if request.method == 'GET':
        """
        dn='r'+"'.*"+drugname+".*'"
        regexp=re.compile(dn)
        #dn='/'+drugname+'/'
        """
        dn='/'+str(drugname)+'/'
        rx = mongo.db.RxTerms.find({'DISPLAY_NAME':dn})
        #fda = mongo.db.fda1.find({"Display_Name":regexp})
        #ndf= mongo.db.ndf.find({"Display_Name":regexp})
        print drugname
        print '2'
        k_json = []
        
        try:
            for doc in rx:
                json_dump = json.dumps(doc, default=json_util.default)
                k_json.append(json_dump)
        except:
            import pdb
            pdb.set_trace()
        print '3'
        """
        try:
            for doc in fda:
                json_dump = json.dumps(doc, default=json_util.default)
                k_json.append(json_dump)
        except:x=1
        
        try:
            for doc in fda:
                json_dump = json.dumps(doc, default=json_util.default)
                k_json.append(json_dump)
        except:x=1
        """
        return jsonify(result=k_json)
    else:
        return "Unsupported MimeType"


if __name__ == '__main__':
    app.debug = True
    app.run()
