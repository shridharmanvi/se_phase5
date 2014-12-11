from flask import Flask, render_template, request,session
from flask_pymongo import PyMongo
from bson import json_util
import json
from flask.json import jsonify
from bson.json_util import dumps

app = Flask(__name__)

app.secret_key = 'dfgsdfgsfgdfsg'
app.config['MONGO_HOST'] = '50.84.62.186'
app.config['MONGO_PORT'] = 27017
app.config['MONGO_DBNAME'] = 'sphv'
app.config['MONGO_USERNAME'] = 'sphv'
app.config['MONGO_PASSWORD'] = '2s3WW6ut'
mongo = PyMongo(app)


JSON_RESULT = []


@app.route("/")
def home_page():
    return render_template('index.html')

@app.route('/search',methods=['POST'])  

def  search():
    '''
    Get the drugname given in the html form.
    '''
    drug_name = request.form['name']
    session['drug_name']=drug_name
    
    '''
    Get IngRxCui from Rxterms, such that, it will be used further to retrieve data from NDF where (Rxterms.ING_RXCUI = Ndfrt.RxNorm_CUI)
    '''
    IngRxCui = getRxterms_data(drug_name)
    '''
    Obtained IngRxCui will be used to retrieve the data from ndf collection.
    '''
    if len(IngRxCui) > 0:
        getNdfrt_data(IngRxCui)
    '''
    Query FDA1 collection with the given drugname, get the primary id list for the drug.
    '''
    Primaryid = getFDA1_data(drug_name) 
    '''
    Obtained Primaryid is used to query and retrieve data from FDA collection.
    '''
    getFDA_data(Primaryid)
    
    global JSON_RESULT
    '''
    Return the final attached result to display on the html page.
    '''
    return (jsonify(Result = JSON_RESULT))

"""
Method        : getRxterms_data(drugName)
Functionality : Get's all the data from 'RxTerms' collection with the given 'drugname'
"""
def getRxterms_data(drugName):
    # Query RxTerms collection with the given drugName and make sure not to fetch the RETIRED records.
    rxTerms = mongo.db.RxTerms.find({ "$and": [ {'DISPLAY_NAME':{'$regex':drugName, '$options' : 'i'}}, {'IS_RETIRED':{"$ne":"TRUE"}} ] })
    global JSON_RESULT
    rxTerms_json = []
    # Iterate through each record from rxTerms
    for items in rxTerms:
        # Get each item and load it inside json
        rxTerms_json.append(json.loads(json_util.dumps(items)))

    if len(JSON_RESULT) >= 1:
        JSON_RESULT[:] = []
    
    # Check if rxTerms_json contain's any data, if yes, append it to final JSON_RESULT
    if (len(rxTerms_json) >= 1):

        JSON_RESULT.append(rxTerms_json)
    
    # Get ING_RXCUI
    ingRxcui = []
    for val in rxTerms_json:
        # Form a list of ING_RXCUI, which will be used further to query Ndfrt
        if 'ING_RXCUI' in val:
            if val['ING_RXCUI'] not in ingRxcui:
                ingRxcui.append(val['ING_RXCUI'])
    return ingRxcui

'''
Method            : getFDA1_data(drugName)
Functionality     : Get's all the data from 'FDA1' collection with the given 'drugname'
                    From files INDI,DRUG,THER
'''
def getFDA1_data(drugName):
    # Query FDA1 collection with the given drugName.
    fda1 = mongo.db.FDA1.find({'DRUGNAME':{'$regex':drugName, '$options' : 'i'}}, {'_id':0})
    
    fda1_json = []
    # Iterate through each record from fda1
    for fda1item in fda1:
        fda1_json.append(json.loads(json_util.dumps(fda1item)))
    
    # Make a list of PRIMARY_ID's obtained from fda1, which will be further used to retrieve the records from FDA collection.
    primaryid = []
    for val in fda1_json:
        if val['PRIMARY_ID'] not in primaryid:
            primaryid.append(val['PRIMARY_ID'])
    
    if (len(fda1_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(fda1_json)
        
    return primaryid
    
'''
Method        : getFDA_data(primary_id)
Functionality : Get's all the data from OUTC,REAC,RSPR,DEMO files embedded and stored in FDA collection for the obtained PRIMARY_ID.
'''
def getFDA_data(primary_id):
    FDA_json = []
    # For each PRIMARY_ID from FDA1 collection query FDA collection.
    for item in primary_id:
        fdadata = mongo.db.FDA.find({'PRIMARY_ID':int(item)}, {'_id':0})
        
        # Iterate through each FDA item and append the same to final json result.
        for fdaitem in fdadata:
            FDA_json.append(json.loads(json_util.dumps(fdaitem)))
        
    if (len(FDA_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(FDA_json)
        #print (JSON_RESULT)

'''
Method        : getNdfrt_data(RxNormcui)
Functionality : Query ndf collection with RxNormcui (ING_RXCUI) obtained from RxTerms
'''   
def getNdfrt_data(RxNormcui):

    RxNormcui_json = []
    # Query ndf collection with RxNormcui obtained from RxTerms.ING_RXCUI.
    for item in RxNormcui:
        ndfData = mongo.db.ndf.find({'RxNorm_CUI':str(item)}, {'_id':0})
        
        # Iterate through each ndfData to store and append the same to final json result
        for ndfitem in ndfData:
            RxNormcui_json.append(json.loads(json_util.dumps(ndfitem)))
       
    if (len(RxNormcui_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(RxNormcui_json)
        
  
if __name__ == "__main__":

    app.run(debug=True)
