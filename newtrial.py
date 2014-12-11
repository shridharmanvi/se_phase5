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
    drug_name = request.form['name']
    session['drug_name']=drug_name
    
    #Calling functions to get data from other files
    print 'hello'
    RxNormcui = getRxterms(drug_name)
    getNdfrt_data(RxNormcui)
      
    Primaryid = getFDA1_data(drug_name) 
    getFDA_data(Primaryid)
     
    global JSON_RESULT
    return (jsonify(Result = JSON_RESULT))


def getRxterms(drugName):
    """
        This function queries the RxTerms201408.txt to find the related RxIngredients.
    """
    rxTerms = mongo.db.RxTerms.find({ "$and": [ {'DISPLAY_NAME':{'$regex':drugName, '$options' : 'i'}}, {'IS_RETIRED':{"$ne":"TRUE"}} ] })
    
    rxTerms_json = []
    #print dumps(rxTerms)
    for items in rxTerms:
        #print items
        rxTerms_json.append(json.loads(json_util.dumps(items)))
    
    #print ('Rxterms:',rxTerms_json)  
    if (len(rxTerms_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(rxTerms_json)
        #print (JSON_RESULT)
    RxNormcui = []
    for val in rxTerms_json:
        RxNormcui.append(val['ING_RXCUI'])
    return RxNormcui
    
def getFDA1_data(drugName):
    """
        To find data from DRUG file in Adverse effects reporting based on queried drug name.
    """
    drugname = session['drug_name']
    adveff = mongo.db.FDA1.find({'DRUGNAME':{'$regex':drugname, '$options' : 'i'}}, {'_id':0})
    adveff_json = []
    for items in adveff:
        adveff_json.append(json.loads(json_util.dumps(items)))
    
    primaryid = []
    for val in adveff_json:
        primaryid.append(val['PRIMARY_ID'])
    
    #print(primaryid)
    
    #print('Drug File:',adveff_json)
    if (len(adveff_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(adveff_json)
        #print (JSON_RESULT)
        
    return primaryid
    #print (drugname)
    
'''
Get all the data from OUTC,REAC,RSPR,DEMO files embeded and stored in FDA db
'''
    
def getFDA_data(primary_id):
    FDA_json = []
    for item in primary_id:
        demofile = mongo.db.FDA.find({'PRIMARY_ID':int(item)}, {'_id':0})
        
        for items in demofile:
            FDA_json.append(json.loads(json_util.dumps(items)))
        
    if (len(FDA_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(FDA_json)
        #print (JSON_RESULT)
   
def getNdfrt_data(RxNormcui):
    """
        To find data from NDFRT XML file based on RxNorm_CUI from RXINGREDIENTS file.
    """
    #print(RxNormcui)
     
    RxNormcui_json = []
    for item in RxNormcui:
        RxNorm_cui = mongo.db.ndf.find({'RxNorm_CUI':str(item)}, {'_id':0})
        #print RxNorm_cui
        
        for items1 in RxNorm_cui:
            print items1
            RxNormcui_json.append(json.loads(json_util.dumps(items1)))
       
    #print ('NDFRT_INGR by ID:',RxNormcui_json)
    if (len(RxNormcui_json) >= 1):
        global JSON_RESULT
        JSON_RESULT.append(RxNormcui_json)
        #print (JSON_RESULT)
        
  
     
if __name__ == "__main__":
    
    app.run(debug=True)
