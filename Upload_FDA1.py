
import csv
import sys

import pymongo;
from pymongo import MongoClient

c = MongoClient("mongodb://sphv:2s3WW6ut@50.84.62.186/sphv")
db = c.sphv
collection1 = db.FDA1

l = []
q = []
r = []
# ---------------------------------------------------

# Import all the data files using csv reader

with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\INDI13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        l.append(row)

with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\DRUG13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        r.append(row)

with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\THER13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        q.append(row)
        # Convert lists into dictionaries with primary key of each table
'''
The data for all the abbreviation will be taken from the dictonary
'''

DrugNameSource = {"1":"Validated trade name used", "2":"Verbatim name used"}        

'''
As the DRUG_SEQUENCE is considered important in only three files DRUG,THERAPY and INDICATION
And, it is important to consider drug-seq with the primary id (As mentioned in the document)
SO inserting the data from these files into a new collection
'''

for row in r[1:]:
    DRUG = {}
    if row[0] != '':
        DRUG["PRIMARY_ID"] = int(row[0])  
    if row[1] != '':
        DRUG["CASE_ID"] = int(row[1])
    if row[2] != '':
        DRUG["DRUG_SEQ"] = row[2]
    if row[3] != '':
        DRUG["ROLE_COD"] = row[3]  
    if row[4] != '':
        DRUG["DRUGNAME"] = row[4]
    if row[5] != '':
        DRUG["VAL_VBM"] = DrugNameSource.get(row[5])
    if row[6] != '':
        DRUG["ROUTE"] = row[6]  
    if row[7] != '':
        DRUG["DOSE_VBM"] = row[7]
    if row[8] != '':
        DRUG["CUM_DOSE_CHR"] = row[8]
    if row[9] != '':
        DRUG["CUM_DOS_UNIT"] = row[9]
    if row[10] != '':
        DRUG["DECHAL"] = row[10]
    if row[11] != '':
        DRUG["RECHAL"] = row[11]
    if row[12] != '':
        DRUG["LOT_NUM"] = row[12]
    if row[13] != '':
        DRUG["EXP_DATE"] = row[13]
    if row[14] != '':
        DRUG["NDA_NUM"] = row[14]
    if row[15] != '':
        DRUG["DOSE_AMOUNT"] = row[15]
    if row[16] != '':
        DRUG["DOSE_UNIT"] = row[16]    
    if row[17] != '':
        DRUG["DOSE_FORM"] = row[17] 
    if row[18] != '':
        DRUG["DOSE_FREQ"] = row[18]   
    collection1.insert(DRUG)
    

for row in q[1:]:
     # Take DRUG_SEQ into consideration while updating the data from Therapy file
    Primarydata = db['FDA1'].find_one({"PRIMARY_ID":int(row[0]), "DRUG_SEQ" : row[2]})
    if Primarydata != None:
        if row[3] != '':
            if "START_DATE" in Primarydata:
                StDate = Primarydata["START_DATE"]
                if StDate != "":
                    StDate1 = StDate + "," + row[3]
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"START_DATE": StDate1}})
                else:
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"START_DATE": StDate}})
            else:
                collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"START_DATE": row[3]}})
        if row[4] != '':
            if "END_DATE" in Primarydata:
                EDate = Primarydata["END_DATE"]
                if EDate != "":
                    EDate1 = EDate + "," + row[4]
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"END_DATE": EDate1}})
                else:
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"END_DATE": EDate}})
            else:
                collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"END_DATE": row[4]}})
        if row[5] != '':
            if "DURATION" in Primarydata:
                DURATION = Primarydata["DURATION"]
                if DURATION != "":
                    DURATION1 = DURATION + "," + row[5]
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION": DURATION1}})
                else:
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION": DURATION}})
            else:
                collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION": row[5]}})
        if row[6] != '':
            if "DURATION_OF_THERAPY" in Primarydata:
                DurTher = Primarydata["DURATION_OF_THERAPY"]
                if DurTher != "":
                    DurTher1 = DurTher + "," + row[5]
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION_OF_THERAPY": DurTher1}})
                else:
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION_OF_THERAPY": DurTher}})
            else:
                collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"DURATION_OF_THERAPY": row[6]}})     
    else:
        THER = {}
        if row[0] != '':
            THER["PRIMARY_ID"] = int(row[0])  
        if row[1] != '':
            THER["CASE_ID"] = int(row[1])
        if row[2] != '':
            THER["DRUG_SEQ"] = row[2]
        if row[3] != '':
            THER["START_DATE"] = row[3]  
        if row[4] != '':
            THER["END_DATE"] = row[4]
        if row[5] != '':
            THER["DURATION"] = row[5]
        if row[5] != '':
            THER["DURATION_OF_THERAPY"] = row[6]
        collection1.insert(THER) 

for row in l[1:]:
    # Take DRUG_SEQ into consideration while updating the data from Indication file
    Primarydata = db['FDA1'].find_one({"PRIMARY_ID":int(row[0]), "DRUG_SEQ" : row[2]})
    if Primarydata != None:
        if row[3] != '':
            if "INDI_PREFERRED_TERM" in Primarydata:
                I_PT = Primarydata["INDI_PREFERRED_TERM"]
                if I_PT != "":
                    I_PT_1 = I_PT + "," + row[3]
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"INDI_PREFERRED_TERM": I_PT_1}})
                else:
                    collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"INDI_PREFERRED_TERM": I_PT}})
            else:
                collection1.update({"PRIMARY_ID": int(row[0])}, {"$set": {"INDI_PREFERRED_TERM": row[3]}})
    else:
        INDI = {}
        if row[0] != '':
            THER["PRIMARY_ID"] = int(row[0])  
        if row[1] != '':
            THER["CASE_ID"] = int(row[1])
        if row[2] != '':
            THER["DRUG_SEQ"] = row[2]
        if row[3] != '':
            THER["INDI_PREFERRED_TERM"] = row[3]
        collection1.insert(INDI) 