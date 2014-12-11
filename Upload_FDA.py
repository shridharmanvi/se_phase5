
import csv
import sys

import pymongo;
from pymongo import MongoClient

c = MongoClient("mongodb://sphv:2s3WW6ut@50.84.62.186/sphv")
db = c.sphv
collection = db.FDA
collection1 = db.FDA1

l = []
o = []
p = []
q = []
r = []
s = []
t = []
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
                        
with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\OUTC13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        o.append(row)

with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\REAC13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        p.append(row)
    
with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\DEMO13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        s.append(row)

with open('E:\\Study\\Special topics in SE\\CS5320_Raw_Data\\FDA Adverse Event Reporting\\ascii\\RPSR13Q4.txt', 'r') as i:
    for row in csv.reader(i, delimiter='$'):
        t.append(row)
        # Convert lists into dictionaries with primary key of each table
'''
The data for all the abbreviation will be taken from the dictonary
'''

Age_Code = {
        'DEC': 'DECADE',
        'YR': 'YEAR',
        'MON': 'MONTH',
        'WK': 'WEEK',
        'DY': 'DAY',
        'HR': 'HOUR'
        }

Gender_Code = {
        'UNK': 'Unknown',
        'M': 'MALE',
        'F': 'FEMALE',
        'NS': 'Not Specified'
        }

Weight_Code = {"KG": "Kilograms", "LBS" : "Pounds", "GMS" : "Grams"}

Occp_Code = {"MD":"Physician", "PH":"Pharmacist", "OT":"Other health-professional", "LW": "Lawyer", "CN":"Consumer"}

'''
Insert the Demographic data into DB
'''
for row in s[1:]:
    DEMO = {}
    if row[0] != '':
        DEMO["PRIMARY_ID"] = int(row[0])  
    if row[1] != '':
        DEMO["CASE_ID"] = int(row[1])
    if row[2] != '':
        DEMO["CASE_VERSION"] = row[2]
    if row[3] != '':
        DEMO["I_F_code"] = row[3]  
    if row[4] != '':
        DEMO["EVENT_DATE"] = row[4]
    if row[5] != '':
        DEMO["MFR_DATE"] = row[5]
    if row[6] != '':
        DEMO["INIT_FDA_DATE"] = row[6]  
    if row[7] != '':
        DEMO["FDA_DATE"] = row[7]
    if row[8] != '':
        DEMO["REPT_CODE"] = row[8]
    if row[9] != '':
        DEMO["MFR_NUM"] = row[9]
    if row[10] != '':
        DEMO["MFR_SNDR"] = row[10]
    if row[11] != '':
        DEMO["AGE"] = row[11]
    if row[12] != '':
        DEMO["AGE_COD"] = Age_Code.get(row[12])
    if row[13] != '':
        DEMO["GNDR_COD"] = Gender_Code.get(row[13])
    if row[14] != '':
        DEMO["E_SUB"] = row[14]
    if row[15] != '':
        DEMO["WT"] = row[15]
    if row[16] != '':
        DEMO["WT_COD"] = Weight_Code.get(row[16])    
    if row[17] != '':
        DEMO["REPT_DT"] = row[17] 
    if row[18] != '':
        DEMO["TO_MFR"] = row[18]  
    if row[19] != '':
        DEMO["OCCP_COD"] = Occp_Code.get(row[19])  
    if row[20] != '':
        DEMO["REPORTER_COUNTRY"] = row[20] 
    if row[21] != '':
        DEMO["OCCR_COUNTRY"] = row[21]    
    collection.insert(DEMO)

Patient_Outcome = {
        'DE': 'DEATH',
        'LT': 'Life-Threatening',
        'HO': 'Hospitalization',
        'DS': 'Disability',
        'CA': 'Congenital Anomaly',
        'RI': 'Required Intervention',
        'OT': 'Other Serious'
        }
        
'''
Insert the Outcome data into DB
'''
for row in o[1:]:
    '''
    Get the data uploaded for respective PRIMARY_ID in the DB and update the same with data from Outcome file
    '''
    Primarydata = db['FDA'].find_one({"PRIMARY_ID":int(row[0])})
    
    if Primarydata != None:
        if row[2] != '':
            if "PATIENT_OUTCOME" in Primarydata:
                PatientOutcome1 = Primarydata["PATIENT_OUTCOME"]
                # If the PATIENT_OUTCOME is already present in the DB for the same PRIMARY_ID, Update it with a comma
                PatientOutcome1 = PatientOutcome1+","+Patient_Outcome.get(row[2])
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"PATIENT_OUTCOME": PatientOutcome1}})
            else:
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"PATIENT_OUTCOME": Patient_Outcome.get(row[2])}})
    else:
        #If no PRIMARY_ID found in the DB, insert a new document
        OUT = {}
        if row[0] != '':
            OUT["PRIMARY_ID"] = int(row[0])  
        if row[1] != '':
            OUT["CASE_ID"] = int(row[1])
        if row[2] != '':
            OUT["PATIENT_OUTCOME"] = Patient_Outcome.get(row[2])
        collection.insert(OUT)

'''
Insert the React data into DB
'''
for row in p[1:]:
    Primarydata = db['FDA'].find_one({"PRIMARY_ID":int(row[0])})
    if Primarydata != None:
        if row[2] != '':
            if "PREFERRED_TERM" in Primarydata:
                PreferredTerm = Primarydata["PREFERRED_TERM"]
                # If the PREFERRED_TERM is already present in the DB for the same PRIMARY_ID, Update it with a comma
                PreferredTerm1 = PreferredTerm+","+row[2]
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"PREFERRED_TERM": PreferredTerm1}})
            else:
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"PREFERRED_TERM": row[2]}})
    else:
        REACT = {}
        if row[0] != '':
            REACT["PRIMARY_ID"] = int(row[0])  
        if row[1] != '':
            REACT["CASE_ID"] = int(row[1])
        if row[2] != '':
            REACT["PREFERRED_TERM"] = row[2]
        collection.insert(REACT) 

Report_Code = {
        'FGN': 'Foreign',
        'SDY': 'Study',
        'LIT': 'Literature',
        'CSM': 'Consumer',
        'HP': 'Health Professional',
        'UF': 'User Facility',
        'CR': 'Company Representative',
        'DT':   'Distributor',
        'OTH': 'Other'
        }

for row in t[1:]:
    Primarydata = db['FDA'].find_one({"PRIMARY_ID":int(row[0])})
    if Primarydata != None:
        if row[2] != '':
            if "REPORT_CODE" in Primarydata:
                ReportCode = Primarydata["REPORT_CODE"]
                ReportCode1 = ReportCode+","+Report_Code.get(row[2])
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"REPORT_CODE": ReportCode1}})
            else:
                collection.update({"PRIMARY_ID": int(row[0])}, {"$set": {"REPORT_CODE": Report_Code.get(row[2])}})
    else:
        RPSR = {}
        if row[0] != '':
            RPSR["PRIMARY_ID"] = int(row[0])  
        if row[1] != '':
            RPSR["CASE_ID"] = int(row[1])
        if row[2] != '':
            RPSR["REPORT_CODE"] = Report_Code.get(row[2])
        collection.insert(RPSR) 

RoleCode = {"PS":"Primary Suspect Drug",
            "SS":"Secondary Suspect Drug",
            "C": "Concomitant",
            "I": "Interacting"}

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
        DRUG["ROLE_COD"] = RoleCode.get(row[3])  
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