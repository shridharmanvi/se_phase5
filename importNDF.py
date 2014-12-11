import xml.etree.ElementTree as ET
from pymongo import MongoClient

c = MongoClient("mongodb://sphv:2s3WW6ut@50.84.62.186/sphv")
db = c.sphv
coll = db.ndf

tree = ET.parse(
    'F:\CSE5320\CS5320_Raw_Data\\National Drug File - Reference Terminology(NDF-RT)\\NDFRT_Public_2014.07.07_TDE.xml')
root = tree.getroot()

kindq = {}
# get all the kind node values and store it in a dictionary so that we can get the kind value for conceptdef
for kinddef in root.findall('kindDef'):
    code = kinddef.find('code').text
    name = kinddef.find('name').text
    kindq[code] = name

role = {}  # get all the role node values and store it in a dictionary
for roledef in root.findall('roleDef'):
    code = roledef.find('code').text
    name = roledef.find('name').text
    role[code] = name

propertyval = {}  # get all the property node values and store it in a dictionary
for propertydef in root.findall('propertyDef'):
    code = propertydef.find('code').text
    name = propertydef.find('name').text
    propertyval[code] = name

association = {}  # get all the association node values and store it in a dictionary
for associationdef in root.findall('associationDef'):
    code = associationdef.find('code').text
    name = associationdef.find('name').text
    association[code] = name

qualifier = {}  # get all the qualifier node values and store it in a dictionary
for qualifierdef in root.findall('qualifierDef'):
    code = qualifierdef.find('code').text
    name = qualifierdef.find('name').text
    qualifier[code] = name

i = 0
concept = {}
print("Data from file NDFRT_Public_2014.07.07_TDE.xml\n")

for conceptdef in root.findall('conceptDef'):
    #if i < 50:
        i += 1
        concept = {}
        name = conceptdef.find('name').text
        code = conceptdef.find('code').text
        idval = int(conceptdef.find('id').text)

        concept['name'] = name
        concept['code'] = code
        concept['id'] = idval

        conceptkind = conceptdef.find('kind').text
        if conceptkind in kindq:  # Get the kind name from kind code in conceptDef
            kindname = kindq.get(conceptkind)
        else:
            kindname = conceptkind
        concept['kindname'] = kindname  # Store the values in a dictionary for further use


        #print(qualifier)

        #coll.insert(concept)
        #j = {}
        for prop in conceptdef.findall("properties/property"):
            f = prop.find('name').text
            concept[propertyval[f]] = prop.find('value').text

        #k = {}
        for rolev in conceptdef.findall("definingRoles/role"):
            g = rolev.find('name').text
            concept[role[g]] = rolev.find('value').text

        #l = {}
        for asso in conceptdef.findall("associations/association"):
            p = asso.find('name').text
            concept[association[p]] = asso.find('value').text

        #n = {}
        for quali in conceptdef.findall("properties/property/qualifiers/qualifier"):
            d = quali.find('name').text
            concept[qualifier[d]] = quali.find('value').text

        #r = {}
        for asqu in conceptdef.findall("associations/association/qualifiers/qualifier"):
            s = asqu.find('name').text
            concept[qualifier[s]] = asqu.find('value').text

        #y = {}
        for defcon in conceptdef.findall("definingConcepts"):
            x = defcon.findtext('concept')
            if x != None:
                concept['concept'] = defcon.findtext('concept')
                #x = defcon.find('concept').text



        #print(concept)
        coll.insert(concept)
print('Insert done!!,',i,'rows inserted')