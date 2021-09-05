import time
import configparser
import requests
import json
import os
import sqlite3
from requests.auth import HTTPBasicAuth

# Helper function for K8S deployment
def endless_loop(msg):
    print(msg + " Entering endless loop. Check and redo deployment?")
    while True:
        pass

def getToken(url, wUsername, wPassword):
    callUrl = url + "users/login"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"}
    body = { "username": wUsername, "password": wPassword}
    response = requests.post(callUrl, headers = headers, data = body ) #TODO
    data = json.loads(response.text)
    print("Login to WeKan with response status " + str(response.status_code) + ".")
    return data["token"]

def getWekanData(url, token, api, log = True):
    callUrl = url + "api/" + api
    headers = { "Content-Type": "application/json", "Accept": "application/json", "Authorization": "" }
    headers["Authorization"] = "Bearer " + token
    response = requests.get(callUrl, headers = headers)
    data = json.loads(response.text)
    if log:
        print("Retrieved data object " + api + " with response status " + str(response.status_code) + ".")
    return data

# Copy 1:1
def copyAsIs(cF, card, nE, isTimeStamp = False):
    if isTimeStamp:
        nE[cF] = str(card[cF])[:-5] + "Z" # Analytics cloud is limited to understand the standard timestamp (removing ms)
    else:
        nE[cF] = card[cF]
    return nE

# Translate custom fields into flat list
def copyCustomFields(cF, card, nE, cFAllData):
    for cFields in card[cF]:
        for cField in cFields:
            if cField == "_id":
                for cusFieldSet in cFAllData:
                    for cusField in cusFieldSet:
                        if cusField == "_id":
                            if cusFieldSet[cusField] == cFields[cField]: #Matches customer field in card
                                if cusFieldSet["type"] == "dropdown":
                                    for cusValue in cusFieldSet["settings"]["dropdownItems"]:
                                        try: # Sometimes the field is not None but completely missing
                                            if cFields["value"] == None:
                                                nE[cusFieldSet["name"]] = "" #Empty field
                                            else:
                                                if cusValue["_id"] == cFields["value"]: nE[cusFieldSet["name"]] = cusValue["name"]
                                        except Exception:
                                            nE[cusFieldSet["name"]] = "" #Empty field
                                elif cusFieldSet["type"] == "checkbox":
                                    if cusFieldSet["settings"] == {}: nE[cusFieldSet["name"]] = ""
                                    else: nE[cusFieldSet["name"]] = "X"
    return nE

def copyListName(cF, card, nE, lists, fieldMap):
    for list in lists:
        if list["_id"] == card[cF]:
            listName = list["title"]
            listType = "undefined"
            for f in fieldMap:
                if fieldMap[f][0] == list["title"]: 
                    listName = fieldMap[f][2] 
                    listType = fieldMap[f][1]
            nE["List"] = listName
            nE["ListType"] = listType
            return nE

def copySwimLaneName(cF, card, nE, swimlanes, fieldMap):
    for swimlane in swimlanes:
        if swimlane["_id"] == card[cF]:
            swimlaneName = swimlane["title"]
            swimlaneType = "undefined"
            for f in fieldMap:
                if fieldMap[f][0] == swimlane["title"]: 
                    swimlaneName = fieldMap[f][2] 
                    swimlaneType = fieldMap[f][1]
            nE["Swimlane"] = swimlaneName
            nE["SwimlaneType"] = swimlaneType
            return nE

def copyUserName(cF, card, nE, userAllData):
    for user in userAllData:
        if user["_id"] == card[cF]:
            try:
                nE["Creator"] = user["profile"]["fullname"]
            except Exception as e:
                nE["Creator"] = user["username"] #Backup in case full name is not maintained
            return nE

def copyAssignees(cF, card, nE, userAllData):
    strAssignees = ""
    lenAss = len(card["assignees"])
    for i in range(lenAss):
        for user in userAllData:
            if user["_id"] == card["assignees"][i]:
                try:
                    ass = user["profile"]["fullname"]
                except Exception as e:
                    ass = user["username"] #Backup in case full name is not maintained
                strAssignees = strAssignees + ass
                if i < (lenAss -1): strAssignees = strAssignees + "/"
    nE["Assignees"] = strAssignees
    return nE

# Mapping function
def copyCheck(cF, card, nE, userAllData, lists, swimlanes, cFAllData, fieldMap):
    if cF == '_id': return copyAsIs(cF, card, nE)
    elif cF == 'title': return copyAsIs(cF, card, nE)
    elif cF == 'customFields': return copyCustomFields(cF, card, nE, cFAllData)
    elif cF == 'listId': return copyListName(cF, card, nE, lists, fieldMap)
    elif cF == 'swimlaneId': return copySwimLaneName(cF, card, nE, swimlanes, fieldMap)
    elif cF == 'type': return copyAsIs(cF, card, nE)
    elif cF == 'archived': return copyAsIs(cF, card, nE)
    elif cF == 'createdAt': return copyAsIs(cF, card, nE, True)
    elif cF == 'modifiedAt': return copyAsIs(cF, card, nE, True)
    elif cF == 'dateLastActivity': return copyAsIs(cF, card, nE, True)
    #elif cF == 'description': return copyAsIs(cF, card, nE)
    elif cF == 'requestedBy': return copyAsIs(cF, card, nE)
    elif cF == 'assignees': return copyAssignees(cF, card, nE, userAllData)
    elif cF == 'userId': return copyUserName(cF, card, nE, userAllData)
    #elif cF == 'dueAt': return copyAsIs(cF, card, nE, True) #needs some more thinking as due-date is only on some cards
    else:
        return nE # Pass back unchanged

def createExportList(eL, userAllData, lists, swimlanes, cFAllData, fieldMap):
    nL = []
    for card in eL:
        nE = {}
        for cF in card:
            # Checks if field is part of export
            nE = copyCheck(cF, card, nE, userAllData, lists, swimlanes, cFAllData, fieldMap)
        # Finally add a counter field = 1 to have a measure
        nE['counter'] = 1
        # Append dataset
        nL = nL + [nE]
    return nL

# To define the structure of the table
def createTableStructure(exportList):
    tabStr = ""
    dataModel = "namespace wekan.export;\n\nentity Cards {\n"
    fieldSequence = []
    for item in exportList:
        for field in item:
            dbfield = ''.join(field.split()).lower()
            if dbfield == "counter":
                tabStr = tabStr + dbfield + " INTEGER, "
                dataModel = dataModel + dbfield + " : Integer;\n"
            else:
                tabStr = tabStr + dbfield + " TEXT, "
                dataModel = dataModel + dbfield + " : String;\n"
            fieldSequence.append(field) # This defines the field sequence for all cards
        break #we just want one entry
    dataModel = dataModel + "}"
    return tabStr[:len(tabStr)-2], dataModel, fieldSequence 

# Creates the sqlite table    
def createTable(con, tabStr):
    try:
        dbCur = con.cursor()
        dbCur.execute("DROP TABLE IF EXISTS CatalogService_Cards")
        print("Dropped old table.")
        tableStructure = "CREATE TABLE IF NOT EXISTS CatalogService_Cards (" + tabStr + ")"
        dbCur.execute(tableStructure)
        print("Created new table.")
    except Exception as e:
        print(e)

# Fill the database
def insertIntoDb(con,list, fieldSequence):
    try:
        dbCur = con.cursor() 
        for item in list: # Loop over dataset
            placeholder = ""
            dataset = []
            for field in fieldSequence: # To ensure a consistent sequence
                placeholder = placeholder + "?," 
                dataset.append(str(item[field]))
            placeholder = placeholder[:len(placeholder)-1] 
            dbCur.execute("INSERT INTO CatalogService_Cards VALUES (" + placeholder + ")", dataset)
    except Exception as e:
        print(e)

def main():
    # Get configuration
    config = configparser.ConfigParser(inline_comment_prefixes="#")
    config.read(['./config/settings.cfg'],encoding="utf8")
    if not config.has_section("server"):
        endless_loop("Config: Server section missing.")
    if not config.has_section("masterdata"):
        endless_loop("Config: masterdata section missing.")
    if not config.has_section("mapping"):
        endless_loop("Config: mapping section missing.")
    # -------------- Parameters ------------------>>>
    wekanUrl = config.get("server","wekanUrl")
    refreshTimer = config.getint("server","refreshTimer")
    boardId = config.get("masterdata","boardId")
    # Get mappings
    tempfMap = dict(config.items('mapping'))
    fMap = {}
    for fieldmap in tempfMap:
        fMapping = tempfMap[fieldmap].split(',')
        fMap[fieldmap] = fMapping
    # -------------- Parameters ------------------<<<

    # Get username and password for WeKan
    try:
        wUsername = os.environ['USERNAME']
        wPassword = os.environ['PASSWORD']
    except Exception:
        endless_loop("Could not retrieve username and/or password.") # Stop here

    # Start retrieving data from WeKan
    loopCondition = True
    while loopCondition:

        print("Starting new data retrieval.")
        # Login to WeKan and get Token
        token = getToken(wekanUrl, wUsername, wPassword)
        # Get list of users 
        users = getWekanData(wekanUrl, token, "users")
        # Get user detailed list
        userAllData = []
        for user in users:
            userDetail = getWekanData(wekanUrl, token, "users/" + user["_id"])
            userAllData = userAllData + [userDetail]
        # Get lists from board
        lists = getWekanData(wekanUrl, token, "boards/" + boardId + "/lists")
        # Get swimlanes from board
        swimlanes = getWekanData(wekanUrl, token, "boards/" + boardId + "/swimlanes")
        # Get all custom fields from board
        customFieldList = getWekanData(wekanUrl, token, "boards/" + boardId + "/custom-fields")
        # Get all custom field data
        cFAllData = []
        for cF in customFieldList:
            cFDetail = getWekanData(wekanUrl, token, "boards/" + boardId + "/custom-fields/" + cF["_id"], False)
            cFAllData = cFAllData + [cFDetail]
        # Get all cards as a list details
        masterList = []
        for list in lists:
            cardL = getWekanData(wekanUrl, token, "boards/" + boardId + "/lists/" + list["_id"] + "/cards", False)
            for card in cardL:
                cardData = getWekanData(wekanUrl, token, "boards/" + boardId + "/lists/" + list["_id"] + "/cards/" + card["_id"], False)
                masterList = masterList + [cardData]

        print("Retrieved all data. Creating export list.")
        # Let's create an enriched list for export
        exportList = createExportList(masterList, userAllData, lists, swimlanes, cFAllData, fMap)

        # Then store the list as SQLite DB to be picked up by CAP
        tableStructure, dataModel, fieldSequence = createTableStructure(exportList)
        conSql=None
        try:
            conSql = sqlite3.connect('./dbdata/wekan-items.db')
        except Exception as e:
            print(e)
        if conSql != None:
            createTable(conSql, tableStructure)
            insertIntoDb(conSql, exportList, fieldSequence)
            conSql.commit()
            conSql.close()
            # Create data-model.cds if it doesn't exist
            if not os.path.isfile("./dbdata/data-model.cds"):
                dataModelCds = open("./dbdata/data-model.cds", "w")
                writtenBytes = dataModelCds.write(dataModel)
                dataModelCds.close()
                print("data-model.cds: " + str(writtenBytes) + " bytes written to shared folder.")   
        # Wait before the next call
        print("Completed data polling and wrote new database successfully. Sleeping now for " + str(refreshTimer) + " seconds.")
        time.sleep(refreshTimer)

if __name__ == "__main__":
    main()