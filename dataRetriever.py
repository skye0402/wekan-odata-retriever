import time
import configparser
import requests
import json
import sqlite3
from requests.auth import HTTPBasicAuth

# Helper function for K8S deployment
def endless_loop(msg):
    print(msg + " Entering endless loop. Check and redo deployment?")
    while True:
        pass

def getToken(url):
    callUrl = url + "users/login"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"}
    body = { "username": "gunter", "password": "pdapda0402"}
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
def copyAsIs(cF, card, nE):
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
                                        if cFields["value"] == None:
                                            nE[cusFieldSet["name"]] = "" #Empty field
                                        else:
                                            if cusValue["_id"] == cFields["value"]: nE[cusFieldSet["name"]] = cusValue["name"]
                                elif cusFieldSet["type"] == "checkbox":
                                    if cusFieldSet["settings"] == {}: nE[cusFieldSet["name"]] = ""
                                    else: nE[cusFieldSet["name"]] = "X"
    return nE

def copyListName(cF, card, nE, lists):
    for list in lists:
        if list["_id"] == card[cF]:
            nE["List"] = list["title"]
            return nE

def copySwimLaneName(cF, card, nE, swimlanes):
    for swimlane in swimlanes:
        if swimlane["_id"] == card[cF]:
            nE["Swimlane"] = swimlane["title"]
            return nE

def copyUserName(cF, card, nE, users):
    for user in users:
        if user["_id"] == card[cF]:
            nE["Creator"] = user["username"]
            return nE

# Mapping function
def copyCheck(cF, card, nE, users, lists, swimlanes, cFAllData):
    if cF == '_id': return copyAsIs(cF, card, nE)
    elif cF == 'title': return copyAsIs(cF, card, nE)
    elif cF == 'customFields': return copyCustomFields(cF, card, nE, cFAllData)
    elif cF == 'listId': return copyListName(cF, card, nE, lists)
    elif cF == 'swimlaneId': return copySwimLaneName(cF, card, nE, swimlanes)
    elif cF == 'type': return copyAsIs(cF, card, nE)
    elif cF == 'archived': return copyAsIs(cF, card, nE)
    elif cF == 'createdAt': return copyAsIs(cF, card, nE)
    elif cF == 'modifiedAt': return copyAsIs(cF, card, nE)
    elif cF == 'dateLastActivity': return copyAsIs(cF, card, nE)
    #elif cF == 'description': return copyAsIs(cF, card, nE)
    elif cF == 'requestedBy': return copyAsIs(cF, card, nE)
    elif cF == 'userId': return copyUserName(cF, card, nE, users)

    else:
        return nE # Pass back unchanged

def createExportList(eL, users, lists, swimlanes, cFAllData):
    nL = []
    for card in eL:
        nE = {}
        for cF in card:
            # Checks if field is part of export
            nE = copyCheck(cF, card, nE, users, lists, swimlanes, cFAllData)
        nL = nL + [nE]
    return nL

# To define the structure of the table
def createTableStructure(exportList):
    tabStr = ""
    for item in exportList:
        for field in item:
            dbfield = ''.join(field.split()).lower()
            tabStr = tabStr + dbfield + " TEXT, "
        break #we just want one entry
    return tabStr[:len(tabStr)-2]    

# Creates the sqlite table    
def createTable(con, tabStr):
    try:
        dbCur = con.cursor()
        dbCur.execute("DROP TABLE IF EXISTS CatalogService_Cards")
        print("Dropped old table.")
        tableStructure = "CREATE TABLE IF NOT EXISTS CatalogService_Cards (" + tabStr + ")"
        print("Created new table.")
        dbCur.execute(tableStructure)
    except Exception as e:
        print("Error occured: " + e + ".")

# Fill the database
def insertIntoDb(con,list):
    try:
        dbCur = con.cursor() 
        for item in list:
            dataSet = ""
            for field in item:
                dataSet = dataSet + "'" + str(item[field]) + "',"
            dataSet = dataSet[:len(dataSet)-1] 
            insertString = "INSERT INTO CatalogService_Cards VALUES (" + dataSet + ")"
            dbCur.execute(insertString) #'2006-01-05','BUY','RHAT',100,35.14)")
    except Exception as e:
        print("Error occured: " + e + ".")

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

    # Start retrieving data from WeKan
    loopCondition = True
    while loopCondition:
        print("Starting new data retrieval.")
        # Login to WeKan and get Token
        token = getToken(wekanUrl)
        # Get data 
        users = getWekanData(wekanUrl, token, "users")
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
        exportList = createExportList(masterList, users, lists, swimlanes, cFAllData)

        # Then store the list as SQLite DB to be picked up by CAP
        tableStructure = createTableStructure(exportList)
        conSql=None
        try:
            conSql = sqlite3.connect('./dbdata/wekan-items.db')
        except Exception as e:
            print("Error occured: " + e + ".")
        if conSql != None:
            createTable(conSql, tableStructure)
            insertIntoDb(conSql, exportList)
            conSql.commit()
            conSql.close()
        # Wait before the next call
        print("Completed data polling and wrote new database successfully. Sleeping now for " + str(refreshTimer) + " seconds.")
        time.sleep(refreshTimer)

if __name__ == "__main__":
    main()