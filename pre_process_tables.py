"""

Schema Information

Table Names
Column Names
Primary keys
Foreign keys


"""


from nltk.corpus import stopwords
import json
from fuzzywuzzy import fuzz
import csv
from divideQueryDifficulty import getDifficultQueryList
import spacy
import numpy as np
import re
import spacy
from pre_process_sql import parseSelect, parseFrom, parseWhere, parseGroupBy, parseOrderBy
import re


# Load a pre-trained English word embedding model
nlp = spacy.load("en_core_web_md")


# Open the JSON file
with open('tables_structured.json', 'r') as file:
   data = json.load(file)



def getTableIndexes(sqlTextNode):
   tList = []
   tableUnitsList = sqlTextNode['sql']['from']['table_units']
   for each in tableUnitsList:
      tList.append(each[1])
   return tList

def getdbID(sqlTextNode):
   return sqlTextNode['db_id']

def buildDataSetSchemaInfo(dataSetSchema):
   pass

def getQuestionTokens(sqlTextNode):
   return sqlTextNode['question_toks']

def getQueryTokens(sqlTextNode):
   return sqlTextNode['query_toks']

def getQuery(sqlTextNode):
   return sqlTextNode['query']


#########################################################################################

#This method takes tableName and datasetID and give out the info of columns/data types/pk/fk's information
def buildSemiStructuredSchema(tableNamesList, datasetId):
   #data has info of tables.json
   dataSetSchema = data[datasetId]
   datasettName = dataSetSchema['db_id']


   schemainfo = {}
   tableKey = ''
   
   for tableIndex, tableName in enumerate(dataSetSchema['table_names_original']):
      # print('INSIDE for tables')
      
      print(f'tableName : {tableName}')
      if tableName.lower() in tableNamesList:
         print('INSIDE if for tables')

         tableKey = str(datasettName)+'_'+str(tableName)

         schemainfo[tableKey] = 'The database name is ' + '"' + str(datasettName).replace("_", " ")+ '"' + '. This database has a table whose name is ' + '"' + str(tableName) + '"' + '. '

         # ADDING COLUMNS INFORMATION
         schemainfo[tableKey] += 'The column names of this table are '
         for eachColInd, eachColInfo in enumerate(dataSetSchema['column_names']):
            if tableIndex == eachColInfo[0]:
               schemainfo[tableKey] += '"' +str(eachColInfo[1]) + '"' + ' which is of type ' + dataSetSchema['column_types'][eachColInd] + ', '  
         schemainfo[tableKey] = schemainfo[tableKey][:-2]
         schemainfo[tableKey] += '. '


         # ADDING PRIMARY KEY INFORMATION
         schemainfo[tableKey] += 'The mandatory column of this table is '
         for eachPriInd in dataSetSchema['primary_keys']:
            if tableIndex == dataSetSchema['column_names'][eachPriInd][0]:
               schemainfo[tableKey] += '"' +str(dataSetSchema['column_names'][eachPriInd][1]) + '"' + '.'
               pk = str(dataSetSchema['column_names'][eachPriInd][1])
      
         # ADDING FOREIGN KEYS INFORMATION
         for eachForInd in dataSetSchema['foreign_keys']:

            tableInd1 = dataSetSchema['column_names'][eachForInd[0]][0]
            tableInd2 = dataSetSchema['column_names'][eachForInd[1]][0]


            if tableInd1 == tableIndex or tableInd2 == tableIndex:
               tableName1 = dataSetSchema['table_names'][tableInd1]
               tableName2 = dataSetSchema['table_names'][tableInd2]
               colName1 = dataSetSchema['column_names'][eachForInd[0]][1]
               colName2 = dataSetSchema['column_names'][eachForInd[1]][1]
               if colName1 == colName2:
                  schemainfo[tableKey] += ' The table '+ '"' +str(tableName1) + '"' + ' has a reference in the table ' + '"' + str(tableName2) + '"' + ' which are linked by the column ' + '"' + str(colName1) + '"' + '.'
               else:
                  schemainfo[tableKey] += ' The table '+ '"' +str(tableName1) + '"' + ' has a reference in the table ' + '"' + str(tableName2) + '"' + ' which are linked by the columns ' + '"' + str(colName1) + '"' + ' and ' + '"' + str(colName2) + '"' +'.'

   schemaInfoValuesConcat = ''
   for key in schemainfo.keys():
      schemaInfoValuesConcat += schemainfo[key]

   return schemaInfoValuesConcat

#########################################################################################

def getTableIndex(extractedTableName, databaseId):
   dataSetSchema = data[databaseId]
   for tableIndex, tableName in enumerate(dataSetSchema['table_names_original']):
      # print(f"tableName = {tableName} ; extractedTableName = {extractedTableName}")
      if tableName.lower() == extractedTableName.lower():
         return tableIndex


def getPrimaryKey(sqlTextInfo, extractedTableName, databaseId):
   dataSetSchema = data[databaseId]
   pk = 'id'
   # Extracting PRIMARY KEY information
   for eachPriInd in dataSetSchema['primary_keys']:
      if getTableIndex(extractedTableName, databaseId) == dataSetSchema['column_names'][eachPriInd][0]:
         pk = str(dataSetSchema['column_names'][eachPriInd][1])
   print(f"TableIndex : {getTableIndex(extractedTableName, databaseId)}")
   print(f"PRIMARY KEY : {pk}")
   print('\n')
   return pk


def buildSemiStructuredSQL(sqlTextInfo, sql, databaseId):
   print('INSIDE buildSemiStructuredSQL')
   #Extracting table info for Primary Key
   tableName = parseFrom(sql)
   tableNamePattern = re.compile(r'<TABLE>(.*?)</TABLE>', re.DOTALL)
   match = tableNamePattern.findall(tableName)
   if match:
      extractedTableName = match[0].strip()
   
   print(f"tableName : {tableName} , extractedTableName : {extractedTableName}")
   print(f"databaseId : {databaseId}")
   pk = getPrimaryKey(sqlTextInfo,extractedTableName, databaseId)

   sql = sql.replace(';','')
   semiStructuredSQL = ''
   semiStructuredSQL += parseSelect(sql,pk)
   semiStructuredSQL += parseFrom(sql)
   semiStructuredSQL += parseWhere(sql)
   semiStructuredSQL += parseGroupBy(sql)
   semiStructuredSQL += parseOrderBy(sql)
   return semiStructuredSQL


#########################################################################################

def findSimilarity(questionToken, schemaInfoToken):
   questionToken = questionToken.lower()
   schemaInfoToken = schemaInfoToken.lower()

   similarityScore = fuzz.ratio(questionToken, schemaInfoToken)

   # vec1 = nlp(questionToken).vector
   # vec2 = nlp(schemaInfoToken).vector
   # dot_product = np.dot(vec1, vec2)
   # magnitude_product = np.linalg.norm(vec1) * np.linalg.norm(vec2)
   # similarityScore = dot_product / magnitude_product

   if similarityScore > 70:
      return 'Match'
   elif similarityScore > 60:
      return 'Partial Match'
   else:
      return 'No Match'


def removeStopWordsList(questionTokensList):
   nlp = spacy.load("en_core_web_sm")

   #Constructing a Map
   questionTokensDict = {}
   for ind, each in enumerate(questionTokensList):
      questionTokensDict[ind] = each

   # Update dictionary values with '@' for stop words
   for key, value in questionTokensDict.items():
      doc = nlp(value)
      updatedValue = " ".join(["@"+token.text if token.is_stop else token.text for token in doc])
      questionTokensDict[key] = updatedValue

   return questionTokensDict


def buildSemiStructuredQuestion(questionTokensList, tableNamesList, databaseId, semiStructuredSQL):
   # print(questionTokensList)
   # print(tableNamesList)
   columnsNamesList = []
   dataSetSchema = data[databaseId]

   ## ADDING COL NAMES AND TABLE NAMES FROM QUERY
   # # building columns list from semistructuredSQL
   # pattern = r'<COLUMN>(.*?)</COLUMN>'
   # colMatches = [match.strip().lower().replace('_',' ') for match in re.findall(pattern, semiStructuredSQL)]

   for tableIndex, tableName in enumerate(tableNamesList):
      for eachColInd, eachColInfo in enumerate(dataSetSchema['column_names']):
         if getTableIndex(tableName, databaseId) == eachColInfo[0]:
            columnsNamesList.append(str(eachColInfo[1]))

   # columnsNamesList = columnsNamesList + colMatches

   # # building tables list from semistructuredSQL
   # pattern = r'<TABLE>(.*?)</TABLE>'
   # tableMatches = [match.strip().lower().replace('_',' ') for match in re.findall(pattern, semiStructuredSQL)]
   # tableNamesList = tableNamesList + tableMatches

   questionTokensDict = removeStopWordsList(questionTokensList)
   print(f"Question Tokens List : {questionTokensDict}")
   print(f"Column Names List : {columnsNamesList}")
   print(f"Table Names List : {tableNamesList}")

   #Finding TABLE information
   for qTokKey, qTokVal  in questionTokensDict.items():
      if '@' not in qTokVal:
         for tableName in tableNamesList:
            res = findSimilarity(qTokVal, tableName)
            if res == 'Match' or res == 'Partial Match':
               questionTokensDict[qTokKey] = '<TABLE> ' +  qTokVal + ' </TABLE>'  

   #Finding COLUMN information
   for qTokKey, qTokVal  in questionTokensDict.items():
      if '@' not in qTokVal and '<TABLE>' not in qTokVal:
         for colName in columnsNamesList:
            res = findSimilarity(qTokVal, colName)
            if res == 'Match' or res == 'Partial Match':
               questionTokensDict[qTokKey] = '<COLUMN> ' +  qTokVal + ' </COLUMN>'

   
          
   
   #Finding Numerical VALUE information
   for qTokKey, qTokVal  in questionTokensDict.items():
      if '@' not in qTokVal and qTokVal.isdigit():
         questionTokensDict[qTokKey] = '<VALUE> ' + qTokVal + ' </VALUE>'

   temp = ''
   for i in range(len(questionTokensDict)):
      temp += questionTokensDict[i].replace('@','') + ' '

   print(temp)
   return temp

#########################################################################################

def getTablesInfo(sqlTextInfo):
   queryTokensList = getQueryTokens(sqlTextInfo)
   tableNames = []
   for ind, each in enumerate(queryTokensList):
      if each == 'FROM':
         tableNames.append(queryTokensList[ind+1].lower())
   return tableNames



def buildTrainingData(sqlTextInfo, count):

   sqlQuery = getQuery(sqlTextInfo) 
   databaseId = getdbID(sqlTextInfo)
   questionTokensList = getQuestionTokens(sqlTextInfo)
   tableNamesList = getTablesInfo(sqlTextInfo)

   print(f'SQL Query : {sqlQuery}')
   semiStructuredSQL = buildSemiStructuredSQL(sqlTextInfo, sqlQuery, databaseId)
   semiStructuredQuestion = buildSemiStructuredQuestion(questionTokensList, tableNamesList, databaseId, semiStructuredSQL)
   semiStructuredSchema = buildSemiStructuredSchema(tableNamesList, databaseId)

   if semiStructuredSchema == '':
      print('SCHEMA IS NOT CAPTURED')

   trainingData = {}
   trainingData['id'] = count
   
   tempDict = {}
   tempDict['input'] = '<QUESTION> ' + semiStructuredQuestion + ' </QUESTION> ' + '<SCHEMA> ' + semiStructuredSchema + ' </SCHEMA>'
   tempDict['output'] = '<QUERY> ' +  semiStructuredSQL + ' </QUERY>'

   trainingData['translation'] = tempDict

   return trainingData

# expected.append(remove_tags(target_text,['<COLUMN>', '</COLUMN>','<QUESTION>', '</QUESTION>', '</SCHEMA>','<SCHEMA>','</QUERY>','<QUERY>','</TABLE>','<TABLE>','<VALUE>','</VALUE>']))
# tags = ['<COLUMN>', '</COLUMN>','<QUESTION>', '</QUESTION>', '</SCHEMA>','<SCHEMA>','</QUERY>','<QUERY>','</TABLE>','<TABLE>','<VALUE>','</VALUE>','<AGGREGATE>','</AGGREGATE>',
# '< COLUMN >', '</ COLUMN >','< QUESTION >', '</ QUESTION >', '</ SCHEMA >','< SCHEMA >','</ QUERY >','< QUERY >','</ TABLE >','< TABLE >','< VALUE >','</ VALUE >','< AGGREGATE >','</ AGGREGATE >']

# Open the JSON file
# with open('train_spider.json', 'r') as file:
with open('easy_dev.json', 'r') as file:
    sqlTextInfoData = json.load(file)

# difficultQueryListIndices = getDifficultQueryList(sqlTextInfoData)


trainingDataList = []

for ind, sqlTextInfo in enumerate(sqlTextInfoData):
   # if ind not in difficultQueryListIndices:

   trainingDataList.append(buildTrainingData(sqlTextInfo, ind))


print('\n\n')
# print(trainingDataList)


# csv_file_path = 'train_data_seq2seq_final_combined.csv'
csv_file_path = 'EVALUATE_SCHEMA_seq2seq_final_combined.csv'

# Writing to CSV file
with open(csv_file_path, 'w', newline='') as csv_file:
    fieldnames = ['id', 'translation']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write header
    writer.writeheader()

    # Write data
    for entry in trainingDataList:
        writer.writerow(entry)







# [{'input': '<question>  /question> <schema> The database name is "department management". This database has a table whose name is "head". The column names of this table are "head id" which is of type number, "name" which is of type text, "born state" which is of type text, "age" which is of type number. The mandatory column of this table is "head id". The table "management" has a reference in the table "head" which are linked by the column "head id". </schema>', 'output': '<query> SELECT <AGGREGATE> count </AGGREGATE>  <PRIMARY KEY> head id </PRIMARY KEY>  FROM  <TABLE> head </TABLE> WHERE <COLUMN> age </COLUMN> greater than <VALUE> 56 </VALUE> </query>'}]