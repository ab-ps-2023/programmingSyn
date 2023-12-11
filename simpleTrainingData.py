import json
import csv

# Open the JSON file
with open('tables_structured.json', 'r') as file:
   data = json.load(file)

def getQuery(sqlTextNode):
   return sqlTextNode['query']

def getQuestion(sqlTextNode):
   return sqlTextNode['question']

def getdbID(sqlTextNode):
   return sqlTextNode['db_id']

def buildTrainingData(sqlTextInfo, count):
    question = getQuestion(sqlTextInfo)
    sql = getQuery(sqlTextInfo)

    trainingData = {}
    trainingData['id'] = count

    tempDict = {}
    tempDict['input'] = question
    tempDict['output'] = sql
    trainingData['translation'] = tempDict

    return trainingData









# with open('train_spider.json', 'r') as file:
# with open('easy_queries_new_updated.json', 'r') as file: 
with open('easy_dev.json', 'r') as file:
    sqlTextInfoData = json.load(file)

trainingDataList = []

for ind, sqlTextInfo in enumerate(sqlTextInfoData):
   # if ind not in difficultQueryListIndices:
   # if ind < 20:
   trainingDataList.append(buildTrainingData(sqlTextInfo, ind))


print('\n\n')
# print(trainingDataList)


csv_file_path = 'EVALUATE_SIMPLE_TRAIN_seq2seq_final_combined.csv'

# Writing to CSV file
with open(csv_file_path, 'w', newline='') as csv_file:
    fieldnames = ['id', 'translation']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write header
    writer.writeheader()

    # Write data
    for entry in trainingDataList:
        writer.writerow(entry)




