

################################
# Assumptions:
#   1. sql is correct
#   2. only table name has alias
#   3. only one intersect/union/except
#
# val: number(float)/string(str)/sql(dict)
# col_unit: (agg_id, col_id, isDistinct(bool))
# val_unit: (unit_op, col_unit1, col_unit2)
# table_unit: (table_type, col_unit/sql)
# cond_unit: (not_op, op_id, val_unit, val1, val2)
# condition: [cond_unit1, 'and'/'or', cond_unit2, ...]
# sql {
#   'select': (isDistinct(bool), [(agg_id, val_unit), (agg_id, val_unit), ...])
#   'from': {'table_units': [table_unit1, table_unit2, ...], 'conds': condition}
#   'where': condition
#   'groupBy': [col_unit1, col_unit2, ...]
#   'orderBy': ('asc'/'desc', [val_unit1, val_unit2, ...])
#   'having': condition
#   'limit': None/limit value
#   'intersect': None/sql
#   'except': None/sql
#   'union': None/sql
# }
################################



CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')

WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}

COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')


# SELECT col1, col2, col3
# SELECT avg(col1), sum(col2), min(col3), max(col4)
# SELECT DISTINCT col1
# SELECT count(*)
# SELECT count(DISTINCT col)
# SELECT *
# SELECT COUNT(*)

import re



def checkDistinct(tok):
    if " " in tok:
        return True
    return False

def parseDistinct(tok):
    tok = tok.split()
    tok = tok[0].upper() + ' <COLUMN> ' + tok[1] +' </COLUMN> '
    return tok

def checkStar(tok):
    if "*" in tok:
        return True
    return False

def parseStar(pk):
    if not pk:
        pk = 'id'
    return ' <PRIMARY KEY> ' + pk +' </PRIMARY KEY> '
    
def buildQuery(value, type):
    if type != None:
        return '<AGGREGATE> '+ type + ' </AGGREGATE> '+ value
    else:
        return value

def addColumnTag(tok):
    return '<COLUMN> ' + tok + ' </COLUMN> '


def parseSelect(sql,pk):
    selectPart = sql.split('FROM')[0] #getting query part before FROM
    selectPart = selectPart.replace('SELECT','') #Removing SELECT keyword
    selectPartTokens = selectPart.split(',') #Splitting the part using comma
    # trimming white spaces
    for ind,each in enumerate(selectPartTokens):
        selectPartTokens[ind] = selectPartTokens[ind].strip()

    # print(selectPartTokens)

    temp = 'SELECT '

    for tok in selectPartTokens:
        matched = False

        for agg in AGG_OPS:
            if tok.startswith(agg):

                match = re.search(r'\((.*?)\)', tok)
                if match:
                    if checkDistinct(tok):
                        # temp += '<AGGREGATE> '+ agg + ' </AGGREGATE> '+ parseDistinct(match.group(1))
                        temp += buildQuery(parseDistinct(match.group(1)) , agg)

                    elif checkStar(tok):
                        # temp += '<AGGREGATE> '+ agg + ' </AGGREGATE> '+ parseStar(pk)
                        temp += buildQuery(parseStar(pk) , agg)

                    else:
                        # temp += '<AGGREGATE> '+ agg + ' </AGGREGATE> '+' <COLUMN> ' + match.group(1) + ' </COLUMN> '
                        temp += buildQuery(addColumnTag(match.group(1)) , agg)
                    
                    matched = True
        
        if not matched:
            if checkDistinct(tok):
                # temp += parseDistinct(tok)
                temp += buildQuery(parseDistinct(tok) , None)

            elif checkStar(tok):
                # temp += parseStar(pk)
                temp += buildQuery(parseStar(pk) , None)

            else: 
                temp += ' <COLUMN> ' + tok + ' </COLUMN> '
                matched = False
    # print(temp)
    return temp
                
###################################################################################################################


def parseFrom(sql):
    fromPart = ''
    if 'FROM' in sql:
        fromPart = sql.split('FROM')[1] #getting query part after FROM
    if 'from' in sql:
        fromPart = sql.split('from')[1] #getting query part after FROM
    return ' FROM ' + ' <TABLE> ' + fromPart.split()[0] + ' </TABLE> '


# sql = 'SELECT avg(name) , distinct born_state ,  age, count(DISTINCT place), count(*), count(col3) FROM head WHERE born_state > 20 ORDER BY age LIMIT 1'
# pk = 'Id'
# parseSelect(sql,pk)

###################################################################################################################

def parseGroupBy(sql):
    if ' GROUP BY ' in sql:
        groupByPart = sql.split('GROUP BY')[1] #getting query part after GROUP BY
        return 'GROUP BY ' + '<COLUMN> ' + groupByPart + ' </COLUMN>'
    return ''



###################################################################################################################

def parseOrderBy(sql):
    if ' ORDER BY ' in sql:
        orderByPart = sql.split('ORDER BY')[1].split() #getting query part after ORDER BY
        if len(orderByPart) > 1:
            return 'ORDER BY ' + '<COLUMN> ' + orderByPart[0] + ' </COLUMN> ' + orderByPart[1] +'ENDING'
        else:
            return 'ORDER BY ' + '<COLUMN> ' + orderByPart[0] + ' </COLUMN>'
    return ''



###################################################################################################################



WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
whereOpsDict = { 'not in' : 'NOT IN',
                'not like' : 'not like',
                'not exists' : 'not exists',
                'not between' : 'not between',
                'not' : 'not',
                'between' : 'between',
                '=' : 'equals',
                '>' : 'greater than',
                '<' : 'less than',
                '>=' : 'greater than or equals',
                '<=' : 'less than or equals',
                '!=' : 'not equals',
                'in' : 'in',
                'like' : 'like',
                'is' : 'is',
                'exists' : 'exists'}

COND_OPS = ('and', 'or')
# COL_NAME WHERE_OPS SQL/VALUE COND_OPS COL_NAME WHERE_OPS COND_OPS SQL/VALUE

def buildWhereNode(colName = None,whereOps = None,sqlOrValue = None,condOps = None, isNested=False):
    temp = ''
    temp += '<COLUMN> '+ colName +' </COLUMN> ' if colName!= None else ''
    # temp += ' <OP> ' + whereOpsDict[whereOps] +' </OP>' if whereOps!= None else ''
    temp +=  whereOpsDict[whereOps.lower()] if whereOps!= None else ''

    if isNested:
        temp += ' <QUERY> '+ sqlOrValue +' </QUERY>' if sqlOrValue!= None else ''
    else:
        temp += ' <VALUE> '+ sqlOrValue +' </VALUE>' if sqlOrValue!= None else ''

    temp += ' ' + condOps + ' ' if condOps!= None else ''
    return temp


def checkIfSQL(node):
    if node and 'SELECT' in node:
        return True
    return False

def parseNestedSQL(sql):
    pk = None

    if sql.startswith('(') and sql.endswith(')'):
        sql = sql[1:-1]

    print(f" Nested SQL : {sql}")

    temp = ''
    temp += parseSelect(sql,pk)
    temp += parseFrom(sql)
    # temp += parseGroupBy(sql) #In easy queries we don't get an other Group By
    # temp += parseOrderBy(sql) #In easy queries we don't get an other Order By
    # temp += parseWhere(sql) #In easy queries we don't get an other Where
    return temp



def processEachWhereNodeToken(whereNode):
    isNested = False

    colName,whereOps,sqlOrValue,condOps = None, None, None, None

    for op in whereOpsDict.keys():
        val = ' '+op.upper()+' '
        if val in whereNode:
            splitList = whereNode.split(val)
            colName = splitList[0].strip()
            whereOps = val.strip().strip()
            sqlOrValue = splitList[1].strip()
            break
    
    if checkIfSQL(sqlOrValue):
        print(f"sqlOrValue : {sqlOrValue}")
        sqlOrValue = parseNestedSQL(sqlOrValue)
        isNested = True
    

    return buildWhereNode(colName,whereOps,sqlOrValue,condOps,isNested)




def parseWhere(sql):
    if 'WHERE' in sql:
        wherePart = sql.split('WHERE')[1] #getting query part after WHERE
        
        wherePartTokens = wherePart.split() #Splitting the part using space

        # trimming white spaces
        for ind,each in enumerate(wherePartTokens):
            wherePartTokens[ind] = wherePartTokens[ind].strip()

        # print(wherePartTokens)

        temp = 'WHERE '
        
        if 'AND' in wherePartTokens or 'OR' in wherePartTokens:
            whereNodes = re.split(r'(\b(?:AND|OR)\b)', ''.join(wherePart))
        else:
            whereNodes = [''.join(wherePart)]

        # print(whereNodes)

        for each in whereNodes:
            temp += processEachWhereNodeToken(each)
        # print('\n\n')
        # print(temp)
        return temp

    return ''





        





# sql = 'SELECT COUNT(*) FROM weather WHERE mean_humidity  >  50 AND mean_visibility_miles  >  8 OR col > 50 AND col1 < 30 OR col > 21 AND col < 34'
# sql1 = 'SELECT catalog_level_name ,  catalog_level_number FROM Catalog_Structure WHERE catalog_level_number BETWEEN 5 AND 10'
# sql2 = 'SELECT * FROM CUSTOMER WHERE State  =  \"NY\"'
# sql3 = 'SELECT Name FROM team WHERE Team_id NOT IN (SELECT Team FROM match_season)'
# sql4 = "SELECT name FROM student WHERE id IN (SELECT id FROM takes WHERE semester  =  'Fall' AND YEAR  =  2003)"
# sql5 = 'SELECT YEAR ,  book_title ,  publisher FROM book_club ORDER BY YEAR ASC'


# import spacy

# # Load spaCy model
# nlp = spacy.load("en_core_web_sm")

# # Example sentence
# sentence = "running shoes are better than walks"

# # Process the sentence with spaCy
# doc = nlp(sentence)

# # Perform lemmatization
# lemmatized_words = [token.lemma_ for token in doc]

# print("Lemmatized Words:", lemmatized_words)