import csv
import sqlparse
import os
import sys
import re

def ReadMetaData(MYSQL,tables):
    f=open('./files/metadata.txt','r');
    MetaData=f.readlines();
    for Line in MetaData:
        Line=Line.strip()
        if Line == '<begin_table>':
            TableBegin = True
            continue;
        if TableBegin == True:
            TableName=Line
            tables.append(TableName.lower())
            MYSQL[TableName]=[]
            TableBegin = False
            continue
        if not Line == '<end_table>':
            MYSQL[TableName].append(Line.lower())
    f.close()

def TableReader(filename,filedata):
        with open('./files/' + filename) as f:
            reader=csv.reader(f)
            for row in reader:
                filedata.append(row);
        return filedata

def printHeader(columnNames,tableNames,dictionary):
    string = []
    dash = '-' * 40
    to_print = "OUTPUT : \n"

    for col in columnNames:
        for tab in tableNames:
            if col in dictionary[tab]:
                string.append(tab + '.' + col)
    print(dash)
    for i in string:
        print(i.upper(),end="\t")
    print('\n')
    print(dash)

def ParseQuery(query):
    select=[]
    tables=[]
    conditions=[]
    Distinct = False
    tokens = [str(x).strip() for x in sqlparse.parse(query)[0].tokens]
    tokens = list(filter(None,tokens))

    tokens=[x.lower() for x in tokens]

    if tokens[1] == 'distinct':
        Distinct = True
        tokens.remove('distinct')

    if ';' in tokens:
        tokens.remove(';')


    if len(tokens) < 4:
        sys.exit("Invalid query! FROM & SELECT parameters are mandatory")

    elif len(tokens) > 4:
        conds =[x.strip() for x in tokens[-1].split('where')]
        if len(conds) < 2:
            sys.exit('syntax ERROR')
        conditions = conds[1]

    select = [x.strip() for x in tokens[1].split(',')]
    tables = [x.strip() for x in tokens[3].split(',')]

    return select,tables,conditions,Distinct

def aggregate(func,colname,table,conditions,MYSQL):

    filedata=[]
    collist=[]

    if colname not in MYSQL[table]:
        sys.exit("Colname not found in table")

    if colname == '*':
        sys.exit("Error")

    tablename = table + '.' + 'csv'
    filedata = TableReader(tablename,filedata)

    if len(conditions) > 0:
        Con = conditions.split(' ')

    isTrue = False
    for data in filedata:
        if len(conditions) > 0:
            string = evaluate(Con,[table],MYSQL,data)
            try:
                if eval(string):
                    isTrue = True
                    collist.append(int(data[MYSQL[table].index(colname)]))
            except :
                sys.exit('Syntax Error')
                pass
        else :
            collist.append(int(data[MYSQL[table].index(colname)]))


    if len(collist) < 1:
        print('NILL')
        return

    to_print = 'NILL'

    if func == 'avg':
        to_print = sum(collist)/len(collist)

    elif func == 'max':
        to_print = max(collist)

    elif func == 'sum':
        to_print = sum(collist)

    elif func == 'min':
        to_print = min(collist)

    else :
        to_print =  "ERROR :" + "Unknown function : ", '"' + str(func) + '"'

    print(to_print)


def selectColumns(ColName,table,Distinct,MYSQL):
    fileData = []
    TableName = table[0] + '.csv'

    if  ColName[0] == '*' and len(ColName) == 1:
        ColName = MYSQL[table[0]]

    for i in ColName:
        if i in MYSQL[table[0]]:
            continue
        else:
            sys.exit("Error Column not found")
            break

    printHeader(ColName,table,MYSQL)
    TableReader(TableName,fileData)

    flag = 0
    temp = []
    temp.append(" ")

    if Distinct == True :
        DistinctData = []

        for data in fileData:
            for i in range(0,len(ColName)):
                DistinctData.append(data[MYSQL[table[0]].index(ColName[i])])
            if DistinctData not in temp:
                for i in range(0,len(ColName)):
                    print(DistinctData[i],end='\t\t')
                temp.append(DistinctData)
                DistinctData = []
                flag = 1
            if flag == 1:
                print("\n")
                print("-"*40)
                flag  = 0


    else :
        for data in fileData:
            for i in range(0,len(ColName)):
                print(data[MYSQL[table[0]].index(ColName[i])],end='\t\t')

            print("\n")
            print("-"*40)

def evaluate(a,table,MYSQL,data):
    string = ''
    for i in a:
        if i == '=':
            string += '=='
        elif i == 'and' or i == 'or':
            string += ' ' + i + ' '
        elif i in MYSQL[table[0]] :
            string += data[MYSQL[table[0]].index(i)]
        else:
            string += i

    return string

def join(ColNames,tables,Distinct,MYSQL):

        temp = []
        temp.append(" ")
        temp1 = []
        temp2 = []
        flag = 0


        TableReader(tables[1] + '.' + 'csv',temp1)
        TableReader(tables[0] + '.' + 'csv',temp2)

        fileData = []
        MYSQL['ExtTable']=[]

        for data1 in temp1:
            for data2 in temp2:
                fileData.append(data2 + data1)

        for i in MYSQL[tables[0]]:
            TableName = tables[0] + '.' + i
            MYSQL["ExtTable"].append(TableName)

        for i in MYSQL[tables[1]]:
            TableName = tables[1] + '.' + i
            MYSQL["ExtTable"].append(TableName)

        MYSQL["JOIN"] = MYSQL[tables[0]] + MYSQL[tables[1]]

        newtable = []
        newtable.insert(0,"ExtTable")

        if(ColNames[0] == '*' and len(ColNames) == 1):
            ColNames = MYSQL[newtable[0]]

        for i in range(len(ColNames)):
            if '.' in ColNames[i]:
                print(ColNames[i].upper(),end='\t')
            else :
                print(ColNames[i].upper(),end='\t\t')

        print("\n")
        print("-" * 80)

        if Distinct == True:
            Distinctdata = []
            for data in fileData:
                for col in ColNames:
                    if '.' in col:
                        try:
                            Distinctdata.append(data[MYSQL[newtable[0]].index(col)])
                        except:
                            sys.exit("COLUMN NOT FOUND")
                    else:
                        try:
                            Distinctdata.append(data[MYSQL["JOIN"].index(col)])
                        except:
                            sys.exit("Column NOT FOUND")

                if Distinctdata in temp:
                    Distinctdata = []
                    continue;

                if Distinctdata not in temp:
                    for i in range(0,len(ColNames)):
                        print(Distinctdata[i],end='\t\t')
                    temp.append(Distinctdata)
                    Distinctdata = []
                    flag = 1

                if flag == 1:
                    print("\n")
                    print("-"*80)
                    flag  = 0

        else :

            for data in fileData:
                for col in ColNames:
                    if '.' in col:
                        try:
                            print(data[MYSQL[newtable[0]].index(col)],end = '\t\t')
                        except:
                            sys.exit("COLUMN NOT FOUND")
                    else:
                        try:
                            print(data[MYSQL["JOIN"].index(col)],end ='\t\t')
                        except:
                            sys.exit("Column NOT FOUND")
                print("\n")
                print("-"*40)

def processWhere(cond,colname,table,Distinct,MYSQL):
    dash  = '-' * 40
    fileData = []
    temp = []
    temp.append(" ")
    TableName = table[0] + '.csv'
    flag =0

    if colname[0] == '*' and len(colname) == 1 :
        colname = MYSQL[table[0]]

    printHeader(colname,table,MYSQL)
    TableReader(TableName,fileData)
    Con = cond.split(' ')

    isTrue = False

    if Distinct == True:
        DistinctData=[]

        for data in fileData:
            string = evaluate(Con,table,MYSQL,data)

            for col in colname:
                try:
                    if eval(string):
                        isTrue = True
                        DistinctData.append(data[MYSQL[table[0]].index(col)])
                except :
                    sys.exit('Syntax Error')
                    pass

            if DistinctData in temp:
                DistinctData = []
                continue;

            if DistinctData not in temp:
                for i in range(len(DistinctData)):
                    print(DistinctData[i],end = '\t\t')
                flag = 1
                temp.append(DistinctData)
                DistinctData = []

            if flag == 1 and isTrue == True:
                print("\n")
                print("-"* 40)
                flag = 0
                isTrue = False

    else :
        for data in fileData:
            string = evaluate(Con,table,MYSQL,data)

            for col in colname:
                try:
                    if eval(string):
                        isTrue = True
                        print(data[MYSQL[table[0]].index(col)],end='\t\t')
                except :
                    sys.exit('Syntax Error')
                    pass

            if isTrue == True:
                isTrue = False
                print("\n")
                print("-"* 40)

def processWhereJoin(cond,ColNames,tables,Distinct,MYSQL):

        temp1 = []
        temp2 = []

        temp = []
        temp.append(" ")
        flag = 0

        TableReader(tables[1] + '.' + 'csv',temp1)
        TableReader(tables[0] + '.' + 'csv',temp2)

        MYSQL['ExtTable']=[]
        fileData = []

        for data1 in temp1:
            for data2 in temp2:
                fileData.append(data2 + data1)

        for i in MYSQL[tables[0]]:
            TableName = tables[0] + '.' + i
            MYSQL["ExtTable"].append(TableName)

        for i in MYSQL[tables[1]]:
            TableName = tables[1] + '.' + i
            MYSQL["ExtTable"].append(TableName)

        MYSQL["JOIN"] = MYSQL[tables[0]] + MYSQL[tables[1]]

        newtable = []
        newtable.insert(0,"ExtTable")

        if(ColNames[0] == '*' and len(ColNames) == 1):
            ColNames = MYSQL[newtable[0]]

        for i in range(len(ColNames)):
            if '.' in ColNames[i]:
                print(ColNames[i].upper(),end='\t')
            else :
                print(ColNames[i].upper(),end='\t\t')

        print("\n")
        print("-" * 80)

        Con = cond.split(' ')

        isTrue = False

        if Distinct == True:
            DistinctData = []
            for data in fileData:
                string = evaluate(Con,newtable,MYSQL,data)

                for col in ColNames:
                    try:
                        if eval(string):
                            isTrue = True
                            if '.' in col:
                                try:
                                    DistinctData.append(data[MYSQL[newtable[0]].index(col)])
                                except:
                                    sys.exit("COLUMN NOT FOUND")
                            else :
                                try:
                                    DistinctData.append(data[MYSQL["JOIN"].index(col)])
                                except:
                                    sys.exit("COLUMN NOT FOUND")
                    except :
                        print('Syntax Error')
                        pass

                if DistinctData in temp:
                    DistinctData = []
                    continue

                if DistinctData not in temp:
                    for i in range(len(DistinctData)):
                        print(DistinctData[i],end = '\t\t')
                    temp.append(DistinctData)
                    DistinctData = []
                    flag = 1

                if isTrue == True and flag == 1:
                    isTrue = False
                    print("\n")
                    print("-"* 80)


        else:
            for data in fileData:
                string = evaluate(Con,newtable,MYSQL,data)

                for col in ColNames:
                    try:
                        if eval(string):
                            isTrue = True
                            if '.' in col:
                                try:
                                    print(data[MYSQL[newtable[0]].index(col)],end = '\t\t')
                                except:
                                    sys.exit("COLUMN NOT FOUND")
                            else :
                                try:
                                    print(data[MYSQL["JOIN"].index(col)],end ='\t\t')
                                except:
                                    sys.exit("COLUMN NOT FOUND")
                    except :
                        sys.exit('Syntax Error')
                        pass

                if isTrue == True:
                    isTrue = False
                    print("\n")
                    print("-"* 80)

        del MYSQL['ExtTable']


def process_query(select,tables,conditions,Distinct,MYSQL):
    names = []
    names.append("")
    names.append("")

    for i in range(len(tables)):
        if tables[i] not in MYSQL.keys():
            sys.exit("Table not found")
    if len(select)==1:
        if '(' in select[0] and ')' in select[0]:
            names=select[0].split('(')
            names[0] = names[0].strip()
            names[1] = (names[1].split(')')[0]).strip()
            aggregate(names[0],names[1],tables[0],conditions,MYSQL)
            return
        elif '(' in select[0] or ')' in select[0]:
                sys.exit("Error")

    if len(conditions) >=1 and  len(tables) == 1:
        processWhere(conditions,select,tables,Distinct,MYSQL)
        return

    if len(conditions) >=1 and  len(tables) > 1:
        processWhereJoin(conditions,select,tables,Distinct,MYSQL)
        return

    if(len(tables) > 1):
        join(select,tables,Distinct,MYSQL)
        return

    selectColumns(select,tables,Distinct,MYSQL)

def main():
    MYSQL = {}
    tables = []
    ReadMetaData(MYSQL,tables);
    a,b,c,d=ParseQuery(str(sys.argv[1]))
    process_query(a,b,c,d,MYSQL)

if __name__ == "__main__":
    main()
