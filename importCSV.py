import os
import re
import pandas
import threading
import numpy as np
import pyodbc as odb


def otherType(data, df, head):
    datePattern = r"\d{4}[-/]\d{2}[-/]\d{2}"
    data = str(data)
    if re.match(datePattern, data):
        if re.match(r".*([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", data):
            return "DATETIME"
        return "DATE"
    elif re.match("^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", data):
        return "TIMESTAMP"
    size = df[head].str.len().max()
    if size >= 8000:
        return "nvarchar(max)"
    return f"VARCHAR({int(size)})"


def typeGess(df) -> dict():
    typeMapping = {}
    for head, type in df.dtypes.items():
        # if head == "index" or head == "Index":
        #     print("change the colum name from Index to something else")
        #     exit()
        size = df[head].astype(str).str.len().max()
        dataType = None
        if type == pandas.Int64Dtype.type:
            if size > 7:
                dataType = f"VARCHAR({size})"
            else:
                dataType = "INT"
        elif type == pandas.Float64Dtype.type:
            if size > 7:
                dataType = f"VARCHAR({size})"
            else:
                dataType = "FLOAT"
        elif type == "bool":
            dataType = "BIT"
        else:
            dataType = otherType(df[head][1], df, head)
        typeMapping[head] = dataType

    return typeMapping

# creting the query for the create statement if the table name is not passed


def createQuery(mapper, table):
    size = len(mapper)
    createQuery = f"CREATE TABLE {table} ("
    for i, (column, type1) in enumerate(mapper.items()):
        if column == "id" and i == 0:
            createQuery += "id INTEGER IDENTITY(1,1) PRIMARY KEY, "
            continue
        elif i == 0:
            createQuery += "id INTEGER IDENTITY(1,1) PRIMARY KEY, "
        cleanColumn = column.replace("'", "").replace(
            "/", "").replace("\\", "").replace(" ", "_")
        createQuery += f"{cleanColumn} {type1}"
        if i != size-1:
            createQuery += " ,"
    createQuery += " );"
    return createQuery.replace("+", "_")


def insertData(df, tableName, mapper, cur=None):
    head = df.columns
    colums = ",".join(head)
    for index, row in df.iterrows():
        # print(str(index)+" DONE")
        values = ""
        for head in colums.split(','):
            if "INT" in mapper[head] or "FLOAT" in mapper[head]:
                if "nan" in str(row[head]):
                    values += "NULL ,"
                else:
                    values += f"{row[head]} ,"
            else:
                cleanV = str(row[head]).replace("'", "").replace(
                    "/", "").replace("\\", "")
                values += f"'{cleanV}' ,"
        v = f"INSERT INTO {tableName} ({colums.replace(' ','_').replace('/','').replace('+','_')}) VALUES ({values[:-1]}) ;"
        # print(v)
        cur.execute(v)
    print("done")


def task(df, tableN, mapper, conString):
    print("job started")
    con = odb.connect(conString)
    cursor = con.cursor()
    insertData(df, tableN, mapper, cursor)
    cursor.commit()
    cursor.close()
    print("job Done")


def main():
    # Change the value
    sep = ","
    csvFile = "TelewireAnalytics.csv"  # name of the csv file
    tableN = csvFile.split('.')[0]
    # replace this with your connection string
    conString = os.getenv("SQLCONN")
    threads = 1

    df = pandas.read_csv(csvFile, sep=sep)
    mapper = typeGess(df)
    # print(df)

    tableQuery = createQuery(mapper, tableN)
    # print(tableQuery)

    print(len(df))
    # well herer we create the connection
    try:
        con = odb.connect(conString)
        cursor = con.cursor()
        # print(tableQuery)
        cursor.execute(tableQuery)
        cursor.commit()
        cursor.close()
        parts = np.array_split(df, threads)
        job = []
        for part in parts:
            frame = pandas.DataFrame(
                part, columns=df.columns).reset_index(drop=True)
            job.append(threading.Thread(target=task, args=(
                frame, tableN, mapper, conString)))
        for j in job:
            j.start()
    except Exception as e:
        if "42000" in str(e):
            print(e)
            print("Change the name of the column from index to something else")
        elif "42S01" in str(e):
            print(
                "The table alredy exist and u tried to create it again")
        else:
            print("---------------------")
            print(type(e))
            print("\n\n\nJust check the connection string")


if __name__ == "__main__":
    main()
