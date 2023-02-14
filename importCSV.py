import os
import re
import pandas
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
        dataType = None
        if type == pandas.Int64Dtype.type:
            dataType = "INT"
        elif type == pandas.Float64Dtype.type:
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
    return createQuery


def insertData(df, tableName, mapper, cur=None):
    head = df.columns
    colums = ",".join(head)
    for index, row in df.iterrows():
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
        v = f"INSERT INTO {tableName} ({colums.replace(' ','_')}) VALUES ({values[:-1]}) ;"
        cur.execute(v)
    print("done")


def main():
    sep = ","
    csvFile = "shark1.csv"
    tableN = "shark"
    conString = os.getenv("SQLCONN")

    df = pandas.read_csv(csvFile, sep=sep)
    mapper = typeGess(df)
    tableQuery = createQuery(mapper, tableN)

    print(len(df))
    # well herer we create the connection
    # try:
    con = odb.connect(conString)
    cursor = con.cursor()
    cursor.execute(tableQuery)
    cursor.commit()
    insertData(df, tableN, mapper, cursor)
    cursor.commit()
    cursor.close()
    # except Exception as e:
    # print("\n\n\nJust check the connection string")
    # print(e)


if __name__ == "__main__":
    main()
