import csv
import os
import re
import pandas
import pyodbc as odb


def otherType(data):
    datePattern = r"\d{4}[-/]\d{2}[-/]\d{2}"
    print(data)
    if re.match(datePattern, data):
        if re.match(r".*([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", data):
            return "DATETIME"
        return "DATE"
    elif re.match("^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$", data):
        return "TIMESTAMP"
    return "VARCHAR"


def typeGess(df) -> dict():
    typeMapping = {}
    for head, type in df.dtypes.items():
        dataType = None
        if type == pandas.Int64Dtype.type:
            dataType = "INTEGER"
        elif type == pandas.Float64Dtype.type:
            dataType = "FLOAT"
        elif type == "bool":
            dataType = "BOOLEAN"
        else:
            print(type)
            dataType = otherType(df[head][1])
        typeMapping[head] = dataType

    return typeMapping

# creting the query for the create statement if the table name is not passed


def createQuery(mapper):
    size = len(mapper)
    createQuery = "CREATE TABLE IF NOT EXISTS employees ("
    for i, (column, type1) in enumerate(mapper.items()):
        createQuery += f"{column.replace(' ','_')} {type1}"
        if i != size-1:
            createQuery += " ,"
    createQuery += " );"
    return createQuery


def main():
    SEP = os.path.sep
    df = pandas.read_csv("quality.csv", sep=";")
    mapper = typeGess(df)
    tableQuery = createQuery(mapper)
    print(tableQuery)


if __name__ == "__main__":
    main()
