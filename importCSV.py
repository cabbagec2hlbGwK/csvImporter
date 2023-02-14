import csv
import os
import re
import pandas
import pyodbc as odb


def otherType(data):
    print(data)
    return "VARCHAR"


def typeGess(headers):
    print(headers)
    typeMapping = {}
    for head, type in headers:
        dataType = None
        if type == pandas.Int64Dtype.type:
            dataType = "INTEGER"
        elif type == pandas.Float64Dtype.type:
            dataType = "FLOAT"
        else:
            dataType = otherType(head)
        typeMapping[head] = dataType

    print(typeMapping)


def main():
    SEP = os.path.sep
    df = pandas.read_csv("quality.csv", sep=";")
    mapper = typeGess(df.dtypes.items())


if __name__ == "__main__":
    main()
