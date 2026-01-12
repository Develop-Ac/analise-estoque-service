import pyodbc
print("Available ODBC Drivers:")
for d in pyodbc.drivers():
    print(d)
