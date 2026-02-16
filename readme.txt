python -m venv .venv
.venv\scripts\activate
pip freeze requirements.txt

# Elenco driver nel sistema:
python -c "import pyodbc; drivers = pyodbc.drivers(); print(chr(10).join(drivers) if drivers else 'No ODBC drivers found')"
# Esempio:
# SQL Server
# ODBC Driver 17 for SQL Server
# Microsoft Access Driver (*.mdb, *.accdb)
# Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)
# Microsoft Access Text Driver (*.txt, *.csv)
# Microsoft Access dBASE Driver (*.dbf, *.ndx, *.mdx)
# SQL Server Native Client 11.0
