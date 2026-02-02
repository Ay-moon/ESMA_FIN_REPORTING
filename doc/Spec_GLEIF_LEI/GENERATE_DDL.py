import re
from pathlib import Path

# CHANGE THIS to your local path when you run it on your machine:
header_file = Path(r"nouveau 2.txt")  # this is your LEI2 columns file

def normalize_col(name: str) -> str:
    name = name.strip().strip('"')
    name = name.replace('.', '_').replace('-', '_')
    name = re.sub(r'__+', '_', name)
    return name

def guess_sql_type(col: str) -> str:
    c = col.lower()

    # fixed LEI
    if c == "lei" or c.endswith("_lei") or c.endswith("associatedlei") or c.endswith("managinglou"):
        return "CHAR(20) NULL"

    # language tag
    if c.endswith("xmllang"):
        return "NVARCHAR(10) NULL"

    # dates / datetimes (dictionary uses dateTime for many)
    if "date" in c or c.endswith("datetime"):
        return "DATETIME2(0) NULL"

    # flags
    if "flag" in c:
        return "NVARCHAR(50) NULL"

    # most dictionary tokenized fields are <= 500
    return "NVARCHAR(500) NULL"

lines = header_file.read_text(encoding="utf-8").splitlines()
cols = [l for l in lines if l.strip().startswith('"')]

col_defs = []
for raw in cols:
    col = raw.strip().strip('"')
    sql_col = normalize_col(col)
    col_defs.append((sql_col, guess_sql_type(sql_col)))

print("CREATE TABLE STG.GLEIF_LEI2_CDF31 (")
print("    ScriptName           NVARCHAR(200) NOT NULL,")
print("    LaunchTimestamp      DATETIME2(0)  NOT NULL,")
print("    SourceFileName       NVARCHAR(260) NOT NULL,")
print("    SourceFileTimestamp  DATETIME2(0)  NOT NULL,")
print("    SourceUrl            NVARCHAR(1000) NULL,")
print("")
for name, typ in col_defs:
    print(f"    [{name}] {typ},")
print(");")
