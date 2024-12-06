import pip

str_package = [ 
    "pandas", 
    "numpy",
    "sqlite3",
    "mysql-connector-python",
    "json5",
    "requests"
]

for package in str_package:
    pip.main(["install", package])