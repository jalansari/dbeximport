# Summary

Database export and import.

A simple tool to:
1) export a database's data into many CSV files (one per database table), and
2) import CSV files into a database.

i.e. a very simple tool to create very simple fixtures.


# Running Tool

## One time set-up of VirtualEnv:

* `python3 -m venv .venv`


## Working in virtualenv:

* `source .venv/bin/activate`
* No requirements yet, but when there are ... `pip install -r requirements.txt`
* Run commands.


## Running script:

* For help:  `./dbeximport.py -h`
* Example:  `./dbeximport.py ~/Downloads/db.sqlite3`


## Deactivate virtualenv when finished:

* `deactivate`
