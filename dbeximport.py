#!/usr/bin/env python3

from argparse import ArgumentParser
import sqlite3
import csv
import os
import ast


NULLSTRING = '***NULL***'


class Sqlite3Db(object):

    def __init__(self, filename):
        self.tables = None
        self.fileName = filename
        self.connection = sqlite3.connect(self.fileName)

    def _isIgnorableTable(self, tname):
        if (tname.startswith('django') or
            tname.startswith('celery') or
            tname.startswith('auth') or
            tname.startswith('sqlite') or
            tname.endswith('user_permissions') or
            tname.endswith('user_groups')
            ):
            return True
        else:
            return False

    def extractTableNames(self):
        cur = self.connection.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        allTables = cur.fetchall()
        self.tables = []
        for table in allTables:
            tablename = table[0]
            if self._isIgnorableTable(tablename):
                continue
            self.tables.append(tablename)
        return len(self.tables)

    def getTableNames(self):
        return self.tables

    def _getTableColumnNames(self, tablename):
        cols = []
        cur = self.connection.cursor()
        cur.execute("PRAGMA table_info("+ tablename +");")
        allColNames = cur.fetchall()
        for col in allColNames:
            cols.append(col[1])
        return cols

    def _getTableData(self, tablename):
        cur = self.connection.cursor()
        cur.execute("SELECT * FROM "+ tablename)
        return cur.fetchall()

    def storeTableDataInCsv(self, tablename, csv_name):
        allData = []
        cols = self._getTableColumnNames(tablename)
        allData.append(cols)
        data = self._getTableData(tablename)
        allData.append(data)
        filepath = None
        with open(csv_name, 'w') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"')
            csvwriter.writerow(allData[0])
            for datarow in allData[1]:
                datarow = list(datarow)
                for idx, val in enumerate(datarow):
                    if val == None:
                        datarow[idx] = NULLSTRING
                csvwriter.writerow(datarow)
            filepath = os.path.abspath(csvfile.name)
        return filepath

    def writeDbRow(self, tablename, attribs, vals):
        cur = self.connection.cursor()
        attribs_str = ",".join(attribs)
        vals_str = "?," * len(vals)
        vals_str = vals_str.rstrip(',')
        for idx, val in enumerate(vals):
            if val == NULLSTRING:
                vals[idx] = None
            elif val.startswith("b'"):
                pickleval = ast.literal_eval(val)
                vals[idx] = pickleval
        cur.execute("INSERT INTO " + tablename + " (" + attribs_str + ") VALUES (" + vals_str + ")", vals)

    def commit_changes(self):
        self.connection.commit()


class CSVFiles(object):

    def __init__(self, name):
        self.csv_files = None
        if os.path.isdir(name):
            self.csv_files = []
            for fl in os.listdir(name):
                filepath = os.path.join(name, fl)
                if os.path.isfile(filepath) and fl.endswith('.csv'):
                    self.csv_files.append(filepath)
        elif os.path.isfile(name) and name.endswith('.csv'):
            self.csv_files = [name]

    def writeCsvToDb(self, csvfilename, dbobj):
        tablename = None
        try:
            slashidx = csvfilename.rindex('/')
            tablename = csvfilename[slashidx+1:]
        except:
            return tablename
        if tablename.endswith('.csv'):
            tablename = tablename[:-4]
        with open(csvfilename, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            headingrow = next(csvreader)
            for row in csvreader:
                dbobj.writeDbRow(tablename, headingrow, row)
            dbobj.commit_changes()
        return tablename


class MainExec(object):

    class ParsedArgs(object):
        fileName = None
        def __init__(self, fn, od, id):
            self.fileName = fn
            self.outputDir = './'
            self.inputDir = None
            self.importOp = False
            if od:
                self.outputDir = od
                if not self.outputDir.endswith('/'):
                    self.outputDir += '/'
            if id:
                self.inputDir = id
                self.importOp = True

    def buildArgParser(self):
        argParser = ArgumentParser(description='Export all Sqlite3 database tables to csv files.')
        argParser.add_argument('inputfile', nargs=1, help='Sqlite3 db file.')
        argParser.add_argument('-o', '--outputDir',
                               help='directory for csv files.')
        argParser.add_argument('-i', '--inputDir',
                               help='directory, or a single file, from which to import csv data.')
        parsed = argParser.parse_args()
        parsedArgs = MainExec.ParsedArgs(parsed.inputfile[0], parsed.outputDir, parsed.inputDir)
        if not os.path.isdir(parsedArgs.outputDir):
            os.makedirs(parsedArgs.outputDir)
        return parsedArgs


if __name__ == '__main__':
    mainex = MainExec()
    parsedargs = mainex.buildArgParser()
    inputfilename = parsedargs.fileName
    importOnly = parsedargs.importOp

    print("DB File   : " + inputfilename)

    sqldb = Sqlite3Db(inputfilename)
    print("Connected to db...")

    if importOnly:
        inputdir = parsedargs.inputDir
        print("Importing data into DB...")
        print("Input : " + inputdir)
        csvfiles = CSVFiles(inputdir)
        if csvfiles.csv_files:
            print("Number of csv files : " + str(len(csvfiles.csv_files)))
            for csvf in csvfiles.csv_files:
                tablename = csvfiles.writeCsvToDb(csvf, sqldb)
                print("Table written : " + tablename)
    else:
        outputdir = parsedargs.outputDir
        print("Extracting data from DB...")
        print("Output Dir: " + outputdir)
        num = sqldb.extractTableNames()
        print("Number of tables : " + str(num))
        tableNameList = sqldb.getTableNames()
        for table_name in tableNameList:
            print("Processing   : " + table_name)
            filename_written = sqldb.storeTableDataInCsv(table_name, outputdir + table_name + '.csv')
            print("File written : " + filename_written)
