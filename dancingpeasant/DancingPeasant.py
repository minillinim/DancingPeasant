#!/usr/bin/env python
###############################################################################
#                                                                             #
#    DancingPeasant.py                                                        #
#                                                                             #
#    Implement a collection of CSV files in SQLite land.                      #
#                                                                             #
#    Copyright (C) Michael Imelfort                                           #
#                                                                             #
###############################################################################
#                                                                             #
#    This program is free software: you can redistribute it and/or modify     #
#    it under the terms of the GNU General Public License as published by     #
#    the Free Software Foundation, either version 3 of the License, or        #
#    (at your option) any later version.                                      #
#                                                                             #
#    This program is distributed in the hope that it will be useful,          #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of           #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            #
#    GNU General Public License for more details.                             #
#                                                                             #
#    You should have received a copy of the GNU General Public License        #
#    along with this program. If not, see <http://www.gnu.org/licenses/>.     #
#                                                                             #
###############################################################################

__author__ = "Michael Imelfort"
__copyright__ = "Copyright 2014"
__credits__ = ["Michael Imelfort"]
__license__ = "GPLv3"
__version__ = "0.0.1"
__maintainer__ = "Michael Imelfort"
__email__ = "mike@mikeimelfort.com"
__status__ = "Dev"

###############################################################################
###############################################################################
###############################################################################
###############################################################################

# system imports
import os
import sqlite3 as lite
import sys
import time

# local imports
from dancingpeasant.DPExceptions import *

###############################################################################
###############################################################################
###############################################################################
###############################################################################

class BaseFile():
    """Class for implementing collection of files within a SQL database"""
    def __init__(self, verbosity=0):
        # this is where we will store all the information about this file
        self.meta = {"verbosity":verbosity}     # how much chitter do we want?
        self.version = -1                       # version unset until opened
        self.connection = None                  # connection is kept until close is called

#------------------------------------------------------------------------------
# BASIC FILE IO

    def openFile(self, fileName):
        """Open the file"""
        # sanity checks
        if self.connection is not None:
            raise DP_FileAlreadyOpenException()
        if not os.path.isfile(fileName):
            raise DP_FileNotFoundException("File %s could not be found" % fileName)

        # now we can open the file
        try:
            self.connection =  lite.connect(fileName)
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        # time to set these variables
        self.meta["fileName"] = fileName
        self.version = self.getVersion()

        self.chatter("File: %s (version: %s) opened successfully" % (fileName, self.version), 1)

    def closeFile(self):
        """Close the file"""
        if self.connection is None:
            raise DP_FileNotOpenException("Trying to close file that is not open")
        try:
            self.connection.close()
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        # reset these variables now
        del self.meta["fileName"]
        self.connection = None
        self.version = -1

    def createNewFile(self,
                      fileName,             # name of the new file
                      version,              # version of this file (is force versioning a good idea?)
                      force=False,          # should we check to see if this is a wise move?
                      verbose=False         # how much chitter do we want?
                      ):
        """Create a new DP database file

        version is mandatory and can be either an integer or a string
        """
        # do some sanity checks
        if not force:
            if self.connection is not None:
                raise DP_FileAlreadyOpenException("Trying to create a new file: %s when another file (%s) is already open", (fileName, self.meta["fileName"]))
            if os.path.isfile(fileName):
                # we should ask the user if they wish to overwrite this file
                if not self.promptOnOverwrite(fileName):
                    self.chatter("Create dbfile %s operation cancelled" % fileName, 1)
                    return
                # delete the file
                self.chatter("Deleting dbfile %s" % fileName, 1)
                os.remove(fileName)

        # now we can create the file
        try:
            # this command will create the file for us
            self.connection = lite.connect(fileName)
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])
        self.meta["fileName"] = fileName

        # create the history table
        self.addTable("history", "time INT, type TEXT, event TEXT", force=False)
        # add the creation time
        self.logMessage("file created")
        self.logVersion("%s" % str(version))

#------------------------------------------------------------------------------
# TABLE MANIPULATION

    def addTable(self,
                 tableName,         # name of the new table
                 columns,           # columns to add
                 force=False):
        """add a new table to the DB

        tableName should be one unique *nonfancy* word
        columns should be a string that looks something like:

            "Id INT, Name TEXT, Price INT"
        """

        # sanity checks
        if self.connection is None:
            raise DP_FileNotOpenException()

        # check to see if the table exists
        if not force:
            try:
                cur = self.connection.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % (tableName))
                rows = cur.fetchall()
                if len(rows) > 0:
                    if not self.promptOnOverwrite(tableName, "table"):
                        self.chatter("Add table %s operation cancelled" % tableName, 1)
                        return
            except lite.Error, e:
                raise DP_FileError("ERROR %s:" % e.args[0])

        # OK to add table
        try:
            cur = self.connection.cursor()
            cur.execute("DROP TABLE IF EXISTS %s" % (tableName))
            cur.execute("CREATE TABLE %s(%s)" % (tableName, columns))
            self.connection.commit()

        except lite.Error, e:
            if self.connection:
                self.connection.rollback()
            raise DP_FileError("ERROR %s:" % e.args[0])

    def dropTable(self,
                  tableName,            # table to drop
                  force=False):
        pass

#------------------------------------------------------------------------------
# HISTORY

    def _addHistory(self,
                    type,        # the type of event: 'message', 'version', 'warning' or 'error'
                    event):
        """Add a history event to the file

        Do not call this directly, use the wrappers below instead
        """
        if self.connection is None:
            raise DP_FileNotOpenException()
        try:
            cur = self.connection.cursor()
            cur.execute("INSERT INTO history (time, type, event) VALUES ('%d', '%s', '%s')" % (int(time.time()), type, event))
            self.connection.commit()
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

    def logMessage(self, message):
        self._addHistory('message', str(message))

    def logWarning(self, warning):
        self._addHistory('warning', str(warning))

    def logError(self, error):
        self._addHistory('error', str(error))

    def logVersion(self, version):
        self._addHistory('version', str(version))

    def getVersion(self):
        """simple wrapper used to get the current version of this file"""
        try:
            cur = self.connection.cursor()
            cur.execute("SELECT * FROM history WHERE type='version' ORDER BY time DESC")
            rows = cur.fetchall()
            return rows[0][2]

        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        return -1
#------------------------------------------------------------------------------
# CHITTER
    def chatter(self,
                message,            # what to say
                verbosityLevel):    # when to say it
        """Handler for chatting with the user"""
        if verbosityLevel >= self.meta["verbosity"]:
            print message

    def promptOnOverwrite(self, entity, entityType="File"):
        """Check that the user is OK with overwriting an entity"""
        input_not_ok = True
        minimal=False
        valid_responses = {'Y':True,'N':False}
        vrs = ",".join([x.lower() for x in valid_responses.keys()])
        while(input_not_ok):
            if(minimal):
                option = raw_input(" Overwrite? ("+vrs+") : ").upper()
            else:
                option = raw_input(" ****WARNING**** "+entityType+": '"+entity+"' exists.\n" \
                                   " If you continue it *WILL* be overwritten\n" \
                                   " Overwrite? ("+vrs+") : ").upper()
            if(option in valid_responses):
                print "****************************************************************"
                return valid_responses[option]
            else:
                print "ERROR: unrecognised choice '"+option+"'"
                minimal = True

###############################################################################
###############################################################################
###############################################################################
###############################################################################
