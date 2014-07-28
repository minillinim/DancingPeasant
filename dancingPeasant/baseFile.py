#!/usr/bin/env python
###############################################################################
#                                                                             #
#    baseFile.py                                                              #
#                                                                             #
#    Implement a collection of CSV files in SQLite land: basic file utils     #
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
__version__ = "0.1.1"
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
from dancingPeasant.exceptions import *

###############################################################################
###############################################################################
###############################################################################
###############################################################################

class BaseFile():
    """Class for implementing collection of files within a SQL database"""
    def __init__(self, verbosity=0):
        # this is where we will store all the information about this file
        self.meta = { "verbosity" : verbosity }  # how much chitter do we want?
        self._connection = None                   # connection is kept until close is called

        # list of all the valid types of history logs
        self.validHistoryTypes = ['version',    # log version / type information
                                  'message',    # basic messages
                                  'warning',    # warnings about possible inconsistencies
                                  'error'       # known inconsistencies
                                  ]

        # list of valid data types for sqllite3
        self.validDataTypes = ['int', 'real', 'text', 'blob', 'integer primary key autoincrement']

#------------------------------------------------------------------------------
# BASIC FILE IO

    def isOpen(self):
        """Quick check to see if the file is open"""
        if self._connection is not None:
            return True
        else:
            return False

    def openFile(self, fileName):
        """Open the file"""
        # sanity checks
        if self.isOpen():
            raise DP_FileAlreadyOpenException()
        if not os.path.isfile(fileName):
            raise DP_FileNotFoundException("File %s could not be found" % fileName)

        # now we can open the file
        try:
            self._connection =  lite.connect(fileName)
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        # time to set these variables
        self.meta["fileName"] = fileName
        self.meta["fileType"] = self.getFileType()
        self.meta["version"] = str(self.getVersion())

        self.chatter("%s file: %s (version: %s) opened successfully" % (self.meta["fileType"], fileName, self.meta["version"]), 0)

        # return the fileType to the calling function
        return self.meta["fileType"]

    def closeFile(self):
        """Close the file"""
        if not self.isOpen():
            raise DP_FileNotOpenException("Trying to close file that is not open")
        try:
            self._connection.close()
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        # reset these variables now
        del self.meta["fileName"]
        del self.meta["fileType"]
        del self.meta["version"]
        self._connection = None

    def createNewFile(self,
                      fileName,             # name of the new file
                      type,                 # what type of file is this? StoreMdb? TrackMdb? etc...
                      version,              # version of this file (is force versioning a good idea?)
                      force=False,          # should we check to see if this is a wise move?
                      ):
        """Create a new DP database file

        type is mandatory and refers to the file format being encoded
        version is mandatory and can be either an integer or a string
        """
        # do some sanity checks
        if not force:
            if self.isOpen():
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
            self._connection = lite.connect(fileName)
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

        self.meta["fileName"] = fileName
        self.meta["fileType"] = type
        self.meta["version"] = version

        # create the history table
        self._addTable("history",
                       {
                        "time" : "INT",             # timestamp of the event
                        "type" : "TEXT",            # type of history to log
                        "event" : "TEXT"            # log message
                       },
                       force=force)

        # add the creation time, type and version of this file
        self.logMessage("created")
        self.logVersion("%s" % str(type))
        time.sleep(1) # make sure that the version has a later date stamp
        self.logVersion("%s" % str(version))

#------------------------------------------------------------------------------
# TABLE MANIPULATION

    def getCursor(self):
        """return the cursor to this DB"""
        # sanity checks
        if self._connection is None:
            raise DP_FileNotOpenException()
        return self._connection.cursor()

    def commit(self):
        """commit changes to the DB"""
        if self._connection is None:
            raise DP_FileNotOpenException()
        self._connection.commit()

    def _addTable(self,
                 tableName,         # name of the new table
                 columns,           # columns to add
                 force=False):
        """add a new table to the DB

        tableName should be one unique *nonfancy* word
        columns should be a dict that looks something like:

            {"Id" : "INT", "Name" : "TEXT", "Price" : "INT"}
        """

        # sanity checks
        if self._connection is None:
            raise DP_FileNotOpenException()

        # check if the column descriptions are well formed
        col_str = []
        for col_name in columns:
            type = columns[col_name].lower()
            if type not in self.validDataTypes:
                raise DP_InvalidDataTypeException("Type: '%s' is not a valid SQLITE3 data type" % columns[col_name])
            col_str.append("%s %s" % (col_name, type))
        col_str = ", ".join(col_str)

        # check to see if the table exists
        if not force:
            try:
                cur = self._connection.cursor()
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
            cur = self._connection.cursor()
            cur.execute("DROP TABLE IF EXISTS %s" % (tableName))
            cur.execute("CREATE TABLE %s(%s)" % (tableName, col_str))
            self._connection.commit()

        except lite.Error, e:
            if self._connection:
                self._connection.rollback()
            raise DP_FileError("ERROR %s:" % e.args[0])

    def _dropTable(self,
                  tableName,            # table to drop
                  force=False):
        pass

#------------------------------------------------------------------------------
# HISTORY

    def _addHistory(self,
                    type,               # the type of event: 'message', 'version', 'warning' or 'error'
                    event,              # the message / event to add to the history
                    ):
        """Add a history event to the file

        Do not call this directly, use the wrappers below instead
        """
        if self._connection is None:
            raise DP_FileNotOpenException()
        try:
            if type not in self.validHistoryTypes:
                raise DP_InvalidHistoryTypeException()
            cur = self._connection.cursor()
            cur.execute("INSERT INTO history (time, type, event) VALUES ('%d', '%s', '%s')" % (int(time.time()), type, event))
            self._connection.commit()
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])

    def logMessage(self, message):
        """Messages are the basic logging unit.
        Feel free to use plenty of these"""
        self._addHistory('message', str(message))

    def logWarning(self, warning):
        """Warnings should be about possible inconsistencies in the
        structure of the file. not for problems in the workflow / procedure
        used to build, maintain or access the structure"""
        self._addHistory('warning', str(warning))

    def logError(self, error):
        """Errors should be about known inconsistencies in the
        structure of the file. not for problems in the workflow / procedure
        used to build, maintain or access the structure"""
        self._addHistory('error', str(error))

    def logVersion(self, version):
        """A version log is either a version number or a description of the
        filetype itself. By convention, the first version log is the filetype
        and the last version log is the current version"""
        self._addHistory('version', str(version))

    def _getHistory(self,
                    type,              # type of history to fetch
                    index=None,        # index of a particular record
                    fromTime=None      # get history at or after this point
                    ):
        """Return the values of a particular type of history log"""
        if type not in self.validHistoryTypes:
            raise DP_InvalidHistoryTypeException()

        if self._connection is None:
            raise DP_FileNotOpenException()
        try:
            cur = self._connection.cursor()
            if fromTime == None:
                cur.execute("SELECT event,time FROM history WHERE type='%s' ORDER BY time ASC" % type)
            else:
                cur.execute("SELECT event,time FROM history WHERE type='%s' and time>='%s' ORDER BY time ASC" % (type, str(fromTime)))
            rows = cur.fetchall()
            if index is not None:
                return rows[index]
            else:
                return rows
        except lite.Error, e:
            raise DP_FileError("ERROR %s:" % e.args[0])
        return []

    def getMessages(self, fromTime=None):
        """Get messages from history table"""
        return self._getHistory('message', fromTime=fromTime)

    def getWarnings(self, fromTime=None):
        """Get warnings from history table"""
        return self._getHistory('warning', fromTime=fromTime)

    def getErrors(self, fromTime=None):
        """Get errors from history table"""
        return self._getHistory('error', fromTime=fromTime)

    def getVersion(self):
        """simple wrapper used to get the current version of this file"""
        version_row = self._getHistory("version", index=-1)
        if len(version_row) > 0:
            return version_row[0]
        return None

    def getFileType(self):
        """simple wrapper used to get the type of this file"""
        version_row = self._getHistory("version", index=0)
        if len(version_row) > 0:
            return version_row[0]
        return "unset"

#------------------------------------------------------------------------------
# CHITTER
    def chatter(self,
                message,            # what to say
                verbosityLevel):    # when to say it
        """Handler for chatting with the user"""
        if self.meta["verbosity"] >= verbosityLevel:
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
