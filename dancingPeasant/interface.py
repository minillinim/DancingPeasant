#!/usr/bin/env python
###############################################################################
#                                                                             #
#    interface.py                                                             #
#                                                                             #
#    Container for maapping SQL-based commands to object-based interfaces     #
#                                                                             #
#     NO SQL TO BE SEEN ABOVE THIS LEVEL                                      #
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
__version__ = "0.1.0"
__maintainer__ = "Michael Imelfort"
__email__ = "mike@mikeimelfort.com"
__status__ = "Dev"

###############################################################################
###############################################################################
###############################################################################
###############################################################################

# system imports
import sqlite3 as lite

# local imports
from dancingPeasant.exceptions import *
from dancingPeasant.baseFile import BaseFile

###############################################################################
###############################################################################
###############################################################################
###############################################################################

class Interface():
    """Class for implementing an interface to a SQL database"""
    def __init__(self, dbFileName, verbosity=0):
        """self.db is set to None
         The class that inherits this MUST set the db to their particular type
        """
        self.dbFileName = dbFileName
        self.connected = False
        self.db = None

#------------------------------------------------------------------------------
# HOUSEKEEPING

    def connect(self, createDB=False):
        """connect to the BaseFile
        if createDB is True the DB will be created if it doesn't exist

        returns True if the DB was created
        """
        created = False
        if self.db is None:
            raise DP_UnsetDBException("Can not connect to DB until self.db is set to the inheriting Class")
        try:
            self.db.openFile(self.dbFileName)
        except DP_FileNotFoundException:
            # the DB could not be found
            if createDB:
                # we will make it
                self.db.createNewFile(self.dbFileName)
                created = True
            else:
                # pass the error on
                raise
        # we now have a connection to an open TrackM DB
        self.connected = True
        return created

    def disconnect(self):
        """close the baseFile"""
        if self.connected:
            self.db.closeFile()

#------------------------------------------------------------------------------
# SQL WRAPPERS

    def insert(self,
               table,       # into this table
               columns,     # these columns
               values,      # list of tuples of values
               debug=False
               ):
        """Speedy import of bulk data into SQL

        table: is a single string. EX: 'bob'
        cols: is an ordered list of column names in the table ['col1', 'col2', ... ]is a list of columns to insert into
        vals is a list of tuples of values to insert

        Note: the use of single quotes on the values. This is important.

        the ordering of cols and vals should make sense
        """
        insert_str = "INSERT INTO %s (%s) VALUES (%s)" % (table, ", ".join(columns), ", ".join(["?" for i in columns]))
        if debug:
            print insert_str
            print values
        cur = self.db.getCursor()
        cur.executemany(insert_str, values)
        self.db.commit()

    def updateSingle(self,
               table,           # into this table
               columns,         # these columns
               values,          # list of tuples of values
               condition=None,  # where to insert
               debug=False
               ):
        """Update a single value in the DB

        table: is a single string. EX: 'bob'
        cols: is an ordered list of column names in the table ['col1', 'col2', ... ]is a list of columns to insert into
        vals is a list of tuples of values to insert

        the ordering of cols and vals should make sense

        condition: is a string which states the SQL condition. i.e. the part that comes after the where:

            "type='big' or color='red'"

        Note: the use of single quotes on the values. This is important.
        """
        tmp = []
        for i in range(len(columns)):
            tmp.append("%s=%s" % (columns[i], values[i]))
        insert_str = "UPDATE %s SET %s" % (table, ", ".join(tmp))
        if condition is not None:
            insert_str += " WHERE %s" % (condition)
        if debug:
            print insert_str
        cur = self.db.getCursor()
        cur.execute(insert_str)
        self.db.commit()

    def select(self,
               table,             # from this table
               columns,           # select these columns
               condition=None,    # where these conditions are met
               order=None         # order = ('col', ['ASC', 'DESC' or None])
               ):
        """ wrap select statements comming in from the outside world

        columns: is an ordered list of column names in the table ['col1', 'col2', ... ] or ['*'] for all
        table: is a single string. EX: 'bob'
        condition: is a string which states the SQL condition. i.e. the part that comes after the where:

            "type='big' or color='red'"

        Note: the use of single quotes on the values. This is important.

        order: is a tuple which dictates the ordering of the results. EX: ('col', 'ASC')

        This function is a generator of rows.

        """
        select_str = "SELECT %s FROM %s" % (", ".join(columns), table)
        if condition is not None:
            select_str += " WHERE %s" % condition
        if order is not None:
            select_str += " ORDER BY %s %s" % (order[0], order[1])

        #print select_str

        cur = self.db.getCursor()
        cur.execute(select_str)
        return cur.fetchall()

###############################################################################
###############################################################################
###############################################################################
###############################################################################
