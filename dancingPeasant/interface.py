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
__version__ = "0.1.5"
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

class Condition():
    """Class for specifying conditions for select / update SQL statements"""
    def __init__(self,
                 e1,            # first entity in the condition
                 joiner,        # boolean joiner
                 e2='?',        # second entity
                 ):
        """To get type='big' use: Conditon("type", "=", "'big'")
        Note: the use of single quotes on the values. This is important.
        Nest condition instances with the appropriate joiners to get more
        complex conditions
        """

        self.data = [e1, joiner, e2]

    def __str__(self):
        return "(%s %s %s)" % tuple([str(i) for i in self.data])

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
               commit=True, # commit these changes to the DB?
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
        if commit:
            self.db.commit()

    def update(self,
               table,           # into this table
               columns,         # these columns
               values,          # list of tuples of values
               condition,       # the conditions for updating
               commit=True,     # commit these changes to the DB?
               debug=False
               ):
        """Update a single value in the DB

        table: is a single string. EX: 'bob'
        cols: is an ordered list of column names in the table ['col1', 'col2', ... ]is a list of columns to insert into
        vals is list of a tuples of values to insert
        condition: is an instance of the class Condition
        """
        insert_str = "UPDATE %s SET " % (table)
        insert_str += ", ".join(["%s=?" % (col) for col in columns])
        insert_str += " WHERE %s" % str(condition)
        if debug:
            print insert_str
        cur = self.db.getCursor()
        cur.executemany(insert_str, values)
        if commit:
            self.db.commit()

    def select(self,
               table,             # from this table
               columns,           # select these columns
               condition=None,    # where these conditions are met
               values=None,       # values to use in the condition
               order=None,        # order = ('col', ['ASC', 'DESC' or None])
               debug=False
               ):
        """ wrap select statements comming in from the outside world

        columns: is an ordered list of column names in the table ['col1', 'col2', ... ] or ['*'] for all
        table: is a single string. EX: 'bob'
        condition: is an instance of the class Condition
        values: is a tuple of values which can be used with the condition statement
        order: is a tuple which dictates the ordering of the results. EX: ('col', 'ASC')

        This function is a generator of rows.

        """
        select_str = "SELECT %s FROM %s" % (", ".join(columns), table)
        if condition is not None:
            select_str += " WHERE %s" % str(condition)
        if order is not None:
            select_str += " ORDER BY %s %s" % (order[0], order[1])

        if debug:
            print select_str

        cur = self.db.getCursor()
        if values is None:
            cur.execute(select_str)
        else:
            cur.execute(select_str, values)
        return cur.fetchall()

###############################################################################
###############################################################################
###############################################################################
###############################################################################
