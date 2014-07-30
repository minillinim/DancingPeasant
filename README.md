# DancingPeasant

## Overview

This is a living / functional experiment in moving bioinformatics workflows away from multiple CSV files and towards SQL (specifically SQLite).
DancingPeasant consists of two main classes: BaseFile and Interface

BaseFile is a base class that enables packaging of several csv-type files into one SQLite database. It supports creation, addition of tables, versioning and history.
Interface is a base class that provides access (currently insert / select / update) to the tables in BaseFile.

## Installation

DancingPeasant depends only on Python core libraries and it's on PIP. so just:

    $ pip install DancingPeasant

## Example usage

You should make your classes derive from BaseFile and Interface. Suppose you want to make a new db-based file format called a foofile (*.ff). First you need to extend the BaseClass:

Somewhere within your "foo" package you should make a file called "db.py" with the following contents:

    # Always version your files!
    __FOO_DB_VERSION__ = "1.0.0"

    from dancingPeasant.baseFile import BaseFile
    from dancingPeasant.exceptions import *

    class FooDB(BaseFile):
        def __init__(self, verbosity=0):
            BaseFile.__init__(self, verbosity)

        def createNewFile(self,
                          fileName,             # name of the new file
                          force=False,          # should we check to see if this is a wise move?
                          ):
            """Create a new Foo database file"""
            # make a basic file
            BaseFile.createNewFile(self,
                                   fileName,
                                   type="Foo_DB",
                                   version=__FOO_DB_VERSION__,
                                   force=force)

            # add Foo specific tables
            self._addTable("bars",                  # the name of the table
                           {
                            "length" : "INT",       # Specify column names and
                            "diameter" : "INT",     # standard SQL syntax for the type
                            "material" : "TEXT",    # As may as you like
                           },
                           force=True)

            # Keep adding tables
            ...


To read and write from this file you'll need to extend the Interface class.
Make a file called importer.py with the following contents:

    from dancingPeasant.exceptions import *
    from dancingPeasant.interface import Interface
    from dancingPeasant.interface import Condition
    from foo.db import FooDB                        # <--- NOTE: foo is the name of your package and FooDB is the name of your file class

class FooInterface(Interface):
    """Use this interface for importing data stored in csv
    files into the TrackM DB"""

    def __init__(self,
                 dbFileName,        # file name to connect to
                 verbosity=-1       # turn off all DP chatter
                 ):
        Interface.__init__(self, dbFileName, verbosity)
        self.db = FooDB(verbosity=verbosity)            # <-- This line is important!

    def addBars(self, bars):
        """Add bars to the database"""
        # connect to the database
        self.connect()

        # bars is an array of tuples. All ordered in the same way
        # EX:
        # [(10, 1, "iron"), (20, 15, "wax"), ... ]
        #
        self.insert("bars",
                    ["length",
                     "diameter",
                     "material"],
                     bars)

        # disconnect once done
        self.disconnect()

    def getBars(self, length):
        """Get bars longer tha a set length"""

        # connect to the database
        self.connect()

        # set the select condition
        C = Condition("length", ">", length)

        # access the database and get rows
        rows = self.select('bars', ["material", "diameter"], C)

        # disconnect once done
        self.disconnect()

        # do something with the results

This is just a starter. Hopefully I'll add some more examples here soon but I'm still busy writing it and trying to learn how to use it.
In the meantime check out the unit tests in dancngpeasant and the various interfaces and dbs used in TrackM and StoreM for more examples on how I use this class.

## Help

If you experience any problems using DancingPeasant, open an [issue](https://github.com/minillinim/DancingPeasant/issues) on GitHub and tell us about it.

## Licence

GPL3, with love.

Project home page, info on the source tree, documentation, issues and how to contribute, see http://github.com/minillinim/DancingPeasant

## Copyright

Copyright (c) 2014 Michael Imelfort. See LICENSE.txt for further details.
