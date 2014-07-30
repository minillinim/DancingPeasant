#!/usr/bin/env python
###############################################################################
#                                                                             #
#    test_baseFile.py                                                         #
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

"""
This code calls many functions in interface. Indeed it can't work without them. No unit testing is done for the
classes in that file. But passes here should indicate that they work correctly
"""

import unittest
import time
from dancingPeasant.baseFile import BaseFile
from dancingPeasant.interface import Interface
from dancingPeasant.interface import Condition

class BaseFileTests(unittest.TestCase):
    BF = BaseFile(verbosity=-1)

    def open(self):
        self.done()
        self.BF.openFile("test.dp")

    def done(self):
        if self.BF.isOpen():
            self.BF.closeFile()

    def testCreation(self):
        self.BF.createNewFile("test.dp", "testing_db", "1.0", force=True)
        self.done()

    def testFileType(self):
        self.open()
        self.assertTrue(self.BF.getFileType() == "testing_db")
        self.done()

    def testVersioning(self):
        self.open()
        self.assertTrue(self.BF.getVersion() == "1.0")
        self.BF.logVersion("1.1")
        self.assertTrue(self.BF.getVersion() == "1.1")
        self.done()

    def testHistory(self):
        self.open()
        self.BF.logMessage("test message")
        (message, message_time) = self.BF.getMessages()[-1]
        self.assertTrue(message == "test message")
        time.sleep(1)
        self.BF.logMessage("test message2")
        messages = self.BF.getMessages(fromTime=message_time+1)
        self.assertTrue(len(messages) == 1)
        (message, message_time) = messages[0]
        self.assertTrue(message == "test message2")
        self.done()

    def testAddTable(self):
        self.open()
        self.BF._addTable("testing",
                          {
                           "time" : "INT",
                           "thing" : "TEXT",
                           "size" : "INT"
                           },
                          force=True)
        self.done()

    def testInsert(self):
        self.open()
        IF = Interface("test.dp")
        IF.db = self.BF
        to_db = [(1,"cat",5),
                 (2,"dog",6),
                 (2,"fish",8),
                 (4, "goat",0),
                 (5,"fish",12)]
        IF.insert('testing', ['time', 'thing', 'size'], to_db)
        self.done()

    def testSelect(self):
        self.open()
        IF = Interface("test.dp")
        IF.db = self.BF
        C = Condition("thing", "=", "'fish'")
        rows = IF.select("testing", ['time'], condition=C)
        self.assertTrue(len(rows)==2)
        C = Condition("thing", "=", "'dog'")
        rows = IF.select("testing", ['time'], condition=C)
        self.assertTrue(len(rows)==1)
        self.done()

    def testSelectCondition(self):
        """
        DB looks like:
        1 cat  5
        2 dog  6
        3 fish 8
        4 goat 0
        5 fish 12
        """
        self.open()
        IF = Interface("test.dp")
        IF.db = self.BF
        C = Condition(Condition("thing", "="), "or", Condition("size", "<"))
        values = ("fish", 6)
        rows = IF.select("testing", ['*'], condition=C, values=values)
        self.assertTrue(len(rows)==4)
        self.done()

    def testUpdate(self):
        self.open()
        IF = Interface("test.dp")
        IF.db = self.BF
        to_db = [("frog", 18, 2),
                 ("yak", 20, 5)]

        C = Condition("time", "=")
        IF.update('testing', ['thing', 'size'], to_db, C)

        """
        DB looks like:
        1 cat  5
        2 frog 18
        2 frog 18
        4 goat 0
        5 yak  20
        """

        C = Condition("thing", "=", "'fish'")
        rows = IF.select("testing", ['time'], condition=C)
        self.assertTrue(len(rows)==0)
        C = Condition("size", ">=", "'18'")
        rows = IF.select("testing", ['time'], condition=C)
        self.assertTrue(len(rows)==3)

        # more complex updates (proxy as test for more complex selects
        s1 = Condition("size", ">=", "'18'")
        s2 = Condition("thing", "!=")
        C = Condition (s1, "and", s2)
        data = [(7, 'frog')]
        IF.update('testing', ['time'], data, C)
        C = Condition("thing", "=", "'yak'")
        rows = IF.select("testing", ['time'], condition=C)
        # should affect 1 row
        self.assertTrue(len(rows)==1)
        # should be the yak row which should now have time = 7
        self.assertTrue(rows[0][0]==7)

        data = [(7,8,"pig")]
        IF.update('testing', ['time', 'size', 'thing'], data, 1)
        C = Condition("time", "=", 7)
        rows = IF.select("testing", ['time'], condition=C)
        # should affect 1 row
        self.assertTrue(len(rows)==5)

        self.done()


def main():
    unittest.main()

if __name__ == '__main__':
    main()