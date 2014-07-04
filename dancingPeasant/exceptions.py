#!/usr/bin/env python
###############################################################################
#                                                                             #
#    exceptions.py                                                            #
#                                                                             #
#    All the exceptions we'd like to raise.                                   #
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

class DP_Exception(BaseException): pass

#------------------------------------------------------------------------------
# FILE IO
class DP_FileException(DP_Exception): pass
class DP_FileAlreadyOpenException(DP_FileException): pass
class DP_FileNotOpenException(DP_FileException): pass
class DP_FileNotFoundException(DP_FileException): pass
class DP_FileError(DP_FileException): pass

#------------------------------------------------------------------------------
# STRUCTUAL
class DP_TableException(DP_Exception): pass
class DP_InvalidDataTypeException(DP_TableException): pass

#------------------------------------------------------------------------------
# HISTORY
class DP_HistoryException(DP_Exception): pass
class DP_InvalidHistoryTypeException(DP_HistoryException): pass

#------------------------------------------------------------------------------
# INTERFACE
class DP_InterfaceException(DP_Exception): pass
class DP_UnsetDBException(DP_InterfaceException): pass


###############################################################################
###############################################################################
###############################################################################
###############################################################################
