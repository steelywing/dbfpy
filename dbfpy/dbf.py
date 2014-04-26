#! /usr/bin/env python
"""DBF accessing helpers.

FIXME: more documentation needed

Examples:

    Create new table, setup structure, add records:

        dbf = Dbf(filename, new=True)
        dbf.addField(
            ("NAME", "C", 15),
            ("SURNAME", "C", 25),
            ("INITIALS", "C", 10),
            ("BIRTHDATE", "D"),
        )
        for (n, s, i, b) in (
            ("John", "Miller", "YC", (1980, 10, 11)),
            ("Andy", "Larkin", "", (1980, 4, 11)),
        ):
            rec = dbf.newRecord()
            rec["NAME"] = n
            rec["SURNAME"] = s
            rec["INITIALS"] = i
            rec["BIRTHDATE"] = b
            rec.store()
        dbf.close()

    Open existed dbf, read some data:

        dbf = Dbf(filename, True)
        for rec in dbf:
            for fldName in dbf.fieldNames:
                print '%s:\t %s (%s)' % (fldName, rec[fldName],
                    type(rec[fldName]))
            print
        dbf.close()

"""

__version__ = "$Revision: 1.9 $"[11:-2]
__date__ = "$Date: 2012/12/17 19:16:57 $"[7:-2]
__author__ = "Jeff Kunce <kuncej@mail.conservation.state.mo.us>"

__all__ = ["Dbf"]

from . import header
from . import memo
from . import record
from .utils import INVALID_VALUE

class Dbf(object):
    """DBF accessor.

    FIXME:
        docs and examples needed (dont' forget to tell
        about problems adding new fields on the fly)

    """

    __slots__ = ("name", "header", "stream", "memo", "_ignore_errors")

    HeaderClass = header.DbfHeader
    RecordClass = record.DbfRecord
    INVALID_VALUE = INVALID_VALUE

    ## initialization and creation helpers

    def __init__(self, f, readOnly=False, new=False, ignoreErrors=False,
                 memoFile=None):
        """Initialize instance.

        Arguments:
            f:
                Filename or file-like object.
            readOnly:
                if ``f`` argument is a string file will
                be opend in read-only mode; in other cases
                this argument is ignored. This argument is ignored
                even if ``new`` argument is True.
            new:
                True if new data table must be created. Assume
                data table exists if this argument is False.
            ignoreErrors:
                if set, failing field value conversion will return
                ``INVALID_VALUE`` instead of raising conversion error.
            memoFile:
                optional path to the FPT (memo fields) file.
                Default is generated from the DBF file name.

        """
        if isinstance(f, str):
            # a filename
            self.name = f
            if new:
                # new table (table file must be
                # created or opened and truncated)
                self.stream = open(f, "w+b")
            else:
                # table file must exist
                self.stream = open(f, ("r+b", "rb")[bool(readOnly)])
        else:
            # a stream
            self.name = getattr(f, "name", "")
            self.stream = f
        
        if new:
            # if this is a new table, header will be empty
            self.header = self.HeaderClass()
        else:
            # or instantiated using stream
            self.header = self.HeaderClass.fromStream(self.stream)
        
        self.ignoreErrors = ignoreErrors
        if memoFile:
            self.memo = memo.MemoFile(memoFile, readOnly=readOnly, new=new)
        elif self.header.hasMemoField:
            self.memo = memo.MemoFile(memo.MemoFile.memoFileName(self.name),
                readOnly=readOnly, new=new)
        else:
            self.memo = None
        self.header.setMemoFile(self.memo)

    ## properties

    @property
    def closed(self):
        return self.stream.closed
    
    @property
    def recordCount(self):
        return self.header.recordCount
    
    @property
    def fieldNames(self):
        return [field.name for field in self.header.fields]
    
    @property
    def fieldDefs(self):
        return self.header.fields
    
    @property
    def ignoreErrors(self):
        """Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """
        return self._ignore_errors
        
    @ignoreErrors.setter
    def ignoreErrors(self, value):
        """Update `ignoreErrors` flag on the header object and self"""
        self.header.ignoreErrors = self._ignore_errors = bool(value)

    ## protected methods

    def _fixIndex(self, index):
        """Return fixed index.

        This method fails if index isn't a numeric object
        (long or int). Or index isn't in a valid range
        (less or equal to the number of records in the db).

        If ``index`` is a negative number, it will be
        treated as a negative indexes for list objects.

        Return:
            Return value is numeric object maning valid index.

        """
        if not isinstance(index, int):
            raise TypeError("Index must be a numeric object")
        if index < 0:
            # index from the right side
            # fix it to the left-side index
            index += len(self) + 1
        if index >= len(self):
            raise IndexError("Record index out of range")
        return index

    ## iterface methods

    def close(self):
        self.stream.close()

    def flush(self):
        """Flush data to the associated stream."""
        self.header.setCurrentDate()
        self.header.write(self.stream)
        self.stream.flush()
        # flush if memo is not None
        if hasattr(self.memo, 'flush'):
            self.memo.flush()

    def newRecord(self):
        """Return new record, which belong to this table."""
        return self.RecordClass(self)

    def append(self, record):
        """Append ``record`` to the database."""
        record.index = self.header.recordCount
        record._write()
        self.header.recordCount += 1

    def addField(self, *defs):
        """Add field definitions.

        For more information see `header.DbfHeader.addField`.

        """
        if self.recordCount > 0:
            raise TypeError("At least one record was added, "
                "structure can't be changed")
        
        self.header.addField(*defs)
        if self.header.hasMemoField:
            if not self.memo:
                self.memo = memo.MemoFile(
                    memo.MemoFile.memoFileName(self.name), new=True)
            self.header.setMemoFile(self.memo)

    ## 'magic' methods (representation and sequence interface)

    def __repr__(self):
        return "Dbf stream '%s'\n" % self.stream + repr(self.header)

    def __len__(self):
        """Return number of records."""
        return self.recordCount

    def __getitem__(self, index):
        """Return `DbfRecord` instance."""
        if isinstance(index, slice):
            return [self[_recno] for _recno in range(self.recordCount)[index]]
        return self.RecordClass.fromStream(self, self._fixIndex(index))

    def __setitem__(self, index, record):
        """Write `DbfRecord` instance to the stream."""
        record.index = self._fixIndex(index)
        record._write()

    #def __del__(self):
    #    """Flush stream upon deletion of the object."""
    #    self.flush()

if (__name__ == '__main__'):
    pass

# vim: set et sw=4 sts=4 :
