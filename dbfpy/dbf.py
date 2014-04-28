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

from .header import DbfHeader
from . import memo
from .record import DbfRecord
from . import utils


class Dbf(object):
    """DBF accessor.

    FIXME:
        docs and examples needed (dont' forget to tell
        about problems adding new fields on the fly)

    """

    __slots__ = ("name", "header", "stream", "memo", "_ignore_errors")

    INVALID_VALUE = utils.INVALID_VALUE

    ## initialization and creation helpers

    def __init__(self, file, read_only=False, new=False, ignore_errors=False,
                 memo_file=None):
        """Initialize instance.

        Arguments:
            file:
                Filename or file-like object.
            read_only:
                if ``f`` argument is a string file will
                be opend in read-only mode; in other cases
                this argument is ignored. This argument is ignored
                even if ``new`` argument is True.
            new:
                True if new data table must be created. Assume
                data table exists if this argument is False.
            ignore_errors:
                if set, failing field value conversion will return
                ``INVALID_VALUE`` instead of raising conversion error.
            memo_file:
                optional path to the FPT (memo fields) file.
                Default is generated from the DBF file name.

        """
        if isinstance(file, str):
            # a filename
            self.name = file
            if new:
                # new table (table file must be
                # created or opened and truncated)
                self.stream = open(file, "w+b")
            else:
                # table file must exist
                self.stream = open(file, ("r+b", "rb")[bool(read_only)])
        else:
            # a stream
            self.name = getattr(file, "name", "")
            self.stream = file

        if new:
            # if this is a new table, header will be empty
            self.header = DbfHeader()
        else:
            # or instantiated using stream
            self.header = DbfHeader.from_stream(self.stream)

        self.ignore_errors = ignore_errors
        if memo_file:
            self.memo = memo.MemoFile(memo_file, readOnly=read_only, new=new)
        elif self.header.has_memo:
            self.memo = memo.MemoFile(memo.MemoFile.memo_file_name(self.name),
                                      readOnly=read_only, new=new)
        else:
            self.memo = None
        self.header.set_memo_file(self.memo)

    ## properties

    @property
    def closed(self):
        return self.stream.closed

    @property
    def record_count(self):
        return self.header.record_count

    @property
    def field_names(self):
        return [field.name for field in self.header.fields]

    @property
    def fields(self):
        return self.header.fields

    @property
    def ignore_errors(self):
        """Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """
        return self._ignore_errors

    @ignore_errors.setter
    def ignore_errors(self, value):
        """Update `ignore_errors` flag on the header object and self"""
        self.header.ignore_errors = self._ignore_errors = bool(value)

    ## protected methods

    def _fix_index(self, index):
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

    ## interface methods

    def close(self):
        self.flush()

        if self.stream.writable():
            # write SUB (ASCII 26) after last record
            self.stream.seek(
                self.header.header_length +
                self.header.record_count * self.header.record_length
            )
            self.stream.write(b"\x1A")

        self.stream.close()

    def flush(self):
        """Flush data to the associated stream."""
        self.header.flush(self.stream)
        self.stream.flush()
        # flush if memo is not None
        if hasattr(self.memo, 'flush'):
            self.memo.flush()

    def new_record(self):
        """Return new record, which belong to this table."""
        return DbfRecord(self.header)

    def write_record(self, record):
        """Write data to the dbf stream.

        If ``record.index`` is None, this record will be appended to the
        records of the DBF this records belongs to; or replaced otherwise.
        """
        if not self.stream.writable():
            return

        if record.index is None:
            record.index = self.header.record_count
            self.header.record_count += 1

        record.validate_index()
        self.stream.seek(record.position)
        self.stream.write(record.to_bytes())

    def append(self, record):
        """Append ``record`` to the database."""
        record.index = None
        self.write_record(record)

    def add_field(self, *defs):
        """Add field definitions.

        For more information see `header.DbfHeader.addField`.

        """
        if self.record_count > 0:
            raise TypeError("At least one record was added, "
                            "structure can't be changed")

        self.header.add_field(*defs)
        if self.header.has_memo:
            if not self.memo:
                self.memo = memo.MemoFile(
                    memo.MemoFile.memo_file_name(self.name), new=True)
            self.header.set_memo_file(self.memo)

    ## 'magic' methods (representation and sequence interface)

    def __str__(self):
        return "Dbf stream '%s'\n" % self.stream + str(self.header)

    def __len__(self):
        """Return number of records."""
        return self.record_count

    def __getitem__(self, index):
        """Return `DbfRecord` instance."""
        if isinstance(index, slice):
            return [self[i] for i in range(self.record_count)[index]]

        record = DbfRecord(
            self.header, index=self._fix_index(index)
        )
        record.from_stream(self.stream)
        return record

    def __setitem__(self, index, record):
        """Write `DbfRecord` instance to the stream."""
        record.index = self._fix_index(index)
        self.write_record(record)

    #def __del__(self):
        #    """Flush stream upon deletion of the object."""
        #    self.flush()

# vim: set et sw=4 sts=4 :
