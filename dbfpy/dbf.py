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
from io import IOBase

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

    __slots__ = ("name", "header", "stream", "memo", "close_stream", "_ignore_errors")

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

        # close self.stream when self.close() ? does not close
        # when file argument is a stream
        self.close_stream = False

        if isinstance(file, str):
            # close self.stream when file argument is a str
            self.close_stream = True

            # file is a filename
            self.name = file
            if new:
                # new table (table file must be
                # created or opened and truncated)
                self.stream = open(file, "w+b")
            else:
                # table file must exist
                self.stream = open(file, ("r+b", "rb")[bool(read_only)])
        elif isinstance(file, IOBase):
            # file is a stream
            self.name = getattr(file, "name", "")
            self.stream = file
        else:
            raise TypeError('Unsupported file type ({})'.format(type(file)))

        if new:
            self.header = DbfHeader()
        else:
            self.header = DbfHeader.parse(self.stream)

        # for IDE inspection
        self._ignore_errors = None

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

    ## interface methods

    def close(self):
        """Close the stream, write the end of record 0x1A and truncate"""
        self.flush()

        if self.stream.writable():
            # write SUB (ASCII 26) after last record
            self.stream.seek(
                self.header.header_length +
                self.header.record_count * self.header.record_length
            )
            self.stream.write(b"\x1A")
            self.stream.truncate()

        if self.close_stream:
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
            raise OSError('Stream is not writable')

        if record.index is None:
            # we must increase record count before set index,
            # because set index will raise error if out of range
            self.header.record_count += 1
            record.index = self.header.record_count - 1

        self.stream.seek(record.position)
        self.stream.write(record.to_bytes())

    def append(self, record):
        """Append ``record`` to the database."""
        record.index = None
        self.write_record(record)

    def add_field(self, *defs):
        """Add field definitions.

        For more information see `header.DbfHeader.add_field`.
        """
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
            self.header, index=index
        )
        record.read(self.stream)
        return record

    def __setitem__(self, index, record):
        """Write `DbfRecord` instance to the stream."""
        assert isinstance(index, int)
        assert isinstance(record, DbfRecord)

        record.index = index
        self.write_record(record)

    #def __del__(self):
        #    """Flush stream upon deletion of the object."""
        #    self.flush()

# vim: set et sw=4 sts=4 :
