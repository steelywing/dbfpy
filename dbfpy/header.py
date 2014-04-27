"""DBF header definition.

TODO:
  - handle encoding of the character fields
    (encoding information stored in the DBF header)

"""
import textwrap
from dbfpy import utils

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2010/12/14 11:07:45 $"[7:-2]

__all__ = ["DbfHeader"]

import io
import datetime
import struct

from . import fields
from .utils import get_gate


class DbfHeader(object):
    """Dbf header definition.

    For more information about dbf header format visit
    `http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_STRUCT`

    Examples:
        Create an empty dbf header and add some field definitions:
            dbfh = DbfHeader()
            dbfh.addField(("name", "C", 10))
            dbfh.addField(("date", "D"))
            dbfh.addField(DbfNumericFieldDef("price", 5, 2))
        Create a dbf header with field definitions:
            dbfh = DbfHeader([
                ("name", "C", 10),
                ("date", "D"),
                DbfNumericFieldDef("price", 5, 2),
            ])

    """

    __slots__ = (
        "signature", "fields", "last_update", "record_length", "record_count",
        "header_length", "changed", "flag", "code_page", "_ignore_errors"
    )

    ## instance construction and initialization methods

    def __init__(
            self, fields=None, header_length=0, record_length=0,
            record_count=0, signature=0x03, last_update=None, flag=0,
            code_page=0, ignore_errors=False,
    ):
        """Initialize instance.

        Arguments:
            fields:
                a list of field definitions;
            record_length:
                size of the records;
            header_length:
                size of the header;
            record_count:
                number of records stored in DBF;
            signature:
                version number (aka signature). using 0x03 as a default meaning
                "File without DBT". for more information about this field visit
                ``http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET``
            last_update:
                date of the DBF's update. this could be a string ('yymmdd' or
                'yyyymmdd'), timestamp (int or float), datetime/date value,
                a sequence (assuming (yyyy, mm, dd, ...)) or an object having
                callable ``ticks`` field.
            ignore_errors:
                error processing mode for DBF fields (boolean)

        """
        self.signature = signature
        self.fields = [] if fields is None else list(fields)
        self.last_update = get_gate(last_update)
        self.record_length = record_length
        self.header_length = header_length
        self.record_count = record_count
        self.flag = flag
        self.code_page = code_page
        self.ignore_errors = ignore_errors
        self.changed = False

    @classmethod
    def from_string(cls, string):
        """Return header instance from the string object."""
        return cls.from_stream(io.StringIO(str(string)))

    @classmethod
    def from_stream(cls, stream):
        """Return header object from the stream."""

        # FoxPro DBF file structure
        # http://msdn.microsoft.com/en-us/library/aa975386%28v=vs.71%29.aspx

        stream.seek(0)
        _data = stream.read(32)
        if _data is None or len(_data) < 32:
            raise ValueError('header data less than 32 bytes')

        (_cnt, _hdrLen, _recLen) = struct.unpack("<I 2H", _data[4:12])
        #reserved = _data[12:32]
        _year, _month, _day = _data[1:4]
        if _year < 80:
            # dBase II started at 1980.  It is quite unlikely
            # that actual last update date is before that year.
            _year += 2000
        else:
            _year += 1900

        _tableFlag = _data[28]
        _codePage = _data[29]

        ## create header object
        _obj = cls(None, _hdrLen, _recLen, _cnt, _data[0],
                   (_year, _month, _day), _tableFlag, _codePage)
        ## append field definitions
        # position 0 is for the deletion flag
        _pos = 1
        _data = stream.read(1)
        while _data[0] != 0x0D:
            _data += stream.read(31)
            _fld = fields.lookup_for(chr(_data[11])).from_string(_data, _pos)
            _obj._add_field(_fld)
            _pos = _fld.end
            _data = stream.read(1)
        return _obj

    ## properties
    @property
    def year(self):
        return self.last_update.year

    @property
    def month(self):
        return self.last_update.month

    @property
    def day(self):
        return self.last_update.day

    @property
    def has_memo(self):
        """True if at least one field is a Memo field"""
        for _field in self.fields:
            if _field.isMemo:
                return True
        return False

    @property
    def ignore_errors(self):
        """Error processing mode for DBF field value conversion

        if set, failing field value conversion will return
        ``INVALID_VALUE`` instead of raising conversion error.

        """
        return self._ignore_errors

    @ignore_errors.setter
    def ignore_errors(self, value):
        """Update `ignoreErrors` flag on self and all fields"""
        value = bool(value)
        self._ignore_errors = value
        for _field in self.fields:
            _field.ignore_errors = value

    def index_of_field_name(self, name):
        """Index of field named ``name``."""
        for index, field in enumerate(self.fields):
            if field.name == name:
                return index
        else:
            raise ValueError('Field not found: {0}'.format(name))

    def __str__(self):
        _rv = textwrap.dedent("""
            Version (signature): 0x%02X
                    Last update: %s
                  Header length: %d
                  Record length: %d
                   Record count: %d
                     Table Flag: 0x%02X
                      Code Page: 0x%02X

             FieldName Type Len Dec
            """ % (self.signature, self.last_update, self.header_length,
                   self.record_length, self.record_count, self.flag, self.code_page)
        )
        _rv += "\n".join([
            "%10s %4s %3s %3s" % field.field_info() for field in self.fields
        ])
        return _rv

    ## internal methods

    def _add_field(self, *defs):
        """Internal variant of the `addField` method.

        This method doesn't set `self.changed` field to True.

        Return value is a length of the appended records.
        Note: this method doesn't modify ``recordLength`` and
        ``headerLength`` fields. Use `addField` instead of this
        method if you don't exactly know what you're doing.

        """
        # insure we have dbf.DbfFieldDef instances first (instantiation
        # from the tuple could raise an error, in such a case I don't
        # wanna add any of the definitions -- all will be ignored)
        _defs = []
        _recordLength = self.record_length
        for _def in defs:
            if isinstance(_def, fields.DbfFieldDef):
                _obj = _def
            else:
                (_name, _type, _len, _dec) = (tuple(_def) + (None,) * 4)[:4]
                _cls = fields.lookup_for(_type)
                _obj = _cls(
                    _name, _len, _dec, _recordLength,
                    ignore_errors=self._ignore_errors
                )
            _recordLength += _obj.length
            _defs.append(_obj)
        # and now extend field definitions and
        # update record length
        self.fields += _defs
        return _recordLength - self.record_length

    def _calc_header_length(self):
        """Update self.headerLength attribute after change to header contents
        """
        # recalculate headerLength
        self.header_length = 32 + (32 * len(self.fields)) + 1
        if self.signature == 0x30:
            # Visual FoxPro files have 263-byte zero-filled field for backlink
            self.header_length += 263

    ## interface methods

    def flush(self, stream):
        if not self.changed:
            return
        self.set_last_update()
        self.write(stream)

    def set_memo_file(self, memo):
        """Attach MemoFile instance to all memo fields; check header signature
        """
        _has_memo = False
        for _field in self.fields:
            if _field.isMemo:
                _field.file = memo
                _has_memo = True
        # for signatures list, see
        # http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET
        # http://www.dbf2002.com/dbf-file-format.html
        # If memo is attached, will use 0x30 for Visual FoxPro file,
        # 0x83 for dBASE III+.
        if (_has_memo and
                    self.signature not in (0x30, 0x83, 0x8B, 0xCB, 0xE5, 0xF5)
        ):
            if memo.is_fpt:
                self.signature = 0x30
            else:
                self.signature = 0x83
        self._calc_header_length()

    def add_field(self, *defs):
        """Add field definition to the header.

        Examples:
            dbfh.addField(
                ("name", "C", 20),
                dbf.DbfCharacterFieldDef("surname", 20),
                dbf.DbfDateFieldDef("birthdate"),
                ("member", "L"),
            )
            dbfh.addField(("price", "N", 5, 2))
            dbfh.addField(dbf.DbfNumericFieldDef("origprice", 5, 2))

        """
        if not self.record_length:
            self.record_length = 1
        self.record_length += self._add_field(*defs)
        self._calc_header_length()
        self.changed = True

    def write(self, stream):
        """Encode and write header to the stream."""
        if not stream.writable():
            return
        stream.seek(0)
        stream.write(self.to_bytes())
        stream.write(b"".join([_fld.to_bytes() for _fld in self.fields]))
        stream.write(b'\x0D')  # cr at end of all hdr data
        _pos = stream.tell()
        if _pos < self.header_length:
            stream.write(b"\x00" * (self.header_length - _pos))
        self.changed = False

    def to_bytes(self):
        """Returned 32 chars length string with encoded header."""
        if self.has_memo:
            self.flag |= 0x02
        else:
            self.flag &= ~0x02

        _header = struct.pack(
            "<4B I 2H 16s 2B 2s",
            self.signature,
            self.year - 1900,
            self.month,
            self.day,
            self.record_count,
            self.header_length,
            self.record_length,
            b"\x00" * 16,
            self.flag,
            self.code_page,
            b"\x00" * 2
        )

        assert len(_header) == 32
        return _header

    def set_last_update(self, date=None):
        """Update ``self.lastUpdate`` field."""
        if date is None:
            date = datetime.date.today()
        self.last_update = date

    def __getitem__(self, item):
        """Return a field definition by numeric index or name string"""
        if isinstance(item, str):
            _name = item.upper()
            for _field in self.fields:
                if _field.name == _name:
                    return _field
            else:
                raise KeyError(item)
        else:
            # item must be field index
            return self.fields[item]

# vim: et sts=4 sw=4 :
