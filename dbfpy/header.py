"""DBF header definition.

TODO:
  - test encoding (windows console can't print utf-8 characters)
"""

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2010/12/14 11:07:45 $"[7:-2]

__all__ = ["DbfHeader"]

import io
import datetime
import struct
import textwrap

from .fields import DbfField, DbfFields
from .utils import get_date
from .code_page import CodePage


class DbfHeader():
    """Dbf header definition.

    For more information about dbf header format visit
    `http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_STRUCT`

    Examples:
        Create an empty dbf header and add some field definitions:
            dbfh = DbfHeader()
            dbfh.add_field(("name", "C", 10))
            dbfh.add_field(("date", "D"))
            dbfh.add_field(DbfNumericField("price", 5, 2))
        Create a dbf header with field definitions:
            dbfh = DbfHeader([
                ("name", "C", 10),
                ("date", "D"),
                DbfNumericField("price", 5, 2),
            ])

    """

    __slots__ = (
        "signature", "fields", "_last_update", "record_length", "record_count",
        "header_length", "_changed", "flag", "_code_page", "_ignore_errors"
    )

    ## instance construction and initialization methods

    def __init__(
            self, fields=None, header_length=0, record_length=1,
            record_count=0, signature=0x03, last_update=None, flag=0,
            code_page=0, ignore_errors=False,
    ):
        """Initialize instance.

        Arguments:
            fields:
                list of field definitions ``DbfField```;
            record_length:
                size of the records; default is 1 byte of deletion flag
            header_length:
                size of the header (including fields definition);
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
        # for IDE inspection
        self._ignore_errors = self._code_page = self._last_update = None

        self.signature = signature
        self.fields = list(fields) if fields is not None else []
        self.last_update = last_update
        self.record_length = record_length
        self.header_length = header_length
        self.record_count = record_count
        self.flag = flag
        self.code_page = code_page
        self.ignore_errors = ignore_errors
        self._changed = False

        if not self.ignore_errors and (
            self._calc_record_length() != self.record_length
        ):
            raise ValueError("record length doesn't match sum(fields.length)")

    @property
    def last_update(self):
        return self._last_update

    @last_update.setter
    def last_update(self, date):
        self._last_update = get_date(date)

    @property
    def code_page(self):
        return self._code_page

    @code_page.setter
    def code_page(self, code_page):
        self._code_page = (
            code_page if isinstance(code_page, CodePage)
            else CodePage(code_page)
        )

    @property
    def changed(self):
        return self._changed

    @classmethod
    def parse(cls, stream):
        """Return header object from the stream."""

        if isinstance(stream, bytes):
            stream = io.BytesIO(stream)

        # FoxPro DBF file structure
        # http://msdn.microsoft.com/en-us/library/aa975386%28v=vs.71%29.aspx
        stream.seek(0)
        data = stream.read(32)
        if data is None or len(data) < 32:
            raise ValueError('header data less than 32 bytes')

        (
            signature, year, month, day, record_count, header_length,
            record_length, flag, code_page
        ) = struct.unpack("< 4B I 2H 16x 2B 2x", data)

        if year < 80:
            # dBase II started at 1980.  It is quite unlikely
            # that actual last update date is before that year.
            year += 2000
        else:
            year += 1900

        code_page = CodePage(code_page)

        # TODO: check file size greater than record count

        # read fields definition
        fields = []
        # position 0 is for the deletion flag
        pos = 1
        while True:
            data = stream.read(32)
            if len(data) < 32 or data[0] == 0x0D:
                break
            field = DbfFields.parse(data, pos)
            if pos != field.start:
                raise ValueError(
                    'dbf fields definition is corrupt, '
                    'fields start does not match.'
                )
            fields.append(field)
            pos = field.start + field.length

        # DbfHeader instance
        return cls(
            fields=fields,
            record_count=record_count,
            record_length=record_length,
            header_length=header_length,
            signature=signature,
            last_update=datetime.date(year, month, day),
            flag=flag,
            code_page=code_page,
        )

    ## properties
    @property
    def has_memo(self):
        """True if at least one field is a Memo field"""
        for _field in self.fields:
            if _field.is_memo:
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
        """Update `ignore_errors` flag on self and all fields"""
        value = bool(value)
        self._ignore_errors = value
        for _field in self.fields:
            _field.ignore_errors = value

    def index_of_field_name(self, name):
        """Index of field named ``name``."""
        if isinstance(name, str):
            name = name.encode(self.code_page.encoding)

        for index, field in enumerate(self.fields):
            if field.name == name:
                return index
        else:
            raise KeyError('Field not found: {}'.format(name))

    def field_names(self):
        return (field.name for field in self.fields)

    def __str__(self):
        return textwrap.dedent("""
            Signature:      0x%02X
            Last update:    %s
            Header length:  %d
            Record length:  %d
            Record count:   %d
            Table Flag:     0x%02X
            Code Page:      %s

        """ % (
            self.signature, self.last_update, self.header_length,
            self.record_length, self.record_count, self.flag, self.code_page)
        ) + "\n".join([
            "%10s %4s %3s %3s" % tuple(row) for row in (
                ['FieldName Type Len Dec'.split()] +
                [
                    [
                        field.name, field.type_code,
                        field.length, field.decimal_count
                    ] for field in self.fields
                ]
            )
        ])

    ## internal methods

    def _calc_record_length(self):
        """Calculte record length using fields.length"""
        return 1 + sum(field.length for field in self.fields)

    def _calc_header_length(self):
        """Update self.headerLength attribute after change to header contents"""
        # recalculate headerLength
        self.header_length = 32 + (32 * len(self.fields)) + 1
        if self.signature == 0x30:
            # Visual FoxPro files have 263-byte zero-filled field for backlink
            self.header_length += 263

    ## interface methods

    def flush(self, stream):
        if not self.changed:
            return
        self.last_update = datetime.date.today()
        self.write(stream)

    def set_memo_file(self, memo):
        """Attach MemoFile instance to all memo fields; check header signature"""
        has_memo = False
        for field in self.fields:
            if field.is_memo:
                field.file = memo
                has_memo = True
        # for signatures list, see
        # http://www.clicketyclick.dk/databases/xbase/format/dbf.html#DBF_NOTE_1_TARGET
        # http://www.dbf2002.com/dbf-file-format.html
        # If memo is attached, will use 0x30 for Visual FoxPro file,
        # 0x83 for dBASE III+.
        if has_memo and self.signature not in (0x30, 0x83, 0x8B, 0xCB, 0xE5, 0xF5):
            if memo.is_fpt:
                self.signature = 0x30
            else:
                self.signature = 0x83
        self._calc_header_length()

    def add_field(self, *fields):
        """Add field definition to the header.

        fields:
            list of DbfField or list of (type_code, name, length, decimal)
        Examples:
            dbf.add_field(
                ("name", "C", 20),
                dbf.DbfCharacterField("surname", 20),
                dbf.DbfDateField("birthdate"),
                ("member", "L"),
            )
            dbfh.add_field(["price", "N", 5, 2])
            dbfh.add_field(dbf.DbfNumericField("origprice", 5, 2))

        """
        if self.record_count > 0:
            raise TypeError("At least one record was added, "
                            "structure can't be changed")

        for field in fields:
            if not isinstance(field, DbfField):
                if not hasattr(field, '__iter__'):
                    raise 'field is not a {} ({})'.format(DbfField, type(field))

                args = list(field)[:4]
                type_code = args.pop(0)
                name = args.pop(0)

                if isinstance(name, str):
                    name = name.encode(self.code_page.encoding)

                field = DbfFields.get(type_code)(
                    name,
                    *args,
                    start=self.record_length,
                    ignore_errors=self._ignore_errors
                )

            self.record_length += field.length
            self.fields.append(field)

        # and now extend field definitions and
        # update record record_length
        self._calc_header_length()
        self._changed = True

    def write(self, stream):
        """Encode and write header to the stream."""
        if not stream.writable():
            return
        stream.seek(0)
        stream.write(self.to_bytes())
        stream.write(b"".join([_fld.to_bytes() for _fld in self.fields]))
        stream.write(b'\x0D')  # cr at end of all hdr data
        pos = stream.tell()
        if pos < self.header_length:
            stream.write(b"\x00" * (self.header_length - pos))
        self._changed = False

    def to_bytes(self):
        """Returned 32 chars length string with encoded header."""
        if self.has_memo:
            self.flag |= 0x02
        else:
            self.flag &= ~0x02

        return struct.pack(
            "< 4B I 2H 16s 2B 2s",
            self.signature,
            self.last_update.year - 1900,
            self.last_update.month,
            self.last_update.day,
            self.record_count,
            self.header_length,
            self.record_length,
            b"\x00" * 16,
            self.flag,
            self.code_page.code_page,
            b"\x00" * 2
        )

    def __contains__(self, key):
        try:
            self[key]
            return True
        except (KeyError, IndexError, TypeError):
            return False

    def __getitem__(self, key):
        """Return a field definition by numeric index or name string"""
        if isinstance(key, str):
            key = key.encode(self.code_page.encoding)

        if isinstance(key, bytes):
            name = key.upper()
            for field in self.fields:
                if field.name == name:
                    return field
            else:
                raise KeyError(key)

        if isinstance(key, int):
            # item must be field index
            return self.fields[key]

        raise TypeError('Unsupported key type ({})'.format(type(key)))

# vim: et sts=4 sw=4 :
