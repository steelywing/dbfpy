"""DBF record definition.

"""

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:05:49 $"[7:-2]

__all__ = ["DbfRecord"]

import io
from .header import DbfHeader
from . import utils
import locale


class DbfRecord(object):
    """DBF record.

    Instances of this class shouldn't be created manualy,
    use `dbf.Dbf.newRecord` instead.

    Class implements mapping/sequence interface, so
    fields could be accessed via their names or indexes
    (names is a preffered way to access fields).

    Hint:
        Use `store` method to save modified record.

    Examples:
        Add new record to the database:
            db = Dbf(filename)
            rec = db.newRecord()
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()
        Or the same, but modify existed
        (second in this case) record:
            db = Dbf(filename)
            rec = db[2]
            rec["FIELD1"] = value1
            rec["FIELD2"] = value2
            rec.store()

    """

    __slots__ = "dbf", "header", "index", "deleted", "fields"

    ## creation and initialization

    def __init__(self, header, index=None, deleted=False, data=None):
        """Instance initialization.

        Arguments:
            fields:
                A `DbfHeader` instance this record belonogs to.
            index:
                An integer record index or None. If this value is
                None, record will be appended to the DBF.
            deleted:
                Boolean flag indicating whether this record
                is a deleted record.
            data:
                A sequence or None. This is a data of the fields.
                If this argument is None, default values will be used.

        """
        if not isinstance(header, DbfHeader):
            raise TypeError('header is not a %s' % DbfHeader)

        self.header = header
        self.index = index
        self.deleted = deleted
        if data is None:
            self.fields = [field.default_value for field in header.fields]
        elif hasattr(data, '__iter__'):
            self.fields = list(data)
        elif isinstance(data, io.IOBase):
            self.from_stream(data)
        elif isinstance(data, bytes):
            self.from_bytes(data)
        else:
            raise TypeError("doesn't support this field data (%s)" % type(data))

    @property
    def position(self):
        """File position of record"""
        return (self.header.header_length +
                self.index * self.header.record_length)

    def from_stream(self, stream):
        """Read record from the stream.

        Arguments:
            stream:
                The record stream.
        """
        # FIXME: validate file position
        stream.seek(self.position)
        self.from_bytes(stream.read(self.header.record_length))

    def from_bytes(self, string):
        """Return record read from the string.

        Arguments:
            string:
                String record should be loaded from.
        """
        self.fields = [field.decode_from_record(string) for field in self.header.fields]

    def __str__(self):
        template = "%%%ds: %%s (%%s)" % max(
            len(field.name) for field in self.header.fields
        )

        string = []
        for field in self.header.fields:
            value = self[field.name]
            if value is utils.INVALID_VALUE:
                string.append(
                    template % (field, "None", "value cannot be decoded")
                )
            else:
                string.append(template % (field.name, value, type(value)))
        return "\n".join(string)

    ## utility methods

    def validate_index(self, allow_undefined=True, check_range=False):
        """Valid ``self.index`` value.

        If ``allow_undefined`` argument is True functions does nothing
        in case of ``self.index`` pointing to None object.

        """
        if self.index is None:
            if not allow_undefined:
                raise ValueError("Index is undefined")
        elif self.index < 0:
            raise ValueError("Index can't be negative (%s)" % self.index)
        elif check_range and self.index <= self.header.record_count:
            raise ValueError("There are only %d records in the DBF" %
                             self.header.record_count)

    def delete(self):
        """Mark method as deleted."""
        self.deleted = True

    def to_bytes(self):
        """Return string packed record values."""
        return b"".join(
            [(b' ', b'*')[self.deleted]] +
            [
                _def.encode_value(_dat)
                for (_def, _dat) in zip(self.header.fields, self.fields)
            ]
        )

    def as_list(self):
        """Return a flat list of fields.

        Note:
            Change of the list's values won't change
            real values stored in this object.

        """
        return self.fields[:]

    def as_dict(self):
        """Return a dictionary of fields.

        Note:
            Change of the dicts's values won't change
            real values stored in this object.

        """
        return dict([_i for _i in zip(self.dbf.field_names, self.fields)])

    def __getitem__(self, key):
        """Return value by field name or field index."""
        if isinstance(key, int):
            # integer index of the field
            return self.fields[key]
        # assuming string field name
        return self.fields[self.header.index_of_field_name(key)]

    def __setitem__(self, key, value):
        """Set field value by integer index of the field or string name."""
        if isinstance(key, int):
            # integer index of the field
            return self.fields[key]
        # assuming string field name
        self.fields[self.header.index_of_field_name(key)] = value

# vim: et sts=4 sw=4 :
