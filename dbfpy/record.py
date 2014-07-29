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
    (names is a preferred way to access fields).
    """

    __slots__ = "dbf", "header", "_index", "deleted", "fields"

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
                Can be None, sequence, IOBase stream or bytes,
                This is a data of the fields.
                If this argument is None, default values will be used.

        """
        if not isinstance(header, DbfHeader):
            raise TypeError('header is not a %s' % DbfHeader)

        self.header = header
        # for IDE inspection
        self._index = None
        self.index = index
        self.deleted = deleted
        if data is None:
            self.fields = [field.default_value for field in header.fields]
        elif hasattr(data, '__iter__'):
            self.fields = list(data)
        elif isinstance(data, (io.IOBase, bytes)):
            self.read(data)
        else:
            raise TypeError("doesn't support this field data (%s)" % type(data))

    @property
    def position(self):
        """File position of record"""
        if self.index is None:
            raise IndexError('Record index is None')

        return (self.header.header_length +
                self.index * self.header.record_length)

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        if index is None:
            # None means append to the last, we set the real index when write to stream
            self._index = None
            return

        if not isinstance(index, int):
            raise TypeError("Index must be int")

        if index >= self.header.record_count:
            raise IndexError("Record index out of range")

        if index < 0:
            # index from the right side
            index += self.header.record_count + 1

        self._index = index

    def decode(self, string):
        """Return record read from the string."""
        try:
            return [field.decode(
                string[field.start:field.start + field.length],
                encoding=self.header.code_page.encoding
            ) for field in self.header.fields]
        except:
            if self.header.ignore_errors:
                return utils.INVALID_VALUE
            else:
                raise

    def read(self, string):
        """Read record from string or stream."""
        if isinstance(string, io.IOBase):
            stream = string
            if not stream.readable():
                raise OSError('Stream is not readable')
            # FIXME: validate file position
            stream.seek(self.position)
            string = stream.read(self.header.record_length)
        if string[0:1] not in b' *':
            raise ValueError('Record deleted flag error ({})', string[0])
        self.deleted = (string[0:1] == b'*')
        self.fields = self.decode(string)
        return self

    def __str__(self):
        names = (field.name for field in self.header.fields)
        template = "%%%ds: %%s (%%s)" % max(len(name) for name in names)

        rows = []
        for field in self.header.fields:
            value = self[field.name]
            if value is utils.INVALID_VALUE:
                rows.append(
                    template % (field, "None", "value cannot be decoded")
                )
            else:
                rows.append(template % (field.name, value, type(value)))
        return "\n".join(rows)

    def delete(self):
        """Mark method as deleted."""
        self.deleted = True

    def to_bytes(self):
        """Return string packed record values."""
        return b''.join(
            [(b' ', b'*')[self.deleted]] +
            [
                _def.encode(_dat, encoding=self.header.code_page.encoding)
                for (_def, _dat) in zip(self.header.fields, self.fields)
            ]
        )

    def as_dict(self):
        """Return a dictionary of fields.

        Note:
            Change of the dicts's values won't change
            real values stored in this object.

        """
        return dict([field for field in zip(self.header.field_names(), self.fields)])

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
