"""DBF record definition.

"""

__version__ = "$Revision: 1.7 $"[11:-2]
__date__ = "$Date: 2007/02/11 09:05:49 $"[7:-2]

__all__ = ["DbfRecord"]

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

    __slots__ = "dbf", "index", "deleted", "field_data"

    ## creation and initialization

    def __init__(self, dbf, index=None, deleted=False, data=None):
        """Instance initialization.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance this record belonogs to.
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
        self.dbf = dbf
        # XXX: I'm not sure ``index`` is necessary
        self.index = index
        self.deleted = deleted
        if data is None:
            self.field_data = [_fd.default_value for _fd in dbf.header.fields]
        else:
            self.field_data = list(data)

    # XXX: validate self.index before calculating position?
    @property
    def position(self):
        return (self.dbf.header.header_length +
                self.index * self.dbf.header.record_length)

    @classmethod
    def raw_from_stream(cls, dbf, index):
        """Return raw record contents read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance containing the record.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is a string containing record data in DBF format.

        """
        # XXX: may be write smth assuming, that current stream
        # position is the required one? it could save some
        # time required to calculate where to seek in the file
        dbf.stream.seek(dbf.header.header_length +
                        index * dbf.header.record_length)
        return dbf.stream.read(dbf.header.record_length)

    @classmethod
    def from_stream(cls, dbf, index):
        """Return a record read from the stream.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            index:
                Index of the record in the records' container.
                This argument can't be None in this call.

        Return value is an instance of the current class.

        """
        return cls.from_string(dbf, cls.raw_from_stream(dbf, index), index)

    @classmethod
    def from_string(cls, dbf, string, index=None):
        """Return record read from the string object.

        Arguments:
            dbf:
                A `Dbf.Dbf` instance new record should belong to.
            string:
                A string new record should be created from.
            index:
                Index of the record in the container. If this
                argument is None, record will be appended.

        Return value is an instance of the current class.

        """
        return cls(dbf, index, string[0] == "*",
                   [_fd.decode_from_record(string) for _fd in dbf.header.fields])

    def __str__(self):
        _template = "%%%ds: %%s (%%s)" % max(len(_fld)
                                             for _fld in self.dbf.field_names)
        _rv = []
        for _fld in self.dbf.field_names:
            _val = self[_fld]
            if _val is utils.INVALID_VALUE:
                _rv.append(_template %
                           (_fld, "None", "value cannot be decoded"))
            else:
                _rv.append(_template % (_fld, _val, type(_val)))
        return "\n".join(_rv)

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
        elif check_range and self.index <= self.dbf.header.recordCount:
            raise ValueError("There are only %d records in the DBF" %
                             self.dbf.header.recordCount)

    ## interface methods

    def store(self):
        """Store current record in the DBF.

        If ``self.index`` is None, this record will be appended to the
        records of the DBF this records belongs to; or replaced otherwise.

        """
        self.validate_index()
        if self.index is None:
            self.index = len(self.dbf)
            self.dbf.append(self)
        else:
            self.dbf[self.index] = self

    def delete(self):
        """Mark method as deleted."""
        self.deleted = True

    def to_bytes(self):
        """Return string packed record values."""
        return b"".join(
            [(b' ', b'*')[self.deleted]] +
            [
                _def.encode_value(_dat)
                for (_def, _dat) in zip(self.dbf.header.fields, self.field_data)
            ]
        )

    def as_list(self):
        """Return a flat list of fields.

        Note:
            Change of the list's values won't change
            real values stored in this object.

        """
        return self.field_data[:]

    def as_dict(self):
        """Return a dictionary of fields.

        Note:
            Change of the dicts's values won't change
            real values stored in this object.

        """
        return dict([_i for _i in zip(self.dbf.field_names, self.field_data)])

    def __getitem__(self, key):
        """Return value by field name or field index."""
        if isinstance(key, int):
            # integer index of the field
            return self.field_data[key]
        # assuming string field name
        return self.field_data[self.dbf.header.index_of_field_name(key)]

    def __setitem__(self, key, value):
        """Set field value by integer index of the field or string name."""
        if isinstance(key, int):
            # integer index of the field
            return self.field_data[key]
        # assuming string field name
        self.field_data[self.dbf.header.index_of_field_name(key)] = value

# vim: et sts=4 sw=4 :
