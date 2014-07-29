"""DBF fields definitions.

TODO:
  - make memos work
  - test encode (windows console can't print utf-8 characters)
  - use DbfField.__new__ construct
"""

__version__ = "$Revision: 1.15 $"[11:-2]
__date__ = "$Date: 2010/12/14 11:04:49 $"[7:-2]

# field classes added at the end of the module
__all__ = ['DbfField', 'DbfFields']

import datetime
import struct
import locale

from .memo import MemoData
from . import utils
from .code_page import CodePage


class DbfFields:
    """All DbfField implementation."""
    _fields = {}

    @classmethod
    def register(cls, field_class):
        """Register field definition class.

        ``field_class`` should be subclass of the `DbfField`.

        Use `lookupFor` to retrieve field definition class
        by the type code.

        """
        if field_class.type_code is None:
            raise ValueError("type code ({}) isn't defined".format(field_class.type_code))

        key = field_class.type_code.upper()
        cls._fields[key] = field_class

    @classmethod
    def get(cls, type_code):
        """Return field definition class for the given type code.

        ``type_code`` must be a 1 length str or bytes. That type should be
        previously registered.

        Use `register` to register new field class.

        Return:
            Return value is a subclass of the `DbfField`.

        """
        if isinstance(type_code, str):
            type_code = type_code.encode()

        if not isinstance(type_code, bytes) or type_code.upper() not in cls._fields:
            raise KeyError('type code ({}) not support'.format(type_code))

        return cls._fields[type_code.upper()]

    @classmethod
    def parse(cls, string, ignore_errors=False):
        """Decode dbf field definition from the string data.

        Arguments:
            string:
                a string, dbf definition is decoded from. length of
                the string must be 32 bytes.
            ignore_errors:
                initial error processing mode for the new field (boolean)

        """
        if not isinstance(string, bytes) or len(string) != 32:
            raise ValueError('String ({}) is not a 32 length bytes'.format(string))

        (
            name, type_code, start, length,
            decimal_count, flag, ai_next, ai_step
        ) = struct.unpack('< 11s c I 3B I B', string[:24])

        return cls.get(type_code)(
            utils.unzfill(name),
            start=start,
            length=length,
            decimal_count=decimal_count,
            flag=flag,
            ai_next=ai_next,
            ai_step=ai_step,
            ignore_errors=ignore_errors,
        )


class DbfField(object):
    """Abstract field definition.

    Child classes must override ``type`` class attribute to provide datatype
    infromation of the field definition. For more info about types visit
    `http://www.clicketyclick.dk/databases/xbase/format/data_types.html`

    Also child classes must override ``defaultValue`` field to provide
    default value for the field value.

    If child class has fixed length ``length`` class attribute must be
    overriden and set to the valid value. None value means, that field
    isn't of fixed length.

    Note: ``name`` field must not be changed after instantiation.

    """

    __slots__ = (
        "_name", "start", "length", "decimal_count",
        "flag", "ai_next", "ai_step", "ignore_errors"
    )

    # field type. for more information about fields types visit
    # `http://www.clicketyclick.dk/databases/xbase/format/data_types.html`
    # must be overriden in child classes
    type_code = None

    # length of the field, None in case of variable-length field,
    # or a number if this field is a fixed-length field
    fixed_length = None

    # default value for the field. this field must be
    # overridden in child classes
    default_value = None

    # True if field data is kept in the Memo file
    is_memo = False

    def __init__(
        self, name, length=None, decimal_count=0, start=None,
        flag=0, ai_next=0, ai_step=0, ignore_errors=False,
    ):
        """Initialize instance."""

        # TODO: add length validation

        if self.fixed_length is None:
            if length is None:
                raise ValueError("[%s] Length isn't specified" % name)
            length = int(length)
            if length <= 0:
                raise ValueError("[%s] Length must be a positive integer" % name)
        else:
            length = self.fixed_length

        self.name = name
        # FIXME: validate length according to the specification at
        # http://www.clicketyclick.dk/databases/xbase/format/data_types.html
        self.start = start
        self.length = length
        self.decimal_count = decimal_count

        # Field flags:
        # 0x01   System Column (not visible to user)
        # 0x02   Column can store null values
        # 0x04   Binary column (for CHAR and MEMO only)
        # 0x06   (0x02+0x04) When a field is NULL and binary (Integer, Currency, and Character/Memo fields)
        # 0x0C   Column is autoincrementing
        self.flag = flag

        # Value of autoincrement Next value
        self.ai_next = ai_next

        # Value of autoincrement Step value
        self.ai_step = ai_step

        self.ignore_errors = ignore_errors

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not isinstance(name, bytes):
            raise TypeError('name must be bytes')

        if len(name) > 10:
            raise ValueError("field name '%s' must less than 10 bytes" % name)

        self._name = name.upper()

    def to_bytes(self):
        """Return encoded field definition.

        Return:
            Return bytes object containing encoded
            definition of this field.
        """
        return struct.pack(
            '< 11s c I 3B I B 8s',
            self.name,
            self.type_code,
            self.start,
            self.length,
            self.decimal_count,
            self.flag,
            self.ai_next,
            self.ai_step,
            b'\x00' * 8,
        )

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return "%-10s %1s %3d %3d" % (
            self.name, self.type_code, self.length, self.decimal_count
        )

    def decode(self, value, encoding=None):
        """Return decoded value from string value.

        This method shouldn't be used publicly. It's called from the
        `decodeFromRecord` method.

        This is an abstract method and it must be overridden in child classes.
        """
        raise NotImplementedError

    def encode(self, value, encoding=None):
        """Return str object containing encoded field value.

        This is an abstract method and it must be overriden in child classes.
        """
        raise NotImplementedError


## real classes

class DbfCharacterField(DbfField):
    """Definition of the character field."""

    type_code = b'C'
    default_value = ''

    def decode(self, value, encoding=locale.getpreferredencoding()):
        """Return string object.

        Return value is a ``value`` argument with stripped right spaces.
        """
        return value.decode(encoding).rstrip(" ")

    def encode(self, value, encoding=locale.getpreferredencoding()):
        """Return raw data string encoded from a ``value``."""
        value = str(value).encode(encoding)
        return value[:self.length].ljust(self.length)


class DbfNumericField(DbfField):
    """Definition of the numeric field."""

    type_code = b'N'
    default_value = 0.0

    def decode(self, value, encoding=locale.getpreferredencoding()):
        """Return a number decoded from ``value``.

        Return:
            Return value is float.
        """
        try:
            return float(value.strip(b" \x00").decode(encoding))
        except ValueError:
            return 0.0

    def encode(self, value, encoding=locale.getpreferredencoding()):
        """Return string containing encoded ``value``."""
        string = ("%*.*f" % (self.length, self.decimal_count, value))
        if len(string) > self.length:
            if not (0 <= string.find(".") <= self.length):
                raise ValueError(
                    "[%s] Numeric overflow: %s (field length: %i)" %
                    (self.name, string, self.length)
                )

            string = string[:self.length]

        return string.encode(encoding)


class DbfFloatField(DbfNumericField):
    """Definition of the float field - same as numeric."""

    type_code = b'F'


class DbfIntegerField(DbfField):
    """Definition of the integer field."""

    type_code = b'I'
    fixed_length = 4
    default_value = 0

    def decode(self, value, encoding=None):
        """Return an integer number decoded from ``value``."""
        return struct.unpack("<i", value)[0]

    def encode(self, value, encoding=None):
        """Return string containing encoded ``value``."""
        return struct.pack("<i", int(value))


class DbfCurrencyField(DbfField):
    """Definition of the currency field."""

    type_code = b'Y'
    fixed_length = 8
    default_value = 0.0

    @property
    def decimal_count(self):
        return 4

    @decimal_count.setter
    def decimal_count(self, value):
        pass

    def decode(self, value, encoding=None):
        """Return float number decoded from ``value``."""
        return struct.unpack("<q", value)[0] / 10000.

    def encode(self, value, encoding=None):
        """Return string containing encoded ``value``."""
        return struct.pack("<q", round(value * 10000))


class DbfLogicalField(DbfField):
    """Definition of the logical field."""

    type_code = b'L'
    default_value = -1
    fixed_length = 1

    def decode(self, value, encoding=None):
        """Return True, False or -1 decoded from ``value``."""
        # Note: value always is 1-char string
        if value == b"?":
            return -1
        if value in b"NnFf ":
            return False
        if value in b"YyTt":
            return True
        raise ValueError("[%s] Invalid logical value %r" % (self.name, value))

    def encode(self, value, encoding=None):
        """Return a character from the "TF?" set.

        Return:
            Return value is "T" if ``value`` is True
            "?" if value is -1 or False otherwise.
        """
        if value is True:
            return b"T"
        elif value == -1:
            return b"?"
        else:
            return b"F"


class DbfGeneralField(DbfField):
    """Definition of the general (OLE object) field."""

    type_code = b'G'
    # MemoData type for strings written to the memo file
    memoType = MemoData.TYPE_OBJECT
    default_value = b"\x00" * 4
    fixed_length = 4
    is_memo = True
    # MemoFile instance.  Must be set before reading or writing to the field.
    file = None

    def decode(self, value, encoding=None):
        """Return MemoData instance containing field data."""
        _block = struct.unpack("<L", value)[0]
        if _block:
            return self.file.read(_block)
        else:
            return MemoData(b'', self.memoType)

    def encode(self, value, encoding=None):
        """Return raw data string encoded from a ``value``.

        Note: this is an internal method.
        """
        if value:
            return struct.pack(
                "<L",
                self.file.write(
                    MemoData(value, self.memoType)
                )
            )
        else:
            return self.default_value


class DbfMemoField(DbfGeneralField):
    """Definition of the memo field."""

    type_code = b'M'
    memoType = MemoData.TYPE_MEMO

    def decode(self, value, encoding=locale.getpreferredencoding()):
        """Return memo string."""
        return super().decode(value).decode(encoding)

    def encode(self, value, encoding=locale.getpreferredencoding()):
        """Return raw data string encoded from a ``value``.

        Note: this is an internal method.
        """
        return super().encode(value.encode(encoding))


class DbfPictureField(DbfGeneralField):
    """Definition of the picture field."""

    # not implement yet
    type_code = b'P'
    memoType = MemoData.TYPE_PICTURE


class DbfDateField(DbfField):
    """Definition of the date field."""

    type_code = b'D'

    @utils.classproperty
    def default_value(cls):
        return datetime.date.today()

    # "yyyymmdd" gives us 8 characters
    fixed_length = 8

    def decode(self, value, encoding=locale.getpreferredencoding()):
        """Return a ``datetime.date`` instance decoded from ``value``."""
        if value.strip():
            return utils.get_date(value.decode(encoding))
        else:
            return None

    def encode(self, value, encoding=locale.getpreferredencoding()):
        """
        Return a string-encoded value.

        ``value`` argument should be a value suitable for the
        `utils.getDate` call.

        Return:
            Return value is a string in format "yyyymmdd".
        """
        if value:
            return utils.get_date(value).strftime("%Y%m%d").encode(encoding)
        else:
            return b" " * self.length


class DbfDateTimeField(DbfField):
    """Definition of the timestamp field."""

    # a difference between JDN (Julian Day Number)
    # and GDN (Gregorian Day Number). note, that GDN < JDN
    JDN_GDN_DIFF = 1721425
    type_code = b'T'

    @utils.classproperty
    def default_value(cls):
        return datetime.datetime.now()

    # two 32-bits integers representing JDN and amount of
    # milliseconds respectively gives us 8 bytes.
    # note, that values must be encoded in LE byteorder.
    fixed_length = 8

    def decode(self, value, encoding=None):
        """Return a `datetime.datetime` instance."""
        assert len(value) == self.length
        # LE byteorder
        _jdn, _msecs = struct.unpack("<2I", value)
        if _jdn >= 1:
            _rv = datetime.datetime.fromordinal(_jdn - self.JDN_GDN_DIFF)
            _rv += datetime.timedelta(0, _msecs / 1000.0)
        else:
            # empty date
            _rv = None
        return _rv

    def encode(self, value, encoding=None):
        """Return a string-encoded ``value``."""
        if value:
            value = utils.get_date_time(value)
            # LE byteorder
            string = struct.pack(
                "<2I", value.toordinal() + self.JDN_GDN_DIFF,
                (value.hour * 3600 + value.minute * 60 + value.second) * 1000
            )
        else:
            string = b"\x00" * self.length

        if len(string) != self.length:
            raise ValueError('encoded string length does not match ({})'.format(string))

        return string


## register generic types
for (type_code, klass) in list(globals().items()):
    if (
        isinstance(klass, type) and
        issubclass(klass, DbfField) and
        klass is not DbfField
    ):
        # validate class
        if klass.type_code is None or klass.default_value is None:
            raise NotImplementedError(
                '{} type_code and default_value must be overridden'.format(klass)
            )
        DbfFields.register(klass)
        __all__.append(klass.__name__)

# vim: et sts=4 sw=4 :
