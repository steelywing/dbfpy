__author__ = 'Wing'

import unittest
import struct
import env
from dbfpy import fields


# Implement DbfField
class _DbfField(fields.DbfField):
    type_code = ' '
    default_value = 0
    default_length = 0


class FieldsTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_field_name(self):
        dbf_field = _DbfField(b'NAME')
        self.assertEqual(dbf_field.name, b'NAME')
        dbf_field.name = b'ID'
        self.assertEqual(dbf_field.name, b'ID')

    def test_field_name_upper_case(self):
        dbf_field = _DbfField(b'item')
        self.assertEqual(dbf_field.name, b'ITEM')

    def test_field_name_max_length(self):
        dbf_field = _DbfField(b'NAME')
        # max length 10 bytes
        with self.assertRaises(ValueError):
            dbf_field.name = b'N' * 11

    def test_parse_type_code(self):
        field_string = bytearray(struct.pack(
            '< 11s c L 2B 14s',
            b'NAME',    # Field name
            b'N',       # Field type
            1,          # Displacement of field in record
            20,         # Length of field
            2,          # Number of decimal places
            b'\x00' * 14,
        ))

        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfNumericField)
        self.assertEqual(field.name, b'NAME')
        self.assertEqual(field.start, 1)
        self.assertEqual(field.length, 20)
        self.assertEqual(field.decimal_count, 2)

        field_string[11:12] = b'F'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfFloatField)

        field_string[11:12] = b'Y'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfCurrencyField)

        field_string[11:12] = b'I'
        field_string[17] = 0
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfIntegerField)

        field_string[11:12] = b'L'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfLogicalField)

        field_string[11:12] = b'D'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfDateField)

        field_string[11:12] = b'T'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfDateTimeField)

        field_string[11:12] = b'P'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfPictureField)

        field_string[11:12] = b'C'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfCharacterField)

        field_string[11:12] = b'M'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfMemoField)

        field_string[11:12] = b'G'
        field = fields.DbfFields.parse(bytes(field_string), 1)
        self.assertIsInstance(field, fields.DbfGeneralField)

    def test_parse_length(self):
        with self.assertRaises(ValueError):
            fields.DbfFields.parse(b' ', 1)

if __name__ == '__main__':
    unittest.main()
