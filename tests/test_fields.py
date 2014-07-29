__author__ = 'Wing'

import unittest
import struct
import env
from dbfpy import fields


# Implement DbfField
class _DbfField(fields.DbfField):
    type_code = ' '
    fixed_length = 0
    default_value = 0


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

    def test_parse_field(self):
        # test parse
        field_string = bytearray(struct.pack(
            '< 11s c L 2B 14s',
            b'NAME',    # Field name
            b'N',       # Field type
            1,          # Displacement of field in record
            10,         # Length of field
            2,          # Number of decimal places
            b'\x00' * 14,
        ))

        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfNumericField)
        self.assertEqual(field.to_bytes(), bytes(field_string))
        self.assertEqual(field.name, b'NAME')
        self.assertEqual(field.start, 1)
        self.assertEqual(field.length, 10)
        self.assertEqual(field.decimal_count, 2)

        field_string[11] = b'F'[0]
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfFloatField)
        self.assertEqual(field.to_bytes(), bytes(field_string))

        field_string[11] = b'Y'[0]
        field_string[16] = 8
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfCurrencyField)
        self.assertEqual(field.to_bytes(), bytes(field_string))

        field_string[11] = b'I'[0]
        field_string[16] = 4
        field_string[17] = 0
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfIntegerField)
        self.assertEqual(field.to_bytes(), bytes(field_string))

        field_string[11] = b'L'[0]
        field_string[16] = 1
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfLogicalField)
        # self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'D'[0]
        field_string[16] = 8
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfDateField)
        # self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'T'[0]
        field_string[16] = 8
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfDateTimeField)
        # self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'C'[0]
        field_string[16] = 200
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfCharacterField)
        self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'P'[0]
        field_string[16] = 4
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfPictureField)
        # self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'M'[0]
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfMemoField)
        self.assertEqual(field.to_bytes(), field_string)

        field_string[11] = b'G'[0]
        field = fields.DbfFields.parse(bytes(field_string))
        self.assertIsInstance(field, fields.DbfGeneralField)
        self.assertEqual(field.to_bytes(), field_string)

        # test parse length
        field_string.pop()
        with self.assertRaises(ValueError):
            fields.DbfFields.parse(bytes(field_string))

if __name__ == '__main__':
    unittest.main()
