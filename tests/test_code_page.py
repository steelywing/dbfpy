__author__ = 'Wing'

import locale
import unittest
import env
from dbfpy.code_page import CodePage


class CodePageTest(unittest.TestCase):
    def setUp(self):
        self.code_page = CodePage()

    def tearDown(self):
        del self.code_page

    def test_default_encoding(self):
        self.assertEqual(
            str(self.code_page),
            locale.getpreferredencoding()
        )

    def test_init(self):
        code_page = CodePage(0x4F)
        self.assertEqual(code_page.code_page, 0x4F)

        code_page = CodePage('cp950')
        self.assertEqual(code_page.encoding, 'cp950')

        with self.assertRaises(TypeError):
            CodePage(b'not support bytes')

    def test_encoding(self):
        self.code_page.code_page = 0x4F
        self.assertEqual(
            self.code_page.encoding,
            'cp950'
        )

        self.code_page.encoding = 'cp950'
        self.assertEqual(
            self.code_page.code_page,
            0x4F
        )

if __name__ == '__main__':
    unittest.main()
