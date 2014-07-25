__author__ = 'Steely Wing <steely.wing@gmail.com>'

import locale

# reference: http://webhelp.esri.com/arcpad/8.0/referenceguide/index.htm#locales/task_code.htm

code_pages = {
    0x01: 'cp437',  # U.S. MS-DOS
    0x02: 'cp850',  # International MS-DOS
    0x03: 'cp1252',  # Window ANSI
    0x08: 'cp865',  # Danish OEM
    0x09: 'cp437',  # Dutch OEM
    0x0A: 'cp850',  # Dutch OEM*
    0x0B: 'cp437',  # Finnish OEM
    0x0D: 'cp437',  # French OEM
    0x0E: 'cp850',  # French OEM*
    0x0F: 'cp437',  # German OEM
    0x10: 'cp850',  # German OEM*
    0x11: 'cp437',  # Italian OEM
    0x12: 'cp850',  # Italian OEM*
    0x13: 'cp932',  # Japanese Shift-JIS
    0x14: 'cp850',  # Spanish OEM*
    0x15: 'cp437',  # Swedish OEM
    0x16: 'cp850',  # Swedish OEM*
    0x17: 'cp865',  # Norwegian OEM
    0x18: 'cp437',  # Spanish OEM
    0x19: 'cp437',  # English OEM (Britain)
    0x1A: 'cp850',  # English OEM (Britain)*
    0x1B: 'cp437',  # English OEM (U.S.)
    0x1C: 'cp863',  # French OEM (Canada)
    0x1D: 'cp850',  # French OEM*
    0x1F: 'cp852',  # Czech OEM
    0x22: 'cp852',  # Hungarian OEM
    0x23: 'cp852',  # Polish OEM
    0x24: 'cp860',  # Portugese OEM
    0x25: 'cp850',  # Potugese OEM*
    0x26: 'cp866',  # Russian OEM
    0x37: 'cp850',  # English OEM (U.S.)*
    0x40: 'cp852',  # Romanian OEM
    0x4D: 'cp936',  # Chinese GBK (PRC)
    0x4E: 'cp949',  # Korean (ANSI/OEM)
    0x4F: 'cp950',  # Chinese Big 5 (Taiwan)
    0x50: 'cp874',  # Thai (ANSI/OEM)
    0x57: 'cp1252',  # ANSI
    0x58: 'cp1252',  # Western European ANSI
    0x59: 'cp1252',  # Spanish ANSI
    0x64: 'cp852',  # Eastern European MS-DOS
    0x65: 'cp866',  # Russian MS-DOS
    0x66: 'cp865',  # Nordic MS-DOS
    0x67: 'cp861',  # Icelandic MS-DOS
    0x6A: 'cp737',  # Greek MS-DOS (437G)
    0x6B: 'cp857',  # Turkish MS-DOS
    0x6C: 'cp863',  # French-Canadian MS-DOS
    0x78: 'cp950',  # Taiwan Big 5
    0x79: 'cp949',  # Hangul (Wansung)
    0x7A: 'cp936',  # PRC GBK
    0x7B: 'cp932',  # Japanese Shift-JIS
    0x7C: 'cp874',  # Thai Windows/MS-DOS
    0x86: 'cp737',  # Greek OEM
    0x87: 'cp852',  # Slovenian OEM
    0x88: 'cp857',  # Turkish OEM
    0xC8: 'cp1250',  # Eastern European Windows
    0xC9: 'cp1251',  # Russian Windows
    0xCA: 'cp1254',  # Turkish Windows
    0xCB: 'cp1253',  # Greek Windows
    0xCC: 'cp1257',  # Baltic Windows
}


class CodePage:
    code_page = 0

    def __init__(self, code=None):
        """
        code:
            code page (int), encoding (string) or this class
        """
        if code is None:
            # default use system encoding
            self.encoding = locale.getpreferredencoding()
        elif isinstance(code, int):
            self.code_page = code
        elif isinstance(code, str):
            self.encoding = code
        else:
            raise TypeError('unsupported code page type ({0})'.format(type(code)))

    @property
    def encoding(self):
        """return encoding name, if code_page not in list, return system encoding"""
        if self.code_page in code_pages:
            return code_pages[self.code_page]
        else:
            return locale.getpreferredencoding()

    @encoding.setter
    def encoding(self, target_encoding):
        for code_page, encoding in code_pages.items():
            if encoding == target_encoding:
                self.code_page = code_page
                break
        else:
            self.code_page = 0

    def __str__(self):
        return self.encoding
