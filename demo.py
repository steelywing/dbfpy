from dbfpy import dbf

## create DBF

db = dbf.Dbf('new.dbf', new=True)
db.addField(
    ("NAME", "C", 15),
    ("SURNAME", "C", 25),
    ("INITIALS", "C", 10),
    ("BIRTHDATE", "D"),
)

for (_n, _s, _i, _b) in (
    ("John", "Miller", "YC", (1981, 1, 2)),
    ("Andy", "Larkin", "AL", (1982, 3, 4)),
    ("Bill", "Clinth", "", (1983, 5, 6)),
    ("Bobb", "McNail", "", (1984, 7, 8)),
):
    _rec = db.newRecord()
    _rec["NAME"] = _n
    _rec["SURNAME"] = _s
    _rec["INITIALS"] = _i
    _rec["BIRTHDATE"] = _b
    _rec.store()

print(repr(db), '\n\n')
db.close()

## read DBF

db = dbf.Dbf('table.dbf', True)
print(repr(db), '\n')
for record in db:
    print(repr(record), '\n')
db.close()
