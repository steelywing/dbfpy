from dbfpy import dbf

## create DBF

db = dbf.Dbf('new.dbf', new=True)
db.header.code_page = 0x7A
db.add_field(
    ("C", "NAME", 15),
    ("C", "SURNAME", 25),
    ("C", "INITIALS", 10),
    ("D", "BIRTHDATE"),
)

for (name, surname, initials, birthdate) in (
        ("John", "Miller", "YC", (1981, 1, 2)),
        ("Andy", "Larkin", "AL", (1982, 3, 4)),
        ("Bill", "Clinth", "简体", (1983, 5, 6)),
        ("Bobb", "McNail", "测试", (1984, 7, 8)),
):
    rec = db.new_record()
    rec["NAME"] = name
    rec["SURNAME"] = surname
    rec["INITIALS"] = initials
    rec["BIRTHDATE"] = birthdate
    db.write_record(rec)

print(db, '\n\n')
db.close()

## read DBF

db = dbf.Dbf('sc.dbf', read_only=True)
print(db, '\n')
for record in db:
    print(record, '\n')
db.close()
