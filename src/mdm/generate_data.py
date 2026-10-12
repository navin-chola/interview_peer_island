"""Regenerate the small synthetic customer input file."""
import csv
from pathlib import Path

ROWS = [
 ["CRM","C-100","  Asha Rao ","ASHA.RAO@example.com ","+91 98765 43210","12 MG Road","IN","2026-01-10 09:00:00"],
 ["BILLING","B-900","Asha Rao","asha.rao@example.com","9876543210","12, M.G. Road","IN","2026-02-02 10:00:00"],
 ["WEB","W-200","Rahul Menon","rahul@example.com","+1 202 555 0100","8 Lake View","US","2026-01-15 12:00:00"],
 ["CRM","C-200","RAHUL MENON","rahul.menon@example.com","12025550100","8 Lake View","US","2026-03-01 08:30:00"],
 ["WEB","W-301","","not-an-email","","Unknown","ZZ","2026-02-01 00:00:00"],
 ["BILLING","B-401","Meera Iyer","meera@example.com"," +91-99887-76655 ","44 Anna Salai","IN","2026-02-18 13:00:00"]
]

def main():
    path = Path("data/customers.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_system","source_record_id","name","email","phone","address","country_code","updated_at"])
        writer.writerows(ROWS)
    print(f"Wrote {len(ROWS)} synthetic records to {path}")

if __name__ == "__main__":
    main()
