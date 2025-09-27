# utils/create_csv.py
import os
import pandas as pd

OUTDIR = os.path.join(os.path.dirname(__file__), "..", "generated")
os.makedirs(OUTDIR, exist_ok=True)

def save(df, name):
    path = os.path.abspath(os.path.join(OUTDIR, name))
    df.to_csv(path, index=False, encoding="utf-8")
    print(path)
    return path

# 1) Vehicle classes
df_classes = pd.DataFrame({
    "vehicle_class": [
        "Class 1","Class 2","Class 3","Class 4","Class 5","Class 6","Class 7",
        "Motorcycle Small","Motorcycle Medium","Motorcycle Large","Tricycle"
    ]
})
save(df_classes, "vehicle_classes.csv")

# 2) Services price book (from your RJ AutoSpa table)
rows = []

def add_prices(service_name, prices_map, note=""):
    for vclass, price in prices_map.items():
        if price is None:  # skip blanks in the source table
            continue
        rows.append({
            "service": service_name,
            "vehicle_class": vclass,
            "price_peso": price,
            "note": note
        })

# Carwash
add_prices("Carwash", {
    "Class 1":170, "Class 2":190, "Class 3":210, "Class 4":230, "Class 5":270, "Class 6":300, "Class 7":350,
    "Motorcycle Small":120, "Motorcycle Medium":130, "Motorcycle Large":140, "Tricycle":140
})

# Bac to Zero
add_prices("Bac to Zero", {
    "Class 1":350, "Class 2":370, "Class 3":390, "Class 4":410, "Class 5":430, "Class 6":450, "Class 7":470
})

# Wax with buffing
add_prices("Wax with buffing", {
    "Class 1":600, "Class 2":700, "Class 3":750, "Class 4":800, "Class 5":900, "Class 6":1000, "Class 7":1050,
    "Motorcycle Small":400, "Motorcycle Medium":500, "Motorcycle Large":600, "Tricycle":400
})

# Engine Wash
add_prices("Engine Wash", {
    "Class 1":500, "Class 2":560, "Class 3":610, "Class 4":660, "Class 5":710, "Class 6":760, "Class 7":800
})

# Muffler Cleaning
add_prices("Muffler Cleaning", {
    "Class 1":500, "Class 2":550, "Class 3":550, "Class 4":650, "Class 5":650, "Class 6":750, "Class 7":750
})

# Armour all
add_prices("Armour all", {
    "Class 1":180, "Class 2":210, "Class 3":230, "Class 4":250, "Class 5":280, "Class 6":310, "Class 7":340,
    "Motorcycle Small":200, "Motorcycle Medium":200, "Motorcycle Large":300, "Tricycle":200
})

# Seat Cover Install
add_prices("Seat Cover Install", {
    "Class 1":140, "Class 2":160, "Class 3":180, "Class 4":180, "Class 5":200, "Class 6":200
})

# Seat Cover Removal
add_prices("Seat Cover Removal", {
    "Class 1":350, "Class 2":400, "Class 3":450, "Class 4":490, "Class 5":520, "Class 6":520
})

# Interior Detailing
add_prices("Interior Detailing", {
    "Class 1":3600, "Class 2":4000, "Class 3":4400, "Class 4":4800, "Class 5":5200, "Class 6":5600, "Class 7":5600
})

# Exterior Detailing
add_prices("Exterior Detailing", {
    "Class 1":4000, "Class 2":4500, "Class 3":5000, "Class 4":5500, "Class 5":6000, "Class 6":6500, "Class 7":7500
})

# Headlight Cleaning (flat for Classes 1–7)
for cl in ["Class 1","Class 2","Class 3","Class 4","Class 5","Class 6","Class 7"]:
    add_prices("Headlight Cleaning", {cl:600})

# Handwax Labor
add_prices("Handwax Labor", {
    "Class 1":300, "Class 2":300, "Class 3":350, "Class 4":350, "Class 5":400, "Class 6":400, "Class 7":450,
    "Motorcycle Small":200, "Motorcycle Medium":250, "Motorcycle Large":300
})

# Glass Restoration per panel (classes 1–7)
for cl in ["Class 1","Class 2","Class 3","Class 4","Class 5","Class 6","Class 7"]:
    add_prices("Glass Restoration", {cl:500}, note="per panel")

# Promos
add_prices("Bac to Zero Promo", {
    "Class 1":550, "Class 2":550, "Class 3":600, "Class 4":600, "Class 5":650, "Class 6":650, "Class 7":700
}, note="Includes Carwash + Vacuum + Armorall")

add_prices("Wax with Buffing Promo", {
    "Class 1":700, "Class 2":800, "Class 3":850, "Class 4":900, "Class 5":1000, "Class 6":1100, "Class 7":1150
}, note="Includes Carwash + Vacuum + Wax + Buffing + Armourall")

df_services = pd.DataFrame(rows).sort_values(["service","vehicle_class"]).reset_index(drop=True)
save(df_services, "services_rj_autospa.csv")

# 3) Commission policy (editable)
df_comm = pd.DataFrame([
    {"rule_id":"carwash_pool", "service_regex":"^Carwash$", "commission_type":"pool_split", "percent":30, "notes":"edit percent to change carwash pool"},
    {"rule_id":"promo_pool", "service_regex":"Promo$", "commission_type":"pool_split", "percent":30, "notes":"promos treated as pool - edit as needed"},
    {"rule_id":"bac_to_zero_direct", "service_regex":"^Bac to Zero$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"wax_buffing_direct", "service_regex":"^Wax with buffing$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"engine_wash_direct", "service_regex":"^Engine Wash$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"muffler_cleaning_direct", "service_regex":"^Muffler Cleaning$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"armour_all_direct", "service_regex":"^Armour all$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"seat_cover_install_direct", "service_regex":"^Seat Cover Install$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"seat_cover_removal_direct", "service_regex":"^Seat Cover Removal$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"interior_detailing_direct", "service_regex":"^Interior Detailing$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"exterior_detailing_direct", "service_regex":"^Exterior Detailing$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"headlight_cleaning_direct", "service_regex":"^Headlight Cleaning$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"handwax_labor_direct", "service_regex":"^Handwax Labor$", "commission_type":"direct", "percent":30, "notes":""},
    {"rule_id":"glass_restoration_direct", "service_regex":"^Glass Restoration$", "commission_type":"direct", "percent":30, "notes":"per panel"},
])
save(df_comm, "commission_policy_editable.csv")

# 4) Employees with overrides
df_emps = pd.DataFrame([
    {"employee_id":"E001","name":"Employee One","role":"Detailer","base_daily_salary":500,
     "base_daily_override":"","pin_hint":"1234","commission_multiplier_pool":"","commission_multiplier_direct":""},
    {"employee_id":"E002","name":"Employee Two","role":"Detailer","base_daily_salary":500,
     "base_daily_override":"","pin_hint":"2468","commission_multiplier_pool":"","commission_multiplier_direct":""},
    {"employee_id":"E003","name":"Team Lead","role":"TeamLead","base_daily_salary":600,
     "base_daily_override":"","pin_hint":"4321","commission_multiplier_pool":"","commission_multiplier_direct":""},
])
save(df_emps, "employees_overrides_template.csv")

# 5) Global payroll settings
df_settings = pd.DataFrame([
    {"key":"default_carwash_commission_percent", "value":30, "note":"% of Carwash revenue goes to pool"},
    {"key":"default_promo_commission_percent", "value":30, "note":"% of Promo revenue goes to pool"},
    {"key":"default_special_commission_percent", "value":30, "note":"% of special services revenue paid direct to performer"},
    {"key":"pool_split_services_regex", "value":"^(Carwash|.*Promo)$", "note":"Regex of services counted as pool_split"},
    {"key":"direct_services_regex", "value":"^(Bac to Zero|Wax with buffing|Engine Wash|Muffler Cleaning|Armour all|Seat Cover Install|Seat Cover Removal|Interior Detailing|Exterior Detailing|Headlight Cleaning|Handwax Labor|Glass Restoration)$", "note":"Regex of services paid direct"},
    {"key":"enable_role_multipliers", "value":"TRUE", "note":"If TRUE, apply role multipliers below"},
    {"key":"role_multiplier_pool_TeamLead", "value":1.1, "note":"TeamLead gets 10% more than normal on pool commissions"},
    {"key":"role_multiplier_direct_TeamLead", "value":1.1, "note":"TeamLead gets 10% more than normal on direct commissions"},
    {"key":"role_multiplier_pool_Detailer", "value":1.0, "note":"Default multiplier for Detailer (pool)"},
    {"key":"role_multiplier_direct_Detailer", "value":1.0, "note":"Default multiplier for Detailer (direct)"},
])
save(df_settings, "payroll_settings.csv")

# 6) Empty attendance & transactions templates
df_att = pd.DataFrame(columns=["timestamp_iso","shift_id","employee_id","action"])
save(df_att, "attendance_template.csv")

df_tx = pd.DataFrame(columns=[
    "timestamp_iso","shift_id","plate","vehicle_class","service","units","price_peso","amount_peso",
    "performed_by_employee_id","notes"
])
save(df_tx, "transactions_template.csv")

print("\nAll CSVs generated in:", os.path.abspath(OUTDIR))
