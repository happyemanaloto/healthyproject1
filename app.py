import re
import json
import time
from datetime import datetime, timedelta, date
import pandas as pd
import streamlit as st
import uuid

# --- Google Sheets (gspread) ---
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ======= CONFIG =======
SHEET_NAME             = st.secrets["sheets"]["workbook_name"]    # e.g. "RJ_AutoSpa_Payroll"
TAB_SERVICES           = "services"
TAB_VEHICLE_CLASSES    = "vehicle_classes"
TAB_VEHICLE_MODELS     = "vehicle_models"  # NEW: model->class mapping
TAB_EMPLOYEES          = "employees"
TAB_COMMISSION_POLICY  = "commission_policy"
TAB_ATTENDANCE         = "attendance"
TAB_TRANSACTIONS       = "transactions"
TAB_PAYROLL_EXPORTS    = "payroll_exports"  # optional archive tab (append-only)

# Transactions expected columns (supports multi-service visits)
TX_COLS = [
    "timestamp_iso","shift_id","visit_id","branch_id","plate","vehicle_model","vehicle_class","service","units",
    "price_peso","amount_peso","amount_paid_peso","payment_method",
    "performed_by_employee_id","customer_name","customer_phone","notes"
]

# Add near TX_COLS
ATT_COLS = ["timestamp_iso","shift_id","branch_id","employee_id","action"]

def ensure_att_columns(df):
    return ensure_columns(df, ATT_COLS)


# 15-day windows: 1–15 and 16–end of month
def current_pay_window(dt: date):
    if dt.day <= 15:
        start = dt.replace(day=1)
        end   = dt.replace(day=15)
    else:
        start = dt.replace(day=16)
        next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month - timedelta(days=1)
    return start, end

# ======= AUTH =======
def get_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client

@st.cache_data(ttl=30)
def load_sheet(sheet_name, tab):
    client = get_client()
    sh = client.open(sheet_name)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=2000, cols=26)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    return df

def write_df(sheet_name, tab, df: pd.DataFrame):
    client = get_client()
    sh = client.open(sheet_name)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=2000, cols=26)
    ws.clear()
    if df.empty:
        ws.update("A1", [list(df.columns)])
    else:
        ws.update([list(df.columns)] + df.astype(object).values.tolist())

# ======= SEED: vehicle_models (brand, model, label, class) =======
VEHICLE_MODELS_SEED = [
    # CLASS 1
    ("TOYOTA","Corolla","TOYOTA - Corolla","Class 1"),
    ("TOYOTA","Altis","TOYOTA - Altis","Class 1"),
    ("TOYOTA","Vios","TOYOTA - Vios","Class 1"),
    ("TOYOTA","Camry","TOYOTA - Camry","Class 1"),
    ("TOYOTA","Echo","TOYOTA - Echo","Class 1"),
    ("TOYOTA","Yaris","TOYOTA - Yaris","Class 1"),
    ("TOYOTA","Matrix","TOYOTA - Matrix","Class 1"),
    ("TOYOTA","Prius","TOYOTA - Prius","Class 1"),
    ("TOYOTA","Veneza","TOYOTA - Veneza","Class 1"),
    ("TOYOTA","Avalon","TOYOTA - Avalon","Class 1"),
    ("TOYOTA","Wigo","TOYOTA - Wigo","Class 1"),
    ("VOLKSWAGEN","Jetta","Volkswagen - Jetta","Class 1"),
    ("VOLKSWAGEN","Golf","Volkswagen - Golf","Class 1"),
    ("MITSUBISHI","Lancer","MITSUBISHI - Lancer","Class 1"),
    ("MITSUBISHI","Galant","MITSUBISHI - Galant","Class 1"),
    ("MITSUBISHI","Eclipse","MITSUBISHI - Eclipse","Class 1"),
    ("MITSUBISHI","Mirage","MITSUBISHI - Mirage","Class 1"),
    ("HONDA","Accord","HONDA - Accord","Class 1"),
    ("HONDA","Civic","HONDA - Civic","Class 1"),
    ("HONDA","Citi","HONDA - Citi","Class 1"),
    ("HONDA","Jazz","HONDA - Jazz","Class 1"),
    ("KIA","Picanto","KIA - Picanto","Class 1"),
    ("SUZUKI","Swift","SUZUKI - Swift","Class 1"),
    ("FORD","Lynx","FORD - Lynx","Class 1"),
    ("AUDI","A4","AUDI - A4","Class 1"),
    # Brand-wide sedans → Class 1 (catch-all labels)
    ("BMW","Any Sedan","BMW - All sedans","Class 1"),
    ("VOLVO","Any Sedan","VOLVO - All sedans","Class 1"),
    ("NISSAN","Any Sedan","NISSAN - All sedans","Class 1"),
    ("HYUNDAI","Accent/Any Sedan","HYUNDAI - Accent (all sedans)","Class 1"),
    ("MAZDA","Any Sedan","MAZDA - All sedans","Class 1"),
    ("TAXI","Generic","TAXI - Any (100 Carwash & Vacuum)","Class 1"),

    # CLASS 2
    ("TOYOTA","Rav4","TOYOTA - Rav4","Class 2"),
    ("TOYOTA","Corolla Cross","TOYOTA - Corolla Cross","Class 2"),
    ("TOYOTA","Avanza (Old Model)","TOYOTA - Avanza (Old)","Class 2"),
    ("TOYOTA","Raize","TOYOTA - Raize","Class 2"),
    ("KIA","Stonic","Kia - Stonic","Class 2"),
    ("KIA","Soul","Kia - Soul","Class 2"),
    ("SUBARU","Legacy","Subaru - Legacy","Class 2"),
    ("SUBARU","Forester","Subaru - Forester","Class 2"),
    ("SUZUKI","Vitara","SUZUKI - Vitara","Class 2"),
    ("SUZUKI","Ertiga","SUZUKI - Ertiga","Class 2"),
    ("BMW","X3","BMW - X3","Class 2"),
    ("FORD","Escape","FORD - Escape","Class 2"),
    ("FORD","Ecosport","FORD - Ecosport","Class 2"),
    ("FORD","Territory","FORD - Territory","Class 2"),
    ("GEELY","Coolray","Geely - Coolray","Class 2"),
    ("MAZDA","Tribute","MAZDA - Tribute","Class 2"),
    ("MAZDA","CX3","MAZDA - CX3","Class 2"),
    ("MAZDA","CX5","MAZDA - CX5","Class 2"),
    ("NISSAN","Kicks","Nissan - Kicks","Class 2"),
    ("NISSAN","Juke","Nissan - Juke","Class 2"),
    ("NISSAN","Xtrail","Nissan - Xtrail","Class 2"),
    ("HONDA","CRV","HONDA - CRV","Class 2"),
    ("HONDA","BRV","HONDA - BRV","Class 2"),
    ("HYUNDAI","Tucson","Hyundai - Tucson","Class 2"),
    ("HYUNDAI","Creta","Hyundai - Creta","Class 2"),
    ("HYUNDAI","Kona","Hyundai - Kona","Class 2"),
    ("CHEVROLET","Spin","Chevrolet - Spin","Class 2"),
    ("CHERY","Tiggo","Chery - Tiggo","Class 2"),
    ("MG","ZS/HS","MG - ZS/HS","Class 2"),
    ("CHANGAN","CS35","Changan - CS35","Class 2"),

    # CLASS 3
    ("TOYOTA","Innova","TOYOTA - Innova","Class 3"),
    ("TOYOTA","Revo","TOYOTA - Revo","Class 3"),
    ("TOYOTA","Lite Ace","TOYOTA - Lite Ace","Class 3"),
    ("TOYOTA","Rush","TOYOTA - Rush","Class 3"),
    ("TOYOTA","Avanza (New Model)","TOYOTA - Avanza (New)","Class 3"),
    ("MAZDA","CX8","MAZDA - CX8","Class 3"),
    ("MAZDA","CX9","MAZDA - CX9","Class 3"),
    ("MITSUBISHI","L200","MITSUBISHI - L200","Class 3"),
    ("MITSUBISHI","Adventure","MITSUBISHI - Adventure","Class 3"),
    ("MITSUBISHI","Highlander","MITSUBISHI - Highlander","Class 3"),
    ("MITSUBISHI","Outlander","MITSUBISHI - Outlander","Class 3"),
    ("MITSUBISHI","Xpander","MITSUBISHI - Xpander","Class 3"),
    ("JEEP","Cherokee","JEEP - Cherokee","Class 3"),
    ("NISSAN","X-Trail","NISSAN - X-Trail","Class 3"),
    ("NISSAN","Terrano","NISSAN - Terrano","Class 3"),
    ("NISSAN","Vanette","NISSAN - Vanette","Class 3"),
    ("HONDA","CRV (New Model)","HONDA - CRV (New)","Class 3"),
    ("ISUZU","Sportivo","ISUZU - Sportivo","Class 3"),
    ("ISUZU","Crosswind","ISUZU - Crosswind","Class 3"),
    ("HYUNDAI","Stargazer","HYUNDAI - Stargazer","Class 3"),
    ("HYUNDAI","Santa Fe","HYUNDAI - Santa Fe","Class 3"),
    ("CHEVROLET","Captiva","CHEVROLET - Captiva","Class 3"),
    ("SUZUKI","MPV","SUZUKI - MPV","Class 3"),

    # CLASS 4
    ("TOYOTA","Hilux","TOYOTA - Hilux","Class 4"),
    ("TOYOTA","Fortuner","TOYOTA - Fortuner","Class 4"),
    ("TOYOTA","4 Runner","TOYOTA - 4 Runner","Class 4"),
    ("TOYOTA","Hi-Ace","TOYOTA - Hi-Ace","Class 4"),
    ("ISUZU","MUX","ISUZU - MUX","Class 4"),
    ("MITSUBISHI","Strada","MITSUBISHI - Strada","Class 4"),
    ("MITSUBISHI","Space Gear","MITSUBISHI - Space Gear","Class 4"),
    ("MITSUBISHI","Grandis","MITSUBISHI - Grandis","Class 4"),
    ("MITSUBISHI","Montero","MITSUBISHI - Montero","Class 4"),
    ("NISSAN","Terra","NISSAN - Terra","Class 4"),
    ("NISSAN","Navara","NISSAN - Navara","Class 4"),
    ("FORD","Everest","FORD - Everest","Class 4"),
    ("JEEP","Cherokee","JEEP - Cherokee","Class 4"),
    ("KIA","Carnival","KIA - Carnival","Class 4"),
    ("CHEVROLET","Trail Blazer","CHEVROLET - Trail Blazer","Class 4"),
    ("SUBARU","Forester","SUBARU - Forester","Class 4"),

    # CLASS 5
    ("TOYOTA","FJ Cruiser","TOYOTA - FJ Cruiser","Class 5"),
    ("TOYOTA","Sequoia","TOYOTA - Sequoia","Class 5"),
    ("TOYOTA","Land Cruiser","TOYOTA - Land Cruiser","Class 5"),
    ("TOYOTA","Tacoma","TOYOTA - Tacoma","Class 5"),
    ("TOYOTA","Tundra","TOYOTA - Tundra","Class 5"),
    ("MITSUBISHI","Pajero","MITSUBISHI - Pajero","Class 5"),
    ("MITSUBISHI","L300","MITSUBISHI - L300","Class 5"),
    ("NISSAN","Patrol","NISSAN - Patrol","Class 5"),
    ("FORD","Explorer","FORD - Explorer","Class 5"),
    ("FORD","F150","FORD - F150","Class 5"),
    ("ISUZU","Trooper","ISUZU - Trooper","Class 5"),
    ("ISUZU","Dmax Pick-up","ISUZU - Dmax Pick-up","Class 5"),
    ("HYUNDAI","Starex","HYUNDAI - Starex","Class 5"),
    ("BMW","X5","BMW - X5","Class 5"),
    ("FORD","Ranger/Raptor/Everest(New)","Ford - Ranger & Ranger Raptor, Everest (New)","Class 5"),

    # CLASS 6
    ("TOYOTA","LC Prado","TOYOTA - LC Prado","Class 6"),
    ("TOYOTA","Grandia","TOYOTA - Grandia","Class 6"),
    ("TOYOTA","Hi-Ace Grandia","TOYOTA - Hi-Ace Grandia","Class 6"),
    ("FORD","Explorer Van","FORD - Explorer Van","Class 6"),
    ("NISSAN","Urvan","NISSAN - Urvan","Class 6"),
    ("CHEVROLET","Silverado","CHEVROLET - Silverado","Class 6"),
    ("MERCEDES BENZ","MB100","MERCEDES BENZ - MB100","Class 6"),
    ("LINCOLN","Navigator","LINCOLN - Navigator","Class 6"),
    ("HYUNDAI","Grand Starex","HYUNDAI - Grand Starex","Class 6"),

    # CLASS 7
    ("TOYOTA","Super Grandia","TOYOTA - Super Grandia","Class 7"),
]

def ensure_vehicle_models_sheet():
    vm = load_sheet(SHEET_NAME, TAB_VEHICLE_MODELS)
    if vm.empty or not set(["brand","model","label","vehicle_class"]).issubset(set(vm.columns)):
        df = pd.DataFrame(VEHICLE_MODELS_SEED, columns=["brand","model","label","vehicle_class"])
        write_df(SHEET_NAME, TAB_VEHICLE_MODELS, df)
        vm = df
    return vm

# ======= BUSINESS LOGIC =======
@st.cache_data(ttl=30)
def load_catalog():
    services = load_sheet(SHEET_NAME, TAB_SERVICES)
    classes  = load_sheet(SHEET_NAME, TAB_VEHICLE_CLASSES)
    policy   = load_sheet(SHEET_NAME, TAB_COMMISSION_POLICY)
    emps     = load_sheet(SHEET_NAME, TAB_EMPLOYEES)
    vmodels  = ensure_vehicle_models_sheet()
    return services, classes, policy, emps, vmodels

def match_commission_rule(service_name, policy_df, branch_id):
    """
    Return (commission_type, percent) for the first matching regex rule for this branch.
    If no branch-specific rule matches, fall back to rules where branch_id is blank/NaN.
    """
    # 1) Prefer branch-specific rules
    pdf = policy_df.copy()
    # normalize branch_id column
    if "branch_id" not in pdf.columns:
        pdf["branch_id"] = ""
    # try branch rows first
    branch_rows = pdf[pdf["branch_id"].astype(str).str.upper() == str(branch_id).upper()]
    for _, r in branch_rows.iterrows():
        pattern = str(r.get("service_regex") or "")
        try:
            if re.search(pattern, service_name, flags=re.IGNORECASE):
                return r.get("commission_type"), float(r.get("percent", 0) or 0)
        except re.error:
            continue
    # 2) fallback: global rules (blank branch)
    global_rows = pdf[(pdf["branch_id"]=="") | (pdf["branch_id"].isna())]
    for _, r in global_rows.iterrows():
        pattern = str(r.get("service_regex") or "")
        try:
            if re.search(pattern, service_name, flags=re.IGNORECASE):
                return r.get("commission_type"), float(r.get("percent", 0) or 0)
        except re.error:
            continue
    return None, 0.0


def get_shift_id(ts=None):
    ts = ts or datetime.now()
    hour = ts.hour
    shift = "Day" if 6 <= hour < 18 else "Night"
    return f"{ts.date()}_{shift}"

def ensure_columns(df, cols, fill_value=""):
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
    if not set(cols).issubset(df.columns):
        return df
    return df[cols]



def ensure_tx_columns(df):
    df = ensure_columns(df, TX_COLS)
    return df[TX_COLS]  # reorder columns


def record_attendance(employee_id, action, branch_id):
    att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    att = ensure_columns(att, ["timestamp_iso","shift_id","employee_id","action","branch_id"])
    row = {
        "timestamp_iso": datetime.now().isoformat(timespec="seconds"),
        "shift_id": get_shift_id(),
        "employee_id": employee_id,
        "action": action,
        "branch_id": branch_id,
    }
    att = pd.concat([att, pd.DataFrame([row])], ignore_index=True)
    write_df(SHEET_NAME, TAB_ATTENDANCE, att)



def record_transaction_rows(rows):
    """Append multiple rows (one visit with many services)."""
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    tx = ensure_tx_columns(tx)
    tx = pd.concat([tx, pd.DataFrame(rows)], ignore_index=True)
    write_df(SHEET_NAME, TAB_TRANSACTIONS, tx)

def who_is_clocked_in(att_df, shift_id, branch_id):
    # Be tolerant of old rows without branch_id: use them as "wildcard"
    att_df = att_df.copy()
    if "branch_id" not in att_df.columns:
        att_df["branch_id"] = ""

    att_shift = att_df[att_df["shift_id"] == shift_id].sort_values("timestamp_iso")
    # prefer exact-branch rows; if none exist, fall back to blank-branch rows
    cand = att_shift[att_shift["branch_id"].astype(str).str.upper() == str(branch_id).upper()]
    if cand.empty:
        cand = att_shift[att_shift["branch_id"] == ""]  # legacy entries without branch

    status = {}
    for _, r in cand.iterrows():
        eid = r["employee_id"]
        if r["action"] == "CLOCK_IN":
            status[eid] = True
        elif r["action"] == "CLOCK_OUT":
            status[eid] = False
    return [eid for eid, on in status.items() if on]


def compute_commissions(start_date, end_date, branch_filter: str | None = None):
    """
    Compute payroll using commission_policy rules.
    branch_filter: None/"ALL" for company-wide, or "B1"/"B2" to scope by branch.
    """
    B2_SHIFT_BASE_PESO = 500.0  # fixed base per shift at B2

    services, classes, policy, emps, vmodels = load_catalog()

    # ---- Load transactions in window (and scope if requested)
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    if tx.empty:
        return pd.DataFrame(), pd.DataFrame()
    tx = ensure_tx_columns(tx).copy()
    if "branch_id" not in tx.columns:
        tx["branch_id"] = "B1"
    tx["branch_id"] = tx["branch_id"].astype(str).str.upper()
    tx["performed_by_employee_id"] = tx["performed_by_employee_id"].astype(str).fillna("")
    tx["timestamp"] = pd.to_datetime(tx["timestamp_iso"], errors="coerce")
    mask = (tx["timestamp"].dt.date >= start_date) & (tx["timestamp"].dt.date <= end_date)
    tx = tx[mask].copy()
    if branch_filter and branch_filter.upper() != "ALL":
        tx = tx[tx["branch_id"] == branch_filter.upper()].copy()

    if tx.empty:
        return pd.DataFrame(), pd.DataFrame()

    # ---- Price map
    price_map = {(r["service"], r["vehicle_class"]): float(r["price_peso"]) for _, r in services.iterrows()}

    # ---- Commission pass driven by policy
    comm_rows = []
    pool_by_key = {}  # (branch_id, shift_id) -> peso pool

    # ensure policy has branch_id col
    policy = policy.copy()
    if "branch_id" not in policy.columns:
        policy["branch_id"] = ""

    for _, t in tx.iterrows():
        branch_id = t["branch_id"]
        service   = t["service"]
        vclass    = t["vehicle_class"]
        units     = float(t.get("units", 1) or 1)
        price     = price_map.get((service, vclass), float(t.get("price_peso", 0) or 0))
        amount    = price * units
        shift_id  = t["shift_id"]
        performer = str(t.get("performed_by_employee_id") or "")

        # look up commission rule from sheet (branch-specific first, then global)
        ctype, pct = match_commission_rule(service, policy, branch_id)

        if ctype == "direct":
            if performer:
                comm_rows.append({
                    "branch_id": branch_id, "shift_id": shift_id, "employee_id": performer,
                    "service": service, "vehicle_class": vclass, "commission_type": "direct",
                    "percent": pct, "base_amount": amount,
                    "commission_peso": amount * (float(pct) / 100.0)
                })
            else:
                # no performer recorded -> safest is to add to pool for this shift
                pool_by_key[(branch_id, shift_id)] = pool_by_key.get((branch_id, shift_id), 0.0) + amount * (float(pct) / 100.0)

        elif ctype == "pool_split":
            pool_by_key[(branch_id, shift_id)] = pool_by_key.get((branch_id, shift_id), 0.0) + amount * (float(pct) / 100.0)

        else:
            # no matching rule -> no commission
            pass

    # ---- 2) split pools by attendance per branch+shift,
    #         but only to people who are active AND performed at least one line.
    comm_df = pd.DataFrame(comm_rows)

    if pool_by_key:
        # Build performers set per (branch, shift) from transactions
        perf_by_key = (
            tx[tx["performed_by_employee_id"] != ""]
            .groupby(["branch_id","shift_id"])["performed_by_employee_id"]
            .apply(lambda s: set(map(str, s)))
            .to_dict()
        )

        # Attendance (may be empty)
        att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
        if not att.empty:
            att = ensure_columns(att, ["timestamp_iso","shift_id","employee_id","action","branch_id"]).copy()
            att["timestamp"]   = pd.to_datetime(att["timestamp_iso"], errors="coerce")
            att["branch_id"]   = att["branch_id"].astype(str).str.upper()
            att["employee_id"] = att["employee_id"].astype(str)
            # Optional scope
            if branch_filter and branch_filter.upper() != "ALL":
                att = att[att["branch_id"] == branch_filter.upper()]

        for (branch_id, shift_id), pool_amt in pool_by_key.items():
            active = set()
            if not att.empty:
                att_shift = att[(att["shift_id"] == shift_id) & (att["branch_id"] == branch_id)].sort_values("timestamp")
                state = {}
                for _, r in att_shift.iterrows():
                    eid = r["employee_id"]
                    if r["action"] == "CLOCK_IN":
                        state[eid] = True
                    elif r["action"] == "CLOCK_OUT":
                        state[eid] = False
                active = {eid for eid, on in state.items() if on}

            performers = perf_by_key.get((branch_id, shift_id), set())

            # NEW rule: split to intersection first
            participants = sorted(active & performers) if (active and performers and (active & performers)) else []

            # Fallbacks: performers-only, then active-only, then UNASSIGNED
            if not participants:
                participants = sorted(performers) if performers else sorted(active)

            if not participants:
                # keep ledger balanced even if totally empty
                comm_df = pd.concat([comm_df, pd.DataFrame([{
                    "branch_id": branch_id, "shift_id": shift_id, "employee_id": "UNASSIGNED",
                    "service": "POOL_SPLIT", "vehicle_class": "", "commission_type": "pool_split",
                    "percent": None, "base_amount": pool_amt, "commission_peso": pool_amt
                }])], ignore_index=True)
            else:
                share = pool_amt / len(participants)
                add = pd.DataFrame([{
                    "branch_id": branch_id, "shift_id": shift_id, "employee_id": eid,
                    "service": "POOL_SPLIT", "vehicle_class": "", "commission_type": "pool_split",
                    "percent": None, "base_amount": pool_amt, "commission_peso": share
                } for eid in participants])
                comm_df = pd.concat([comm_df, add], ignore_index=True)

    # If we built across ALL but user asked for a specific branch, filter ledger now too
    if branch_filter and branch_filter.upper() != "ALL" and not comm_df.empty:
        comm_df = comm_df[comm_df["branch_id"].astype(str).str.upper() == branch_filter.upper()].copy()

    # ---- Base pay (days present and B2 base)
    emps2 = load_sheet(SHEET_NAME, TAB_EMPLOYEES).copy()
    for c in ["employee_id","name","role","base_daily_salary"]:
        if c not in emps2.columns:
            emps2[c] = 0 if c == "base_daily_salary" else ""
    emps2["base_daily_salary"] = pd.to_numeric(emps2["base_daily_salary"], errors="coerce").fillna(0)

    att_all = load_sheet(SHEET_NAME, TAB_ATTENDANCE).copy()
    if att_all.empty:
        days_present = pd.DataFrame(columns=["employee_id","days_present_branch"])
        b2_shifts    = pd.DataFrame(columns=["employee_id","b2_shifts"])
    else:
        att_all = ensure_columns(att_all, ["timestamp_iso","shift_id","employee_id","action","branch_id"])
        att_all["timestamp"] = pd.to_datetime(att_all["timestamp_iso"], errors="coerce")
        att_all["branch_id"] = att_all["branch_id"].astype(str).str.upper()
        mask2 = (att_all["timestamp"].dt.date >= start_date) & (att_all["timestamp"].dt.date <= end_date)
        att_all = att_all[mask2].copy()

        # scope attendance to branch if requested (for base_daily_salary day counting)
        if branch_filter and branch_filter.upper() != "ALL":
            att_scope = att_all[(att_all["action"] == "CLOCK_IN") & (att_all["branch_id"] == branch_filter.upper())]
        else:
            att_scope = att_all[att_all["action"] == "CLOCK_IN"]

        if att_scope.empty:
            days_present = pd.DataFrame(columns=["employee_id","days_present_branch"])
        else:
            att_scope["date"] = att_scope["timestamp"].dt.date
            days_present = att_scope.groupby(["employee_id","date"]).size().reset_index()
            days_present = days_present.groupby("employee_id").size().rename("days_present_branch").reset_index()

        # B2 shift base: count unique (employee_id, shift_id) clock-ins at B2
        if branch_filter and branch_filter.upper() == "B1":
            b2_shifts = pd.DataFrame(columns=["employee_id","b2_shifts"])
        else:
            b2_only = att_all[(att_all["action"] == "CLOCK_IN") & (att_all["branch_id"] == "B2")]
            if b2_only.empty:
                b2_shifts = pd.DataFrame(columns=["employee_id","b2_shifts"])
            else:
                b2_shifts = b2_only.groupby(["employee_id","shift_id"]).size().reset_index().groupby("employee_id").size()
                b2_shifts = b2_shifts.rename("b2_shifts").reset_index()

    payroll = days_present.merge(emps2[["employee_id","name","role","base_daily_salary"]], on="employee_id", how="left")
    if payroll.empty:
        payroll = pd.DataFrame(columns=["employee_id","name","role","base_daily_salary","days_present_branch"])
    payroll["base_pay_peso"] = payroll["base_daily_salary"] * payroll["days_present_branch"]

    # add B2 base
    payroll = payroll.merge(b2_shifts, on="employee_id", how="left")
    payroll["b2_shifts"] = pd.to_numeric(payroll["b2_shifts"], errors="coerce").fillna(0).astype(int)
    payroll["b2_shift_base_peso"] = payroll["b2_shifts"] * B2_SHIFT_BASE_PESO

    # commissions
    if not comm_df.empty:
        comm_sum = comm_df.groupby("employee_id")["commission_peso"].sum().rename("commission_peso").reset_index()
        payroll = payroll.merge(comm_sum, on="employee_id", how="left")
    else:
        payroll["commission_peso"] = 0.0
    payroll["commission_peso"] = payroll["commission_peso"].fillna(0.0)

    payroll["total_peso"] = (
        payroll["base_pay_peso"].fillna(0.0)
        + payroll["b2_shift_base_peso"].fillna(0.0)
        + payroll["commission_peso"].fillna(0.0)
    )

    payroll["period_start"] = start_date.isoformat()
    payroll["period_end"]   = end_date.isoformat()
    payroll["branch_scope"] = (branch_filter or "ALL").upper()

    order = ["employee_id","days_present_branch","name","role","base_daily_salary",
             "base_pay_peso","b2_shifts","b2_shift_base_peso",
             "commission_peso","total_peso","branch_scope","period_start","period_end"]
    payroll = payroll[[c for c in order if c in payroll.columns]]

    if not comm_df.empty:
        comm_df = comm_df.sort_values(["branch_id","shift_id","employee_id","service"])

    return payroll.sort_values(["employee_id"]), comm_df



def active_employees_for(branch_id: str, shift_id: str):
    att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    if att.empty:
        return []
    att = ensure_att_columns(att).copy()
    att["employee_id"] = att["employee_id"].astype(str)
    att["branch_id"] = att["branch_id"].astype(str).fillna("")

    # 1) exact branch match for this shift
    rows = att[(att["shift_id"] == shift_id) & (att["branch_id"].str.upper() == branch_id.upper())]
    # 2) fallback: legacy rows with blank branch_id for this shift
    if rows.empty:
        rows = att[(att["shift_id"] == shift_id) & (att["branch_id"] == "")]

    rows = rows.sort_values("timestamp_iso")
    state = {}
    for _, r in rows.iterrows():
        eid = r["employee_id"]
        if r["action"] == "CLOCK_IN":
            state[eid] = True
        elif r["action"] == "CLOCK_OUT":
            state[eid] = False
    return [eid for eid, on in state.items() if on]


def log_visit_ui(branch_id: str):
    """
    Log a Visit for a specific branch.
    - Requires an assignee per service line.
    - Saves one row per service in `transactions` with branch_id.
    """
    st.subheader(f"🧾 Log a Visit — {branch_id}")
    services_df, classes_df, policy_df, emps_df, vmodels_df = load_catalog()

    # Make sure ids are strings
    if not emps_df.empty and "employee_id" in emps_df.columns:
        emps_df["employee_id"] = emps_df["employee_id"].astype(str)

    # Build the performer list once (attendance-based)
    current_shift = get_shift_id()
    active_now = active_employees_for(branch_id, current_shift)  # uses attendance.branch_id

    show_only_active = st.toggle(
        "Show only clocked-in staff for this branch",
        value=True, key=f"toggle_{branch_id}"
    )

    if show_only_active and active_now:
        emps_for_picker = emps_df[emps_df["employee_id"].isin(active_now)].copy()
    else:
        emps_for_picker = emps_df.copy()

    # If somehow empty, fall back to all employees so the form remains usable
    if emps_for_picker.empty:
        emps_for_picker = emps_df.copy()

    st.caption(
        f"Current shift: `{current_shift}` • Active @ {branch_id}: "
        f"{', '.join(active_now) if active_now else 'none'}"
    )

    # --- Vehicle model selector ---
    vmodels_df = vmodels_df.copy()
    vmodels_df["label"] = vmodels_df["label"].astype(str)
    vehicle_label = st.selectbox(
        "Vehicle model (search by name)",
        options=sorted(vmodels_df["label"].unique().tolist()),
        key=f"vehicle_label_{branch_id}"
    )
    vehicle_class = vmodels_df.set_index("label").loc[vehicle_label, "vehicle_class"]
    st.caption(f"Detected vehicle class: **{vehicle_class}**")

    # Visit-level fields
    c0, c1, c2, c3 = st.columns(4)
    with c0: plate = st.text_input("Plate (optional)", key=f"plate_{branch_id}")
    with c1: customer_name = st.text_input("Customer name (optional)", key=f"cname_{branch_id}")
    with c2: customer_phone = st.text_input("Customer phone (optional)", key=f"cphone_{branch_id}")
    with c3: payment_method = st.selectbox("Payment method", ["", "cash", "gcash", "card", "other"], key=f"pm_{branch_id}")
    amount_paid = st.number_input("Amount paid (₱) — per visit (total)", min_value=0.0, value=0.0, step=10.0, key=f"paid_{branch_id}")

    # Services
    st.markdown("### Select services")
    all_services = sorted(services_df["service"].unique().tolist())
    selected_services = st.multiselect("Services included in this visit", options=all_services, key=f"svcsel_{branch_id}")

    per_line_inputs, services_total = [], 0.0
    for svc in selected_services:
        with st.expander(f"{svc}", expanded=True):
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                units = st.number_input(f"{svc} — Units", min_value=1.0, value=1.0, step=1.0, key=f"units_{branch_id}_{svc}")
            with c2:
                price_row = services_df[(services_df["service"] == svc) & (services_df["vehicle_class"] == vehicle_class)]
                if price_row.empty:
                    st.warning("No price found for this model's class; enter manually.")
                    price = st.number_input(f"{svc} — Price (₱)", min_value=0.0, step=10.0, value=0.0, key=f"price_{branch_id}_{svc}")
                else:
                    price = float(price_row.iloc[0]["price_peso"])
                    st.write(f"Price (₱): **{price:,.2f}**")
            with c3:
                performer = st.selectbox(
                    f"{svc} — Performed by (required)",
                    options=emps_for_picker["employee_id"].tolist(),
                    format_func=lambda x: f"{x} — {emps_for_picker.set_index('employee_id').loc[x, 'name']}",
                    key=f"perf_{branch_id}_{svc}"
                )

            notes = st.text_input(f"{svc} — Notes (optional)", key=f"notes_{branch_id}_{svc}")
            line_total = float(price) * float(units)
            services_total += line_total
            st.caption(f"Line total: ₱{line_total:,.2f}")

            per_line_inputs.append({
                "service": svc,
                "units": float(units),
                "price_peso": float(price),
                "amount_peso": float(line_total),
                "performed_by_employee_id": performer,
                "notes": notes
            })

    st.metric("Services total (₱)", f"{services_total:,.2f}")
    st.metric("Change (₱)", f"{(amount_paid - services_total):,.2f}")

    # Save
    if st.button(f"🧾 Save visit — {branch_id}", type="primary", disabled=(len(per_line_inputs) == 0), key=f"save_{branch_id}"):
        missing = [li["service"] for li in per_line_inputs if not li["performed_by_employee_id"]]
        if missing:
            st.error("Every service must have an assigned employee.")
            st.stop()

        now_iso = datetime.now().isoformat(timespec="seconds")
        shift_id = get_shift_id()
        visit_id = f"{now_iso}-{uuid.uuid4().hex[:6].upper()}"

        rows = []
        for item in per_line_inputs:
            rows.append({
                "timestamp_iso": now_iso,
                "shift_id": shift_id,
                "visit_id": visit_id,
                "branch_id": branch_id,
                "plate": plate.upper() if plate else "",
                "vehicle_model": vehicle_label,
                "vehicle_class": vehicle_class,
                "service": item["service"],
                "units": item["units"],
                "price_peso": item["price_peso"],
                "amount_peso": item["amount_peso"],
                "amount_paid_peso": float(amount_paid or 0.0),
                "payment_method": payment_method,
                "performed_by_employee_id": item["performed_by_employee_id"],
                "customer_name": customer_name,
                "customer_phone": customer_phone,
                "notes": item["notes"]
            })
        record_transaction_rows(rows)
        st.success(f"Saved visit {visit_id} ({len(rows)} service line(s)) for branch {branch_id}.")



# ======= UI =======
st.set_page_config(page_title="RJ AutoSpa Payroll", page_icon="🧽", layout="wide")
st.title("🏎️ Bodi's 24/7 Car Wash Payroll")

# Tabs
tab_run, tab_tx_b1, tab_tx_b2, tab_admin, tab_pay = st.tabs(["Clock In/Out", "Log Visit — Branch 1", "Log Visit — Branch 2", "Admin", "Payroll"])


with tab_admin:
    st.subheader("🔧 Google Sheets connection")
    st.write("Workbook:", SHEET_NAME)
    if st.button("Refresh Catalog (Services, Classes, Policy, Employees, Models)"):
        st.cache_data.clear()
        services, classes, policy, emps, vmodels = load_catalog()
        st.success("Refreshed.")

    if st.button("Create empty tabs if missing"):
        for t in [TAB_SERVICES, TAB_VEHICLE_CLASSES, TAB_VEHICLE_MODELS, TAB_EMPLOYEES, TAB_COMMISSION_POLICY, TAB_ATTENDANCE, TAB_TRANSACTIONS, TAB_PAYROLL_EXPORTS]:
            _ = load_sheet(SHEET_NAME, t)
        st.success("Tabs ensured / created if missing.")

    st.info("Tune commission behavior in **commission_policy**: "
            "`commission_type` = 'pool_split' or 'direct'; `percent` as needed. Regex in `service_regex` lets you target services.")

with tab_run:
    st.subheader("👤 Clock In / Clock Out")
    services, classes, policy, emps, vmodels = load_catalog()

    # Branch selector for attendance
    branch_choice = st.selectbox("Branch for this shift", ["B1", "B2"], index=0)

    def get_pin_for(eid):
        row = emps.set_index("employee_id").loc[eid]
        if "password_hint" in row and pd.notna(row["password_hint"]) and str(row["password_hint"]).strip() != "":
            return str(row["password_hint"])
        return str(row.get("pin_hint",""))

    employee = st.selectbox(
        "Employee",
        options=emps["employee_id"].tolist(),
        format_func=lambda x: f"{x} — {emps.set_index('employee_id').loc[x, 'name']}"
    )
    pwd = st.text_input("Simple PIN", type="password")
    col_in, col_out = st.columns(2)
    with col_in:
        if st.button("CLOCK IN"):
            hint = get_pin_for(employee)
            if pwd == hint:
                record_attendance(employee, "CLOCK_IN", branch_choice)
                st.cache_data.clear() 
                st.success(f"{employee} clocked in at {branch_choice}.")
                st.rerun()
            else:
                st.error("Wrong PIN.")
    with col_out:
        if st.button("CLOCK OUT"):
            hint = get_pin_for(employee)
            if pwd == hint:
                record_attendance(employee, "CLOCK_OUT", branch_choice)
                st.success(f"{employee} clocked out at {branch_choice}.")
            else:
                st.error("Wrong PIN.")


with tab_tx_b1:
    log_visit_ui("B1")

with tab_tx_b2:
    log_visit_ui("B2")

with tab_pay:
    st.subheader("🧮 Payroll (15-day periods)")
    today = date.today()
    s_guess, e_guess = current_pay_window(today)
    colx, coly = st.columns(2)
    with colx:
        start_date = st.date_input("Start", value=s_guess)
    with coly:
        end_date   = st.date_input("End", value=e_guess)

    if st.button("Compute Payroll"):
        payroll, ledger = compute_commissions(start_date, end_date)
        if payroll.empty:
            st.warning("No data in this range.")
        else:
            st.success(f"Computed payroll for {start_date} → {end_date}")
            st.dataframe(payroll, use_container_width=True)
            st.download_button("⬇️ Download Payroll CSV", data=payroll.to_csv(index=False), file_name=f"payroll_{start_date}_{end_date}.csv", mime="text/csv")

            with st.expander("See Commission Ledger (per employee & shift)"):
                st.dataframe(ledger, use_container_width=True)

            if st.checkbox("Append this run to 'payroll_exports' tab"):
                exports = load_sheet(SHEET_NAME, TAB_PAYROLL_EXPORTS)
                new = pd.concat([exports, payroll], ignore_index=True) if not exports.empty else payroll.copy()
                write_df(SHEET_NAME, TAB_PAYROLL_EXPORTS, new)
                st.success("Appended to payroll_exports.")
