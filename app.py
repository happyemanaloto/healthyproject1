import re
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
    "timestamp_iso","shift_id","visit_id","plate","vehicle_model","vehicle_class","service","units",
    "price_peso","amount_peso","amount_paid_peso","payment_method",
    "performed_by_employee_id","customer_name","customer_phone","notes"
]

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

def match_commission_rule(service_name, policy_df):
    """Return (commission_type, percent) for the first matching regex rule."""
    for _, r in policy_df.iterrows():
        pattern = str(r.get("service_regex") or "")
        try:
            if re.search(pattern, service_name, flags=re.IGNORECASE):
                return r.get("commission_type"), float(r.get("percent", 0))
        except re.error:
            continue
    return None, 0.0

def get_shift_id(ts=None):
    ts = ts or datetime.now()
    hour = ts.hour
    shift = "Day" if 6 <= hour < 18 else "Night"
    return f"{ts.date()}_{shift}"

def ensure_columns(df, cols, fill_value=""):
    """
    Ensure columns exist. Uses a scalar fill so it broadcasts correctly
    even when df already has rows (prevents ValueError length mismatch).
    """
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value  # scalar -> broadcasts to all existing rows
    return df[cols] if all(c in df.columns for c in cols) else df


def ensure_tx_columns(df):
    df = ensure_columns(df, TX_COLS)
    return df[TX_COLS]  # reorder columns


def record_attendance(employee_id, action):
    att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    att = ensure_columns(att, ["timestamp_iso","shift_id","employee_id","action"])
    row = {
        "timestamp_iso": datetime.now().isoformat(timespec="seconds"),
        "shift_id": get_shift_id(),
        "employee_id": employee_id,
        "action": action
    }
    att = pd.concat([att, pd.DataFrame([row])], ignore_index=True)
    write_df(SHEET_NAME, TAB_ATTENDANCE, att)

def record_transaction_rows(rows):
    """Append multiple rows (one visit with many services)."""
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    tx = ensure_tx_columns(tx)
    tx = pd.concat([tx, pd.DataFrame(rows)], ignore_index=True)
    write_df(SHEET_NAME, TAB_TRANSACTIONS, tx)

def who_is_clocked_in(att_df, shift_id):
    att_df = att_df[att_df["shift_id"] == shift_id].sort_values("timestamp_iso")
    status = {}
    for _, r in att_df.iterrows():
        eid = r["employee_id"]
        if r["action"] == "CLOCK_IN":
            status[eid] = True
        elif r["action"] == "CLOCK_OUT":
            status[eid] = False
    return [eid for eid, on in status.items() if on]

def compute_commissions(start_date, end_date):
    services, classes, policy, emps, vmodels = load_catalog()
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    if tx.empty:
        return pd.DataFrame(), pd.DataFrame()

    tx = ensure_tx_columns(tx)
    tx["timestamp"] = pd.to_datetime(tx["timestamp_iso"], errors="coerce")
    mask = (tx["timestamp"].dt.date >= start_date) & (tx["timestamp"].dt.date <= end_date)
    tx = tx[mask].copy()

    # Pre-index prices for quicker lookup
    price_map = {(r["service"], r["vehicle_class"]): float(r["price_peso"]) for _, r in services.iterrows()}

    # Commission ledger per employee
    comm_rows = []

    # 1) Direct commissions & pool accrual per shift
    pool_by_shift = {}  # shift_id -> peso amount

    for _, t in tx.iterrows():
        service = t["service"]
        vclass  = t["vehicle_class"]
        units   = float(t.get("units", 1) or 1)
        price   = price_map.get((service, vclass), float(t.get("price_peso", 0) or 0))
        amount  = price * units
        shift_id = t["shift_id"]
        ctype, pct = match_commission_rule(service, policy)

        if ctype == "direct":
            performer = str(t.get("performed_by_employee_id") or "")
            if performer == "":
                # safety: if missing, treat as pool
                pool_by_shift[shift_id] = pool_by_shift.get(shift_id, 0.0) + amount * (pct/100.0)
            else:
                comm_rows.append({
                    "shift_id": shift_id,
                    "employee_id": performer,
                    "service": service,
                    "vehicle_class": vclass,
                    "commission_type": "direct",
                    "percent": pct,
                    "base_amount": amount,
                    "commission_peso": amount * (pct/100.0)
                })
        else:
            pool_by_shift[shift_id] = pool_by_shift.get(shift_id, 0.0) + amount * (pct/100.0)

    # 2) Split pools among clocked-in staff
    if not att.empty:
        for shift_id, pool_amt in pool_by_shift.items():
            active = who_is_clocked_in(att, shift_id)
            if len(active) == 0:
                comm_rows.append({
                    "shift_id": shift_id,
                    "employee_id": "UNASSIGNED",
                    "service": "POOL_SPLIT",
                    "vehicle_class": "",
                    "commission_type": "pool_split",
                    "percent": None,
                    "base_amount": pool_amt,
                    "commission_peso": pool_amt
                })
            else:
                share = pool_amt / len(active)
                for eid in active:
                    comm_rows.append({
                        "shift_id": shift_id,
                        "employee_id": eid,
                        "service": "POOL_SPLIT",
                        "vehicle_class": "",
                        "commission_type": "pool_split",
                        "percent": None,
                        "base_amount": pool_amt,
                        "commission_peso": share
                    })

    comm_df = pd.DataFrame(comm_rows)

    # 3) Base salary calculation from attendance (count CLOCK_IN days)
    att_df = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    att_df["timestamp"] = pd.to_datetime(att_df["timestamp_iso"], errors="coerce")
    mask2 = (att_df["timestamp"].dt.date >= start_date) & (att_df["timestamp"].dt.date <= end_date)
    att_df = att_df[mask2].copy()

    att_df["date"] = att_df["timestamp"].dt.date
    present = att_df[att_df["action"] == "CLOCK_IN"].groupby(["employee_id","date"]).size().reset_index()
    days_present = present.groupby("employee_id").size().rename("days_present").reset_index()

    emps2 = load_sheet(SHEET_NAME, TAB_EMPLOYEES)
    emps2["base_daily_salary"] = pd.to_numeric(emps2.get("base_daily_salary", 0), errors="coerce").fillna(0)

    payroll = days_present.merge(emps2[["employee_id","name","role","base_daily_salary"]], on="employee_id", how="left")
    if payroll.empty:
        payroll = pd.DataFrame(columns=["employee_id","name","role","base_daily_salary","days_present"])
    payroll["base_pay_peso"] = payroll["base_daily_salary"] * payroll["days_present"]

    if not comm_df.empty:
        comm_sum = comm_df.groupby("employee_id")["commission_peso"].sum().rename("commission_peso").reset_index()
        payroll = payroll.merge(comm_sum, on="employee_id", how="left")
    else:
        payroll["commission_peso"] = 0.0

    payroll["commission_peso"] = payroll["commission_peso"].fillna(0.0)
    payroll["total_peso"] = payroll["base_pay_peso"].fillna(0.0) + payroll["commission_peso"]

    payroll["period_start"] = start_date.isoformat()
    payroll["period_end"]   = end_date.isoformat()

    ledger = comm_df.sort_values(["shift_id","employee_id","service"])
    return payroll.sort_values("employee_id"), ledger

# ======= UI =======
st.set_page_config(page_title="RJ AutoSpa Payroll", page_icon="🧽", layout="wide")
st.title("🧽 RJ AutoSpa — 24/7 Car Wash Payroll (Google Sheets + Streamlit)")

# Tabs
tab_run, tab_tx, tab_admin, tab_pay = st.tabs(["Clock In/Out", "Log Visit", "Admin", "Payroll"])

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

    # accept either password_hint or pin_hint
    def get_pin_for(eid):
        row = emps.set_index("employee_id").loc[eid]
        if "password_hint" in row and pd.notna(row["password_hint"]) and str(row["password_hint"]).strip() != "":
            return str(row["password_hint"])
        return str(row.get("pin_hint",""))

    employee = st.selectbox("Employee", options=emps["employee_id"].tolist(), format_func=lambda x: f"{x} — {emps.set_index('employee_id').loc[x, 'name']}")
    pwd = st.text_input("Simple PIN", type="password")
    col_in, col_out = st.columns(2)
    with col_in:
        if st.button("CLOCK IN"):
            hint = get_pin_for(employee)
            if pwd == hint:
                record_attendance(employee, "CLOCK_IN")
                st.success(f"{employee} clocked in.")
            else:
                st.error("Wrong PIN.")
    with col_out:
        if st.button("CLOCK OUT"):
            hint = get_pin_for(employee)
            if pwd == hint:
                record_attendance(employee, "CLOCK_OUT")
                st.success(f"{employee} clocked out.")
            else:
                st.error("Wrong PIN.")

with tab_tx:
    st.subheader("🧾 Log a Visit (multi-service, per-service assignee, optional customer info)")
    services_df, classes_df, policy_df, emps_df, vmodels_df = load_catalog()

    # model dropdown (staff-friendly)
    vmodels_df = vmodels_df.copy()
    vmodels_df["label"] = vmodels_df["label"].astype(str)
    vehicle_label = st.selectbox("Vehicle model (search by name)", options=sorted(vmodels_df["label"].unique().tolist()))
    vehicle_class = vmodels_df.set_index("label").loc[vehicle_label, "vehicle_class"]
    st.caption(f"Detected vehicle class: **{vehicle_class}**")

    plate = st.text_input("Plate (optional)")
    customer_name = st.text_input("Customer name (optional)")
    customer_phone = st.text_input("Customer phone (optional)")
    payment_method = st.selectbox("Payment method", ["", "cash", "gcash", "card", "other"])
    amount_paid = st.number_input("Amount paid (₱)", min_value=0.0, value=0.0, step=10.0, help="Total paid for this visit")

    st.markdown("### Select services")
    # Checkbox-like multi-select
    all_services = sorted(services_df["service"].unique().tolist())
    selected_services = st.multiselect("Services in this visit", options=all_services)

    # Per-service inputs (units + performer + notes), every service must have an employee
    per_line_inputs = []
    total_amount = 0.0
    for svc in selected_services:
        with st.expander(f"{svc}", expanded=True):
            cols = st.columns([1, 1, 1])
            with cols[0]:
                units = st.number_input(f"{svc} — Units", min_value=1.0, value=1.0, step=1.0, key=f"units_{svc}")
            with cols[1]:
                # price lookup via (service, vehicle_class)
                price_row = services_df[(services_df["service"] == svc) & (services_df["vehicle_class"] == vehicle_class)]
                if price_row.empty:
                    st.warning("No price found for this model's class; enter manually.")
                    price = st.number_input(f"{svc} — Price (₱)", min_value=0.0, step=10.0, value=0.0, key=f"price_{svc}")
                else:
                    price = float(price_row.iloc[0]["price_peso"])
                    st.write(f"Price (₱): **{price:,.2f}**")
            with cols[2]:
                performer = st.selectbox(f"{svc} — Performed by", options=emps_df["employee_id"].tolist(), format_func=lambda x: f"{x} — {emps_df.set_index('employee_id').loc[x,'name']}", key=f"perf_{svc}")

            notes = st.text_input(f"{svc} — Notes (optional)", key=f"notes_{svc}")
            amount_line = price * units
            total_amount += amount_line
            st.caption(f"Line total: ₱{amount_line:,.2f}")

            per_line_inputs.append({
                "service": svc,
                "units": float(units),
                "price_peso": float(price),
                "amount_peso": float(amount_line),
                "performed_by_employee_id": performer,
                "notes": notes
            })

    st.metric("Services total (₱)", f"{total_amount:,.2f}")
    st.metric("Change (₱)", f"{(amount_paid - total_amount):,.2f}")

    # Save visit
    if st.button("🧾 Save visit", type="primary", disabled=(len(per_line_inputs)==0)):
        # Validate: each service must have an assignee
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
                "plate": plate.upper() if plate else "",
                "vehicle_model": vehicle_label,
                "vehicle_class": vehicle_class,
                "service": item["service"],
                "units": item["units"],
                "price_peso": item["price_peso"],
                "amount_peso": item["amount_peso"],
                "amount_paid_peso": float(amount_paid or 0.0),  # repeated per row for convenience
                "payment_method": payment_method,
                "performed_by_employee_id": item["performed_by_employee_id"],
                "customer_name": customer_name,
                "customer_phone": customer_phone,
                "notes": item["notes"]
            })

        record_transaction_rows(rows)
        st.success(f"Saved visit {visit_id} with {len(rows)} service(s).")

    st.divider()
    st.caption("Recent transactions (today)")
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    if not tx.empty:
        tx = ensure_tx_columns(tx)
        today = pd.Timestamp.now().date()
        tx["timestamp"] = pd.to_datetime(tx["timestamp_iso"], errors="coerce")
        st.dataframe(
            tx[tx["timestamp"].dt.date == today].sort_values(["timestamp_iso","visit_id"]).tail(50),
            use_container_width=True
        )
    else:
        st.info("No transactions yet.")

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
