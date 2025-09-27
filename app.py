import re
import time
from datetime import datetime, timedelta, date
import pandas as pd
import streamlit as st

# --- Google Sheets (gspread) ---
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ======= CONFIG =======
SHEET_NAME            = st.secrets["sheets"]["workbook_name"]    # e.g. "RJ_AutoSpa_Payroll"
TAB_SERVICES          = "services"
TAB_VEHICLE_CLASSES   = "vehicle_classes"
TAB_EMPLOYEES         = "employees"
TAB_COMMISSION_POLICY = "commission_policy"
TAB_ATTENDANCE        = "attendance"
TAB_TRANSACTIONS      = "transactions"
TAB_PAYROLL_EXPORTS   = "payroll_exports"  # optional archive tab (append-only)

# 15-day windows: 1–15 and 16–end of month
def current_pay_window(dt: date):
    if dt.day <= 15:
        start = dt.replace(day=1)
        end   = dt.replace(day=15)
    else:
        start = dt.replace(day=16)
        # end-of-month
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
        ws = sh.add_worksheet(title=tab, rows=1000, cols=26)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    return df

def write_df(sheet_name, tab, df: pd.DataFrame):
    client = get_client()
    sh = client.open(sheet_name)
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=1000, cols=26)
    ws.clear()
    if df.empty:
        ws.update("A1", [list(df.columns)])
    else:
        ws.update([list(df.columns)] + df.astype(object).values.tolist())

# ======= BUSINESS LOGIC =======
@st.cache_data(ttl=30)
def load_catalog():
    services = load_sheet(SHEET_NAME, TAB_SERVICES)
    classes  = load_sheet(SHEET_NAME, TAB_VEHICLE_CLASSES)
    policy   = load_sheet(SHEET_NAME, TAB_COMMISSION_POLICY)
    emps     = load_sheet(SHEET_NAME, TAB_EMPLOYEES)
    return services, classes, policy, emps

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
    # 24/7 rolling shift: “YYYY-MM-DD DayShift/NightShift”
    ts = ts or datetime.now()
    hour = ts.hour
    shift = "Day" if 6 <= hour < 18 else "Night"
    return f"{ts.date()}_{shift}"

def ensure_columns(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = []
    return df[cols]

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

def record_transaction(plate, vclass, service, units, amount, performer_id="", notes=""):
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    tx = ensure_columns(tx, ["timestamp_iso","shift_id","plate","vehicle_class","service","units",
                             "price_peso","amount_peso","performed_by_employee_id","notes"])
    row = {
        "timestamp_iso": datetime.now().isoformat(timespec="seconds"),
        "shift_id": get_shift_id(),
        "plate": plate.upper() if plate else "",
        "vehicle_class": vclass,
        "service": service,
        "units": float(units),
        "price_peso": float(amount),
        "amount_peso": float(amount) * float(units),
        "performed_by_employee_id": performer_id,
        "notes": notes
    }
    tx = pd.concat([tx, pd.DataFrame([row])], ignore_index=True)
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
    services, classes, policy, emps = load_catalog()
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    att = load_sheet(SHEET_NAME, TAB_ATTENDANCE)
    if tx.empty:
        return pd.DataFrame(), pd.DataFrame()

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
            if str(t.get("performed_by_employee_id") or "") == "":
                # no performer → treat as pool to be safe
                pool_by_shift[shift_id] = pool_by_shift.get(shift_id, 0.0) + amount * (pct/100.0)
            else:
                comm_rows.append({
                    "shift_id": shift_id,
                    "employee_id": t["performed_by_employee_id"],
                    "service": service,
                    "vehicle_class": vclass,
                    "commission_type": "direct",
                    "percent": pct,
                    "base_amount": amount,
                    "commission_peso": amount * (pct/100.0)
                })
        else:
            # pool_split (default for Carwash/Promos)
            pool_by_shift[shift_id] = pool_by_shift.get(shift_id, 0.0) + amount * (pct/100.0)

    # 2) Split pools among clocked-in staff
    if not att.empty:
        for shift_id, pool_amt in pool_by_shift.items():
            active = who_is_clocked_in(att, shift_id)
            if len(active) == 0:
                # If no one clocked in, donate pool to "UNASSIGNED" (adjust manually)
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

    # Consider a "day present" if the employee CLOCK_IN at least once per calendar day
    att_df["date"] = att_df["timestamp"].dt.date
    present = att_df[att_df["action"] == "CLOCK_IN"].groupby(["employee_id","date"]).size().reset_index()
    days_present = present.groupby("employee_id").size().rename("days_present").reset_index()

    # Merge base salary
    emps2 = emps.rename(columns={"employee_id":"employee_id","base_daily_salary":"base_daily_salary"})
    emps2["base_daily_salary"] = pd.to_numeric(emps2["base_daily_salary"], errors="coerce").fillna(0)

    payroll = days_present.merge(emps2[["employee_id","name","role","base_daily_salary"]], on="employee_id", how="left")
    if payroll.empty:
        payroll = pd.DataFrame(columns=["employee_id","name","role","base_daily_salary","days_present"])
    payroll["base_pay_peso"] = payroll["base_daily_salary"] * payroll["days_present"]

    # Add commissions
    if not comm_df.empty:
        comm_sum = comm_df.groupby("employee_id")["commission_peso"].sum().rename("commission_peso").reset_index()
        payroll = payroll.merge(comm_sum, on="employee_id", how="left")
    else:
        payroll["commission_peso"] = 0.0

    payroll["commission_peso"] = payroll["commission_peso"].fillna(0.0)
    payroll["total_peso"] = payroll["base_pay_peso"].fillna(0.0) + payroll["commission_peso"]

    payroll["period_start"] = start_date.isoformat()
    payroll["period_end"]   = end_date.isoformat()

    # Detailed ledger for transparency
    ledger = comm_df.sort_values(["shift_id","employee_id","service"])
    return payroll.sort_values("employee_id"), ledger

# ======= UI =======
st.set_page_config(page_title="RJ AutoSpa Payroll", page_icon="🧽", layout="wide")

st.title("🧽 RJ AutoSpa — 24/7 Car Wash Payroll (Google Sheets + Streamlit)")

# Tabs
tab_run, tab_tx, tab_admin, tab_pay = st.tabs(["Clock In/Out", "Log Service", "Admin", "Payroll"])

with tab_admin:
    st.subheader("🔧 Google Sheets connection")
    st.write("Workbook:", SHEET_NAME)
    if st.button("Refresh Catalog (Services, Classes, Policy, Employees)"):
        st.cache_data.clear()
        services, classes, policy, emps = load_catalog()
        st.success("Refreshed.")

    st.markdown("**Seed/Update Sheets** (paste from CSV templates you imported):")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Create empty tabs if missing"):
            # just touches tabs via load_sheet
            for t in [TAB_SERVICES, TAB_VEHICLE_CLASSES, TAB_EMPLOYEES, TAB_COMMISSION_POLICY, TAB_ATTENDANCE, TAB_TRANSACTIONS, TAB_PAYROLL_EXPORTS]:
                _ = load_sheet(SHEET_NAME, t)
            st.success("Tabs ensured.")

    st.info("Change commission behavior in the **commission_policy** sheet: "
            "`commission_type` = 'pool_split' or 'direct'; `percent` = 30 (or any). "
            "Regex in `service_regex` lets you target services flexibly.")

with tab_run:
    st.subheader("👤 Clock In / Clock Out")
    services, classes, policy, emps = load_catalog()

    employee = st.selectbox("Employee", options=emps["employee_id"].tolist(), format_func=lambda x: f"{x} — {emps.set_index('employee_id').loc[x, 'name']}")
    pwd = st.text_input("Simple PIN (password hint)", type="password")
    if st.button("CLOCK IN"):
        # super-simple check (use a hint column)
        hint = str(emps.set_index("employee_id").loc[employee, "password_hint"])
        if pwd == hint:
            record_attendance(employee, "CLOCK_IN")
            st.success(f"{employee} clocked in.")
        else:
            st.error("Wrong PIN.")
    if st.button("CLOCK OUT"):
        hint = str(emps.set_index("employee_id").loc[employee, "password_hint"])
        if pwd == hint:
            record_attendance(employee, "CLOCK_OUT")
            st.success(f"{employee} clocked out.")
        else:
            st.error("Wrong PIN.")

with tab_tx:
    st.subheader("🧾 Log a Service")
    services_df, classes_df, policy_df, emps_df = load_catalog()

    col1, col2, col3 = st.columns(3)
    with col1:
        plate = st.text_input("Plate (optional)")
        vclass = st.selectbox("Vehicle Class", options=classes_df["vehicle_class"].tolist())
    with col2:
        service_name = st.selectbox("Service", options=sorted(services_df["service"].unique().tolist()))
        units = st.number_input("Units (e.g., number of panels for Glass Restoration)", min_value=1.0, value=1.0, step=1.0)
    with col3:
        # auto price lookup
        price_row = services_df[(services_df["service"] == service_name) & (services_df["vehicle_class"] == vclass)]
        if price_row.empty:
            st.warning("No price found for this class; enter manually.")
            price = st.number_input("Price (₱)", min_value=0.0, step=10.0, value=0.0)
        else:
            price = float(price_row.iloc[0]["price_peso"])
            st.write(f"Price (₱): **{price:,.2f}**")
        performer = st.selectbox("Performed by (for direct commissions)", options=[""] + emps_df["employee_id"].tolist())
        notes = st.text_input("Notes (optional)")

    if st.button("Add Transaction"):
        record_transaction(plate, vclass, service_name, units, price, performer, notes)
        st.success("Transaction logged.")

    st.divider()
    st.caption("Recent transactions (today)")
    tx = load_sheet(SHEET_NAME, TAB_TRANSACTIONS)
    if not tx.empty:
        today = pd.Timestamp.now().date()
        tx["timestamp"] = pd.to_datetime(tx["timestamp_iso"], errors="coerce")
        st.dataframe(tx[tx["timestamp"].dt.date == today].sort_values("timestamp_iso").tail(30), use_container_width=True)
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

            # Optional: append to archive tab
            if st.checkbox("Append this run to 'payroll_exports' tab"):
                exports = load_sheet(SHEET_NAME, TAB_PAYROLL_EXPORTS)
                new = pd.concat([exports, payroll], ignore_index=True) if not exports.empty else payroll.copy()
                write_df(SHEET_NAME, TAB_PAYROLL_EXPORTS, new)
                st.success("Appended to payroll_exports.")
