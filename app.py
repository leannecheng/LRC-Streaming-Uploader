import streamlit as st
import pandas as pd
import io
import re
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ───────────────────────────────────────────────
# CONFIGURATION — fill in your Drive file ID
MASTER_FILE_ID = "1-oPMoY0D_vaF0vhxPVKizDLnNIFulcYa"
SCOPES = ["https://www.googleapis.com/auth/drive"]
REDIRECT_URI = "https://lrc-streaming-laura-eyes-only.streamlit.app/"  # Replace with your deployed app URL
# ───────────────────────────────────────────────

def creds_to_dict(creds):
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }

@st.cache_resource(show_spinner=False)
def get_drive_service():
    if "credentials" not in st.session_state:
        st.error("You must authenticate first using the Google login button above.")
        st.stop()
    creds = Credentials(**st.session_state["credentials"])
    return build("drive", "v3", credentials=creds)

def sort_terms_dict(terms: dict) -> dict:
    order_map = {"Winter": 0, "SpSu": 1, "Fall": 2}
    def key_fn(item):
        term_str, _ = item
        parts = term_str.split()
        season = parts[0]
        year = parts[1] if len(parts) > 1 else "0"
        y = int(year) if year.isdigit() else 0
        return (y, order_map.get(season, 3))
    return dict(sorted(terms.items(), key=key_fn))

st.title("LRC Streaming Data Uploader")

# ───────────────────────────────
# Google OAuth Flow
flow = Flow.from_client_config(
    {
        "web": {
            "client_id": st.secrets["drive_oauth"]["client_id"],
            "client_secret": st.secrets["drive_oauth"]["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token"
        }
    },
    scopes=SCOPES,
    redirect_uri=REDIRECT_URI,
)

query_params = st.experimental_get_query_params()
if "code" in query_params:
    code = query_params["code"][0]
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        st.session_state["credentials"] = creds_to_dict(creds)
        st.success("✅ Successfully authenticated with Google!")
    except Exception as e:
        st.error(f"Failed to fetch token: {e}")

if "credentials" not in st.session_state:
    auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline", include_granted_scopes="true")
    st.warning("🔐 You must log in with Google to continue.")
    st.markdown(f"[Click here to authenticate with Google]({auth_url})")
    st.stop()
# ───────────────────────────────

# PART 1: RAW EXCEL UPLOAD & CLEANING
st.header("1️⃣ Prepare & Upload Raw Excel")
st.markdown("""
- Upload your new Excel file containing streaming data.
- Ensure:
  - **Course** formatted `DEPT NUM` (e.g. `FRENCH 220`)
  - **Language** cells contain only valid languages
  - Headers exactly: `Uniquename`, `Course`, `Section`, `CIR_COL::LANGUAGE`, `Enrollment`
  - **Only one** sheet in raw Excel file
""")
st.subheader("Example")
st.table(pd.DataFrame({
    "Uniquename":       ["abc123", "xyz456", "abc123"],
    "Course":           ["FRENCH 220","GERMAN 101","FRENCH 220"],
    "Section":          [1,2,1],
    "CIR_COL::LANGUAGE":["French","German","French"],
    "Enrollment":       [25,18,25]
}))

uploaded = st.file_uploader("Upload original .xlsx:", type="xlsx", key="raw")
term_input = st.text_input("Enter term and year in EXACT FORM: 'Winter 2025', 'SpSu 2025', 'Fall 2025'")

if uploaded and term_input:
    term = " ".join(term_input.strip().split())
    st.session_state["term"] = term

    try:
        df_raw = pd.read_excel(uploaded, sheet_name=0)
    except Exception as e:
        st.error(f"Error reading Excel: {e}")
        st.stop()

    df_raw.columns = df_raw.columns.str.strip()
    lower_map = {c.lower(): c for c in df_raw.columns}
    required = ["uniquename","course","section","cir_col::language","enrollment"]
    missing = [r for r in required if r not in lower_map]
    if missing:
        st.error(f"Missing columns: {missing}. Found: {list(df_raw.columns)}")
        st.stop()

    U, C, S, L, E = (lower_map[k] for k in required)

    # Basic cleaning
    df = df_raw[df_raw[C].notna() & df_raw[C].str.strip().ne("")].copy()
    df[C] = df[C].str.strip().str.upper()  # uppercase course
    df = df[~df[C].str.lower().str.contains("testcourse", na=False)]

    records = []
    for (instr, course, section), group in df.groupby([U, C, S], dropna=False):
        langs = group[L].dropna().astype(str).str.strip().tolist()
        seen = []
        for ln in langs:
            if ln not in seen:
                seen.append(ln)
        language = ", ".join(seen)

        reservations = len(group)
        non_null = group[E].dropna()
        if len(non_null):
            rawv = str(non_null.iloc[0])
            digs = re.findall(r"\d+", rawv)
            students = int(digs[0]) if digs else 0
        else:
            students = 0

        sec = section if pd.notna(section) and str(section).strip() else "Unknown Section"
        records.append({
            "Instructor":       instr,
            "Course":           course,
            "Section":          sec,
            "Language":         language,
            "Reservations":     reservations,
            "Students Enrolled": students
        })

    clean_df = pd.DataFrame(records)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        clean_df.to_excel(writer, sheet_name=term, index=False)
    excel_bytes = buf.getvalue()
    st.session_state["excel_bytes"] = excel_bytes

    st.download_button(
        label="Download Processed Excel",
        data=excel_bytes,
        file_name=f"{term.replace(' ', '_')}_processed.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    st.info("Check duplicates, typos, valid languages. Then click **Next**.")
    if st.button("Next ➡️"):
        st.session_state["step2"] = True

# PART 2: MERGE TO MASTER JSON & MANUAL REVERT
if st.session_state.get("step2"):
    st.header("2️⃣ Upload Checked Excel to Dashboard")
    st.info("Important! Download a backup copy before uploading new data.")

    drive = get_drive_service()
    req = drive.files().get_media(fileId=MASTER_FILE_ID)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    master_data = json.loads(fh.getvalue().decode())

    st.download_button(
        label="Download Backup Copy",
        data=json.dumps(master_data, indent=2),
        file_name="master_streaming_terms.json",
        mime="application/json"
    )

    checked = st.file_uploader("Upload checked .xlsx:", type="xlsx", key="checked")

    if checked:
        term = st.session_state["term"]
        df_term = pd.read_excel(checked, sheet_name=term)

        term_json = {"total_students": 0, "total_reservations": 0, "departments": {}}
        for _, row in df_term.iterrows():
            code = str(row["Course"]).strip()
            if not code or "practice" in code.lower():
                continue

            raw_dept = code.split()[0].strip().lower()
            lang_cell = row["Language"]
            if pd.isna(lang_cell):
                first_lang = None
            else:
                first_lang_raw = str(lang_cell).split(",")[0].strip().split()[0]
                first_lang = first_lang_raw if first_lang_raw.lower() != "nan" else None

            if raw_dept in ("asianlan", "slavic", "asian") and first_lang:
                dept_key = f"{raw_dept}: {first_lang}".upper()
            else:
                dept_key = raw_dept.upper()

            m = re.search(r"\d+", code)
            level = str((int(m.group()) // 100) * 100) if m else "Unknown"

            students = int(row["Students Enrolled"])
            reservs  = int(row["Reservations"])

            dept = term_json["departments"].setdefault(dept_key, {
                "levels": {}, "total_students": 0, "total_reservations": 0
            })
            lvl = dept["levels"].setdefault(level, {"students": 0, "reservations": 0})
            lvl["students"]     += students
            lvl["reservations"] += reservs
            dept["total_students"]     += students
            dept["total_reservations"] += reservs
            term_json["total_students"]     += students
            term_json["total_reservations"] += reservs

        master_data.setdefault("terms", {})
        master_data["terms"][term] = term_json
        master_data["terms"] = sort_terms_dict(master_data["terms"])

        updated_bytes = json.dumps(master_data, indent=2).encode()
        media = MediaIoBaseUpload(io.BytesIO(updated_bytes), mimetype="application/json")
        drive.files().update(fileId=MASTER_FILE_ID, media_body=media).execute()

        st.success(f"✅ Term '{term}' uploaded. Refresh dashboard to see new data!")

        st.subheader("🧐 Something looks wrong?")
        backup = st.file_uploader("Upload the backup, refresh the page, then try again. You can skip the first download step.", type="json", key="backup")
        if backup:
            new_bytes = backup.read()
            media2 = MediaIoBaseUpload(io.BytesIO(new_bytes), mimetype="application/json")
            drive.files().update(fileId=MASTER_FILE_ID, media_body=media2).execute()
            st.success("🔄 Master JSON overwritten with uploaded backup.")
