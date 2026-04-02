from pathlib import Path
from datetime import datetime
import re
import base64
import html
import textwrap
import time
import warnings
from urllib.parse import quote_plus
 
import altair as alt
import pandas as pd
import streamlit as st
 
st.set_page_config(page_title="Error Responsibility Dashboard", layout="wide")
 
DATA_FILE_URL = "https://raw.githubusercontent.com/Daniel15maria/Excel/refs/heads/main/LP_Error_Log.xlsx"
ASSET_DIR = Path("data")
AUTO_REFRESH_INTERVAL = "60s"
HEADER_MARKER = "Rollout Date"
NAME_CONVERSION_SHEET = "Name_Converstion"
ROLE_COLUMNS = {
    "error": "Error",
}
 
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    module="openpyxl",
)
 
 
def normalize_column_name(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")
 
 
def clean_text(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text
 
 
def parse_rollout_date(value):
    if pd.isna(value) or clean_text(value) == "":
        return pd.NaT
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return pd.Timestamp("1899-12-30") + pd.to_timedelta(float(value), unit="D")
    text = clean_text(value)
    if re.match(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$", text):
        return pd.to_datetime(text, errors="coerce", dayfirst=False)
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", text):
        return pd.to_datetime(text, errors="coerce", dayfirst=False)
    return pd.to_datetime(text, errors="coerce", dayfirst=True)
 
 
def read_excel_file(**kwargs):
    try:
        return pd.read_excel(DATA_FILE_URL, **kwargs)
    except Exception as exc:
        raise ConnectionError(
            "Unable to load the Excel file from the remote URL. "
            "Check the link, network access, and workbook permissions, then refresh."
        ) from exc
 
 
def read_name_mapping():
    try:
        name_df = read_excel_file(sheet_name=NAME_CONVERSION_SHEET)
    except ValueError:
        return {}
 
    name_df = name_df.rename(columns=lambda column: normalize_column_name(str(column)))
    short_form_column = next(
        (column for column in name_df.columns if column in {"short_form", "shortform"}),
        None,
    )
    name_column = next(
        (column for column in name_df.columns if column in {"name", "full_name", "fullname"}),
        None,
    )
 
    if not short_form_column or not name_column:
        return {}
 
    name_df[short_form_column] = name_df[short_form_column].map(clean_text)
    name_df[name_column] = name_df[name_column].map(clean_text)
    name_df = name_df[
        (name_df[short_form_column] != "") & (name_df[name_column] != "")
    ].copy()
 
    return {
        short_form.upper(): full_name
        for short_form, full_name in zip(name_df[short_form_column], name_df[name_column])
    }
 
 
def parse_error_log_sheet(raw_df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    header_matches = raw_df.apply(
        lambda row: row.astype(str).str.strip().eq(HEADER_MARKER).any(),
        axis=1,
    )
    if not header_matches.any():
        raise ValueError(f"Could not find header row containing '{HEADER_MARKER}' in sheet '{sheet_name}'.")
 
    header_row_index = header_matches[header_matches].index[0]
    sheet_df = raw_df.iloc[header_row_index:].reset_index(drop=True)
 
    headers = sheet_df.iloc[0].fillna("").map(clean_text)
    data_df = sheet_df.iloc[1:].copy()
    data_df.columns = headers
 
    valid_columns = [
        column
        for column in data_df.columns
        if column and not str(column).startswith("Unnamed")
    ]
    data_df = data_df.loc[:, valid_columns]
    data_df = data_df.rename(columns=lambda column: normalize_column_name(str(column)))
    data_df = data_df.dropna(how="all").reset_index(drop=True)
 
    for column in data_df.columns:
        if data_df[column].dtype == object:
            data_df[column] = data_df[column].map(clean_text)
 
    if "error_type" in data_df.columns:
        data_df["error_type"] = data_df["error_type"].replace("", "Unclassified")
 
    if "rollout_date" in data_df.columns:
        data_df["rollout_date"] = data_df["rollout_date"].map(parse_rollout_date)
 
    data_df["source_sheet"] = sheet_name
    return data_df
 
 
def read_data():
    workbook_sheets = read_excel_file(sheet_name=None, header=None)
    data_frames = []
 
    for sheet_name, raw_df in workbook_sheets.items():
        if sheet_name == NAME_CONVERSION_SHEET:
            continue
 
        try:
            parsed_df = parse_error_log_sheet(raw_df, sheet_name)
        except ValueError:
            continue
 
        if not parsed_df.empty:
            data_frames.append(parsed_df)
 
    if not data_frames:
        raise ValueError(
            f"Could not find any data sheet containing the '{HEADER_MARKER}' header."
        )
 
    data_df = pd.concat(data_frames, ignore_index=True)
    data_df["error_id"] = data_df.index + 1
    return data_df
 
 
@st.cache_data
def load_name_mapping(file_mtime):
    return read_name_mapping()
 
 
@st.cache_data
def load_data(file_mtime):
    return read_data()
 
 
@st.cache_data
def load_workbook_bundle(file_mtime):
    return read_data(), read_name_mapping()
 
 
def read_fresh_workbook(max_attempts: int = 4, delay_seconds: float = 0.2):
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return read_data(), read_name_mapping()
        except (ConnectionError, OSError, ValueError) as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(delay_seconds)
    raise last_exc
 
 
def refresh_workbook_bundle():
    st.cache_data.clear()
    source_df, name_mapping = read_fresh_workbook()
    st.session_state.file_mtime = get_file_version()
    st.session_state.refresh_nonce += 1
    set_refresh_timestamp()
    return source_df, name_mapping
 
 
def build_responsibility_df(source_df: pd.DataFrame) -> pd.DataFrame:
    assignment_frames = []
 
    for column_name, role_label in ROLE_COLUMNS.items():
        if column_name not in source_df.columns:
            continue
 
        role_df = source_df.copy()
        role_df["responsibility_role"] = role_label
        if column_name == "error":
            def resolve_people(row):
                raw_val = clean_text(row.get("error", ""))
                normalized = raw_val.lower().replace(" ", "")
                if normalized == "owner":
                    return [clean_text(row.get("owner", ""))]
                if normalized in {"peer1", "peer_1"}:
                    return [
                        clean_text(row.get("owner", "")),
                        clean_text(row.get("peer_1", "")),
                    ]
                if normalized in {"peer2", "peer_2"}:
                    return [clean_text(row.get("peer_2", ""))]
                if normalized == "client":
                    return [
                        clean_text(row.get("owner", "")),
                        clean_text(row.get("peer_1", "")),
                        clean_text(row.get("peer_2", "")),
                    ]
                return [raw_val]
 
            role_df["responsible_person"] = role_df.apply(resolve_people, axis=1)
            role_df = role_df.explode("responsible_person")
        else:
            role_df["responsible_person"] = role_df[column_name].map(clean_text)
        role_df["responsible_person"] = role_df["responsible_person"].map(clean_text)
        role_df = role_df[role_df["responsible_person"] != ""].copy()
        assignment_frames.append(role_df)
 
    if not assignment_frames:
        return pd.DataFrame()
 
    responsibility_df = pd.concat(assignment_frames, ignore_index=True)
    responsibility_df["error_classification"] = responsibility_df["error"].map(classify_error_origin)
    responsibility_df["responsibility_count"] = 1
    return responsibility_df
 
 
def build_fixed_bar_chart(chart_df: pd.DataFrame, x_col: str, y_col: str, y_max: int):
    return (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X(x_col, sort="-y", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y(y_col, title=None, scale=alt.Scale(domain=[0, y_max])),
            tooltip=[
                alt.Tooltip(x_col, title=x_col.replace("_", " ").title()),
                alt.Tooltip(y_col, title=y_col.replace("_", " ").title()),
            ],
        )
        .properties(height=320)
    )
 
 
def apply_optional_filter(df: pd.DataFrame, column: str, selected_values: list[str]) -> pd.DataFrame:
    if not selected_values:
        return df
    return df[df[column].isin(selected_values)]
 
 
def apply_search_filter(df: pd.DataFrame, search_term: str) -> pd.DataFrame:
    normalized_search = clean_text(search_term)
    if normalized_search == "":
        return df
 
    search_mask = df.apply(
        lambda row: row.astype(str).str.contains(normalized_search, case=False, na=False, regex=False).any(),
        axis=1,
    )
    return df[search_mask]
 
 
def classify_error_origin(value: str) -> str:
    normalized = clean_text(value).lower().replace(" ", "")
    return "Escaped" if normalized == "client" else "Internal"
 
 
def image_to_base64(path: Path) -> str:
    if not path.exists():
        return ""
    return base64.b64encode(path.read_bytes()).decode("ascii")
 
 
def get_file_version():
    return DATA_FILE_URL
 
 
if "refresh_nonce" not in st.session_state:
    st.session_state.refresh_nonce = 0
 
def get_local_timestamp_label():
    return datetime.now().astimezone().strftime("%d %b %Y, %I:%M:%S %p")
 
 
def get_cache_token():
    return (st.session_state.file_mtime, st.session_state.refresh_nonce)
 
 
if "file_mtime" not in st.session_state:
    st.session_state.file_mtime = get_file_version()
 
if "last_refresh_label" not in st.session_state:
    st.session_state.last_refresh_label = get_local_timestamp_label()
 
 
def set_refresh_timestamp():
    st.session_state.last_refresh_label = get_local_timestamp_label()
 
 
@st.fragment(run_every=AUTO_REFRESH_INTERVAL)
def watch_excel_file():
    return
 
 
watch_excel_file()
 
st.markdown(
    """
    <style>
    header[data-testid="stHeader"] {
        display: none;
    }
    .stAppDeployButton {
        display: none;
    }
    div[data-testid="stToolbar"] {
        display: none;
    }
    #MainMenu {
        visibility: hidden;
    }
    :root {
        --brand-maroon: #7a1f28;
        --brand-maroon-dark: #58131a;
        --brand-maroon-light: #a53d49;
        --brand-blue: #0f8ec7;
        --brand-blue-deep: #0c5a78;
        --brand-ink: #1e293b;
        --brand-muted: #64748b;
        --surface: rgba(255, 255, 255, 0.9);
        --surface-strong: rgba(255, 255, 255, 0.98);
        --border-soft: rgba(15, 142, 199, 0.16);
        --shadow-soft: 0 18px 48px rgba(15, 23, 42, 0.08);
        --shadow-strong: 0 24px 64px rgba(15, 23, 42, 0.12);
    }
    a[data-testid="stAnchorLink"] {
        display: none !important;
    }
    footer,
    footer *,
    button[title="Open navigation menu"],
    button[aria-label="Open navigation menu"],
    button[title="Streamlit menu"],
    [data-testid="collapsedControl"],
    div[data-testid="stDecoration"] {
        display: none !important;
        visibility: hidden !important;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type {
        position: sticky;
        top: 0;
        z-index: 1000;
        align-items: center;
        background:
            linear-gradient(135deg, rgba(255, 255, 255, 0.94), rgba(244, 249, 255, 0.97)),
            radial-gradient(circle at top left, rgba(15, 142, 199, 0.12), transparent 38%);
        backdrop-filter: blur(14px);
        padding: 0.65rem 0.35rem;
        border: 1px solid rgba(255, 255, 255, 0.55);
        border-radius: 26px;
        box-shadow: var(--shadow-soft);
        margin-bottom: 0.55rem;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type > div[data-testid="column"] {
        display: flex;
        align-items: center;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type > div[data-testid="column"] > div {
        width: 100%;
        min-height: 58px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type > div[data-testid="column"]:first-child > div {
        justify-content: flex-start;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type > div[data-testid="column"]:last-child > div {
        justify-content: center;
        align-items: flex-end;
        flex-direction: column;
        gap: 0.12rem;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type h1 {
        margin: 0 !important;
        width: 100%;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type div[data-testid="stButton"] {
        width: min(250px, 100%);
        margin: 0 !important;
        display: flex;
        align-items: center;
        justify-content: flex-end;
        height: auto;
    }
    div[data-testid="stHorizontalBlock"]:first-of-type div[data-testid="stButton"] > button {
        margin: 0 !important;
        width: 100%;
    }
    .stApp {
        font-family: "Segoe UI Variable", "Aptos", "Trebuchet MS", sans-serif;
        background:
            radial-gradient(circle at top left, rgba(15, 142, 199, 0.16), transparent 28%),
            radial-gradient(circle at top right, rgba(122, 31, 40, 0.13), transparent 26%),
            linear-gradient(180deg, #eef6fc 0%, #f8fbff 24%, #ffffff 50%, #f7fafc 100%);
    }
    .block-container {
        padding-top: 0.2rem;
        padding-bottom: 2.4rem;
        max-width: 1780px;
        padding-left: 1.15rem;
        padding-right: 1.15rem;
    }
    h1 {
        font-size: clamp(2.35rem, 2.8vw, 3.35rem) !important;
        line-height: 1.2 !important;
        margin-bottom: 0.15rem !important;
        word-break: break-word;
        color: var(--brand-ink) !important;
        text-align: center;
        letter-spacing: -0.04em;
        font-weight: 800 !important;
    }
    h3 {
        color: var(--brand-ink) !important;
        letter-spacing: -0.03em;
    }
    .logo-row {
        display: flex;
        align-items: center;
        gap: 0.9rem;
        flex-wrap: nowrap;
        white-space: nowrap;
        padding: 0.4rem 0.55rem;
        min-height: 58px;
        background: rgba(255, 255, 255, 0.82);
        border: 1px solid rgba(255, 255, 255, 0.7);
        border-radius: 18px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.55);
    }
    .logo-row img {
        height: 44px;
        width: auto;
        object-fit: contain;
        display: block;
        flex: 0 0 auto;
    }
    .logo-row img[alt="flybuys"] {
        height: 42px;
    }
    .logo-row img[alt="musigma"] {
        height: 42px;
    }
    .refresh-meta {
        display: flex;
        justify-content: flex-end;
        margin-bottom: 0;
        width: min(250px, 90%);
        height: auto;
    }
    .refresh-pill {
        display: block;
        padding: 0;
        font-size: 0.78rem;
        color: var(--brand-muted);
        width: 100%;
        margin-top: -0.25rem;
        text-align: center;
    }
    .section-heading {
        font-size: 1.55rem;
        font-weight: 800;
        color: var(--brand-ink);
        margin: 0 0 0.55rem 0;
        letter-spacing: -0.03em;
    }
    .filter-shell {
        padding: 1rem 1.1rem 0.35rem 1.1rem;
        border: 1px solid rgba(0, 146, 210, 0.16);
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(248, 250, 252, 0.98));
        margin-bottom: 1.5rem;
        box-shadow: var(--shadow-soft);
    }
    .filter-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: #102a43;
        margin-bottom: 0.2rem;
    }
    .filter-subtitle {
        font-size: 0.9rem;
        color: #475569;
        margin-bottom: 0.59rem;
    }
    div[data-testid="stTextInput"],
    div[data-testid="stSelectbox"],
    div[data-testid="stRadio"],
    div[data-testid="stMultiselect"],
    div[data-testid="stDateInput"] {
        border-radius: 18px !important;
        border: 1px solid rgba(0, 146, 210, 0.14) !important;
        background: #ffffff !important;
        box-shadow: 0 14px 32px rgba(15, 23, 42, 0.05) !important;
        padding: 0.45rem 0.6rem !important;
    }
    div[data-testid="stTextInput"] input,
    div[data-testid="stSelectbox"] select,
    div[data-testid="stMultiselect"] input {
        border-radius: 14px !important;
        background: #f7fbff !important;
        font-size: 1rem !important;
    }
    div[data-testid="stTextInput"] label p,
    div[data-testid="stRadio"] label p,
    div[data-testid="stMultiselect"] label p {
        font-weight: 600 !important;
        color: var(--brand-ink) !important;
    }
    .filter-subtitle {
        font-size: 0.9rem;
        color: #4b5563;
        margin-bottom: 0.75rem;
    }
    .filter-actions {
        margin-top: 1.15rem;
    }
    div[data-testid="stExpander"] {
        margin-bottom: 0.6rem;
    }
    div[data-testid="stExpander"] details {
        border: 1px solid rgba(0, 146, 210, 0.16);
        border-radius: 20px;
        background: rgba(255, 255, 255, 0.94);
        box-shadow: 0 14px 34px rgba(15, 23, 42, 0.06);
        overflow: hidden;
    }
    div[data-testid="stExpander"] summary {
        padding: 0.6rem 1rem !important;
    }
    div[data-testid="stExpander"] summary p {
        font-size: 1rem !important;
        font-weight: 700 !important;
        color: var(--brand-ink) !important;
    }
    div[data-testid="stExpander"] details[open] summary {
        border-bottom: 1px solid rgba(0, 146, 210, 0.12);
    }
    div[data-testid="stModalBackground"] {
        backdrop-filter: blur(6px);
        background: rgba(15, 23, 42, 0.25);
    }
    div[data-testid="stButton"] button[kind="primary"] {
        background: linear-gradient(135deg, var(--brand-maroon-dark), var(--brand-maroon-light)) !important;
        border: none !important;
        color: #ffffff !important;
        font-weight: 700 !important;
        border-radius: 16px !important;
        min-height: 50px !important;
        box-shadow: 0 16px 30px rgba(122, 31, 40, 0.24);
    }
    div[data-testid="stButton"] button:not([kind="primary"]) {
        border: 1px solid rgba(15, 142, 199, 0.55) !important;
        color: var(--brand-maroon) !important;
        background: rgba(255, 255, 255, 0.92) !important;
        border-radius: 16px !important;
        min-height: 50px !important;
        font-weight: 600 !important;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(0, 146, 210, 0.18);
        border-radius: 18px;
        overflow: hidden;
        box-shadow: 0 16px 34px rgba(15, 23, 42, 0.08);
    }
    div[data-testid="stDataFrame"] [data-testid="stHeader"] {
        background: linear-gradient(180deg, rgba(0, 146, 210, 0.09), rgba(0, 146, 210, 0.04));
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(247,250,253,0.96));
        border: 1px solid rgba(0, 146, 210, 0.14);
        border-radius: 22px;
        padding: 1rem 1.1rem 0.9rem 1.1rem;
        box-shadow: var(--shadow-soft);
        min-height: 118px;
    }
    div[data-testid="stMetricLabel"] {
        justify-content: center;
    }
    div[data-testid="stMetricLabel"] p {
        font-weight: 600;
        color: #334155;
        text-align: center;
    }
    div[data-testid="stMetricValue"] {
        justify-content: center;
    }
    div[data-testid="stMetricValue"] > div {
        text-align: center;
    }
    .person-table-wrap {
        border: 1px solid rgba(0, 146, 210, 0.14);
        border-radius: 24px;
        overflow: hidden;
        background: rgba(255,255,255,0.98);
        box-shadow: var(--shadow-strong);
        margin-bottom: 1rem;
    }
    .person-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
    }
    .person-table th {
        padding: 0.86rem 0.8rem;
        text-align: center;
        font-size: 0.8rem;
        font-weight: 700;
        color: var(--brand-ink);
        background: linear-gradient(180deg, rgba(0, 146, 210, 0.11), rgba(0, 146, 210, 0.03));
        border-bottom: 1px solid rgba(0, 146, 210, 0.12);
    }
    .person-table td {
        padding: 0.75rem 0.8rem;
        text-align: center;
        font-size: 0.91rem;
        color: #1f2937;
        border-bottom: 1px solid #eef2f6;
        vertical-align: middle;
    }
    .person-table tbody tr:nth-child(even) td {
        background: rgba(248, 251, 255, 0.7);
    }
    .person-table tbody tr:last-child td {
        border-bottom: none;
    }
    .person-table tbody tr:hover td {
        background: linear-gradient(90deg, rgba(0, 146, 210, 0.08), rgba(0, 146, 210, 0.02));
    }
    .person-table .detail-col {
        width: 84px;
    }
    .person-name {
        font-weight: 700;
        color: #132238;
    }
    .count-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 42px;
        padding: 0.38rem 0.7rem;
        border-radius: 999px;
        font-weight: 700;
        font-size: 0.82rem;
        border: 1px solid transparent;
    }
    .count-badge.total {
        background: rgba(30, 41, 59, 0.08);
        color: var(--brand-ink);
    }
    .count-badge.escaped {
        background: rgba(122, 31, 40, 0.1);
        color: var(--brand-maroon-dark);
        border-color: rgba(122, 31, 40, 0.16);
    }
    .count-badge.internal {
        background: rgba(15, 142, 199, 0.1);
        color: var(--brand-blue-deep);
        border-color: rgba(15, 142, 199, 0.16);
    }
    .detail-link {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 38px;
        height: 38px;
        border-radius: 14px;
        background: linear-gradient(135deg, rgba(122, 31, 40, 0.12), rgba(15, 142, 199, 0.14));
        border: 1px solid rgba(15, 142, 199, 0.14);
        color: var(--brand-maroon);
        text-decoration: none;
        transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease;
    }
    .detail-link:hover {
        background: linear-gradient(135deg, rgba(128, 0, 0, 0.16), rgba(0, 146, 210, 0.2));
        transform: translateY(-1px);
        box-shadow: 0 8px 18px rgba(0, 146, 210, 0.18);
    }
    .detail-link svg {
        width: 16px;
        height: 16px;
        stroke: currentColor;
    }
    @media (max-width: 960px) {
    }
    @media (max-width: 640px) {
        h1 {
            text-align: left;
        }
        .block-container {
            padding-left: 0.85rem;
            padding-right: 0.85rem;
        }
    }
    .person-row button {
        min-height: 36px !important;
        border-radius: 10px !important;
    }
    .person-row button > div {
        font-size: 1.2rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
 
logo_col, title_col, refresh_col = st.columns([2.2, 5.2, 1.6])
with logo_col:
    fly_logo = image_to_base64(ASSET_DIR / "flybuys.png")
    mu_logo = image_to_base64(ASSET_DIR / "Musigma.png")
    if fly_logo or mu_logo:
        st.markdown(
            textwrap.dedent(
                f"""
                <div class="logo-row">
                    {"<img src='data:image/png;base64," + fly_logo + "' alt='flybuys' />" if fly_logo else ""}
                    {"<img src='data:image/png;base64," + mu_logo + "' alt='musigma' />" if mu_logo else ""}
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )
with title_col:
    st.title("Error Responsibility Dashboard")
with refresh_col:
    refresh_clicked = st.button(
        "Refresh Data",
        type="primary",
        use_container_width=True,
    )
    st.markdown(
        f'<div class="refresh-meta"><span class="refresh-pill">Last refreshed: {st.session_state.last_refresh_label}</span></div>',
        unsafe_allow_html=True,
    )
 
file_mtime = get_cache_token()
try:
    if refresh_clicked:
        with st.spinner("Refreshing data from Excel sheets..."):
            source_df, name_mapping = refresh_workbook_bundle()
    else:
        source_df, name_mapping = load_workbook_bundle(file_mtime)
except (ConnectionError, ValueError) as exc:
    st.error(str(exc))
    st.stop()
 
responsibility_df = build_responsibility_df(source_df)
responsibility_df["responsible_name"] = responsibility_df["responsible_person"].map(
    lambda short_form: name_mapping.get(clean_text(short_form).upper(), "")
)
selected_person = st.query_params.get("person", "")
 
required_columns = {"pod", "error_type", "type_of_deliverable"}
missing_columns = [column for column in required_columns if column not in source_df.columns]
if missing_columns:
    st.error(f"Missing expected columns in Excel: {', '.join(missing_columns)}")
    st.stop()
 
if responsibility_df.empty:
    st.warning("No Error values were found to build the dashboard.")
    st.stop()
 
if selected_person:
    all_selected_person_df = responsibility_df[
        responsibility_df["responsible_person"].astype(str) == str(selected_person)
    ].copy()
    selected_person_df = all_selected_person_df.copy()
 
    if selected_person_df.empty:
        st.info("Not available")
        if st.button("Back to Dashboard"):
            st.query_params.clear()
            st.rerun()
        st.stop()
 
    person_search_col, person_action_col = st.columns([4, 1.4])
    with person_search_col:
        person_search = st.text_input(
            "Search",
            placeholder="Search this person's errors, deliverables",
            key="person_search",
        )
    with person_action_col:
        st.markdown('<div class="filter-actions"></div>', unsafe_allow_html=True)
        if st.button("Back to Dashboard", use_container_width=True):
            st.query_params.clear()
            st.rerun()
 
    person_classification_mode = st.radio(
        "View",
        ["Escaped", "Internal", "Both"],
        horizontal=True,
        key="person_classification_mode",
        index=2,
    )
 
    person_filter_col1, person_filter_col2, person_filter_col3 = st.columns(3)
 
    person_deliverable_options = sorted(
        [value for value in selected_person_df["type_of_deliverable"].dropna().unique() if value != ""]
    )
    with person_filter_col1:
        person_selected_deliverables = st.multiselect(
            "Type of Deliverable",
            person_deliverable_options,
            default=[],
            placeholder="All deliverable types",
            key="person_deliverable_filter",
        )
 
    person_error_type_options = sorted(
        [value for value in selected_person_df["error_type"].dropna().unique() if value != ""]
    )
    with person_filter_col2:
        person_selected_error_types = st.multiselect(
            "Error Type",
            person_error_type_options,
            default=[],
            placeholder="All error types",
            key="person_error_type_filter",
        )
 
    person_qh_options = sorted(
        [value for value in selected_person_df["discussed_in_qh"].dropna().unique() if value != ""]
    ) if "discussed_in_qh" in selected_person_df.columns else []
    with person_filter_col3:
        person_selected_qh = st.multiselect(
            "Discussed in QH",
            person_qh_options,
            default=[],
            placeholder="All QH values",
            key="person_qh_filter",
        )
 
    if person_selected_deliverables:
        selected_person_df = selected_person_df[
            selected_person_df["type_of_deliverable"].isin(person_selected_deliverables)
        ].copy()
 
    if person_selected_error_types:
        selected_person_df = selected_person_df[
            selected_person_df["error_type"].isin(person_selected_error_types)
        ].copy()
 
    if person_selected_qh and "discussed_in_qh" in selected_person_df.columns:
        selected_person_df = selected_person_df[
            selected_person_df["discussed_in_qh"].isin(person_selected_qh)
        ]
 
    if person_classification_mode != "Both" and "error_classification" in selected_person_df.columns:
        selected_person_df = selected_person_df[
            selected_person_df["error_classification"].eq(person_classification_mode)
        ]
 
    if person_search:
        selected_person_df = apply_search_filter(selected_person_df, person_search)
 
    detail_columns = [
        "error_id",
        "rollout_date",
        "deliverable_name",
        "type_of_deliverable",
        "pod",
        "error",
        "error_type",
        "error_classification",
        "error_description",
        "impact",
        "mitigation",
        "discussed_in_qh",
    ]
    detail_columns = [
        column for column in detail_columns if column in selected_person_df.columns
    ]
 
    if selected_person_df.empty:
        st.info("Not available")
    else:
        st.metric("Total Errors", int(all_selected_person_df["responsibility_count"].sum()))
        selected_person_df = selected_person_df[detail_columns].sort_values(
            by=[column for column in ["rollout_date", "error_id"] if column in detail_columns],
            ascending=[False, False] if "rollout_date" in detail_columns else [False],
        )
        st.dataframe(selected_person_df, width="stretch", hide_index=True)
 
    st.stop()
 
def reset_filters():
    st.session_state["dashboard_search"] = ""
    st.session_state["dashboard_pods"] = []
    st.session_state["dashboard_deliverables"] = []
    st.session_state["dashboard_error_types"] = []
    st.session_state["dashboard_roles"] = []
    st.session_state["dashboard_people"] = []
    st.session_state["dashboard_qh"] = []
    st.session_state["dashboard_classification_mode"] = "Escaped"
 
st.markdown('<div class="section-heading">Search and Filter the Dashboard</div>', unsafe_allow_html=True)
search_col, action_col = st.columns([4, 1])
with search_col:
    search = st.text_input(
        "Search",
        placeholder="Search by name, error, deliverable",
        key="dashboard_search",
    )
with action_col:
    st.markdown('<div class="filter-actions"></div>', unsafe_allow_html=True)
    st.button("Reset Filters", use_container_width=True, on_click=reset_filters)
 
classification_mode = st.session_state.get("dashboard_classification_mode", "Escaped")
 
dashboard_panel = st.expander("Filters", expanded=False)
with dashboard_panel:
    pod_options = sorted([value for value in responsibility_df["pod"].dropna().unique() if value != ""])
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        selected_pods = st.multiselect(
            "POD",
            pod_options,
            default=[],
            placeholder="All PODs",
            key="dashboard_pods",
        )
 
    deliverable_options = sorted(
        [value for value in responsibility_df["type_of_deliverable"].dropna().unique() if value != ""]
    )
    with filter_col2:
        selected_deliverables = st.multiselect(
            "Type of Deliverable",
            deliverable_options,
            default=[],
            placeholder="All deliverable types",
            key="dashboard_deliverables",
        )
 
    error_type_options = sorted(
        [value for value in responsibility_df["error_type"].dropna().unique() if value != ""]
    )
    with filter_col3:
        selected_error_types = st.multiselect(
            "Error Type",
            error_type_options,
            default=[],
            placeholder="All error types",
            key="dashboard_error_types",
        )
 
    person_options = sorted(
        [value for value in responsibility_df["responsible_person"].dropna().unique() if value != ""]
    )
    qh_options = sorted(
        [value for value in responsibility_df["discussed_in_qh"].dropna().unique() if value != ""]
    ) if "discussed_in_qh" in responsibility_df.columns else []
    filter_col4, filter_col5, filter_col6 = st.columns(3)
    with filter_col4:
        selected_people = st.multiselect(
            "Responsible Person",
            person_options,
            default=[],
            placeholder="All people",
            key="dashboard_people",
        )
    with filter_col5:
        selected_qh = st.multiselect(
            "Discussed in QH",
            qh_options,
            default=[],
            placeholder="All QH values",
            key="dashboard_qh",
        )
    with filter_col6:
        st.write("")
 
filtered_responsibility_df = responsibility_df.copy()
filtered_responsibility_df = apply_optional_filter(filtered_responsibility_df, "pod", selected_pods)
filtered_responsibility_df = apply_optional_filter(
    filtered_responsibility_df,
    "type_of_deliverable",
    selected_deliverables,
)
filtered_responsibility_df = apply_optional_filter(
    filtered_responsibility_df,
    "error_type",
    selected_error_types,
)
filtered_responsibility_df = apply_optional_filter(
    filtered_responsibility_df,
    "responsible_person",
    selected_people,
)
if selected_qh and "discussed_in_qh" in filtered_responsibility_df.columns:
    filtered_responsibility_df = filtered_responsibility_df[
        filtered_responsibility_df["discussed_in_qh"].isin(selected_qh)
    ]
 
if search:
    filtered_responsibility_df = apply_search_filter(filtered_responsibility_df, search)
 
filtered_responsibility_all_views_df = filtered_responsibility_df.copy()
 
if classification_mode != "Both" and "error_classification" in filtered_responsibility_df.columns:
    filtered_responsibility_df = filtered_responsibility_df[
        filtered_responsibility_df["error_classification"].eq(classification_mode)
    ]
 
filtered_error_ids = filtered_responsibility_df["error_id"].unique()
filtered_source_df = source_df[source_df["error_id"].isin(filtered_error_ids)].copy()
 
if filtered_responsibility_df.empty:
    st.info("Not available")
    st.stop()
 
with dashboard_panel:
    metric_spacer_left, col1, col2, col3, metric_spacer_right = st.columns([0.18, 1, 1, 1, 0.18])
    col1.metric("Error Records", int(filtered_source_df["error_id"].nunique()))
    col2.metric("People Involved", int(filtered_responsibility_df["responsible_person"].nunique()))
 
    qh_yes_count = 0
    if "discussed_in_qh" in filtered_source_df.columns:
        qh_yes_count = int(
            filtered_source_df["discussed_in_qh"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("yes")
            .sum()
        )
    col3.metric("Discussed in QH", qh_yes_count)
 
with st.expander("Insights", expanded=False):
    chart_col1, chart_col2, chart_col3 = st.columns(3)
 
    errors_by_person = (
        filtered_responsibility_df.groupby("responsible_person")["responsibility_count"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .reset_index()
    )
    errors_by_pod = (
        filtered_source_df.groupby("pod")["error_id"]
        .nunique()
        .sort_values(ascending=False)
        .reset_index()
    )
    errors_by_type = (
        filtered_source_df.groupby("error_type")["error_id"]
        .nunique()
        .sort_values(ascending=False)
        .reset_index()
    )
 
    common_y_max = max(
        1,
        int(errors_by_person["responsibility_count"].max()) if not errors_by_person.empty else 0,
        int(errors_by_pod["error_id"].max()) if not errors_by_pod.empty else 0,
        int(errors_by_type["error_id"].max()) if not errors_by_type.empty else 0,
    )
 
    with chart_col1:
        st.write("Errors by Responsible Person")
        st.altair_chart(
            build_fixed_bar_chart(
                errors_by_person,
                "responsible_person",
                "responsibility_count",
                common_y_max,
            ),
            width="stretch",
        )
 
    with chart_col2:
        st.write("POD-wise Error Count")
        st.altair_chart(
            build_fixed_bar_chart(errors_by_pod, "pod", "error_id", common_y_max),
            width="stretch",
        )
 
    with chart_col3:
        st.write("Error Count by Error Type")
        st.altair_chart(
            build_fixed_bar_chart(errors_by_type, "error_type", "error_id", common_y_max),
            width="stretch",
        )
 
classification_mode = st.radio(
    "View",
    ["Escaped", "Internal", "Both"],
    horizontal=True,
    key="dashboard_classification_mode",
)
 
st.markdown('<div class="section-heading">Person-wise Count</div>', unsafe_allow_html=True)
person_summary_df = (
    filtered_responsibility_all_views_df.groupby("responsible_person", as_index=False)["responsibility_count"]
    .sum()
    .rename(columns={"responsibility_count": "total_errors"})
    .sort_values(by=["total_errors", "responsible_person"], ascending=[False, True])
)
person_summary_df["name"] = person_summary_df["responsible_person"].map(
    lambda short_form: name_mapping.get(clean_text(short_form).upper(), "")
)
classification_summary_df = (
    filtered_responsibility_all_views_df.groupby(["responsible_person", "error_classification"])["responsibility_count"]
    .sum()
    .unstack(fill_value=0)
    .reset_index()
)
for column in ["Escaped", "Internal"]:
    if column not in classification_summary_df.columns:
        classification_summary_df[column] = 0
classification_summary_df = classification_summary_df.rename(
    columns={"Escaped": "escaped_count", "Internal": "internal_count"}
)
person_summary_df = person_summary_df.merge(classification_summary_df, on="responsible_person", how="left")
person_summary_df = person_summary_df[person_summary_df["name"].map(clean_text) != ""].copy()
person_summary_df["escaped_count"] = person_summary_df["escaped_count"].fillna(0).astype(int)
person_summary_df["internal_count"] = person_summary_df["internal_count"].fillna(0).astype(int)
if classification_mode == "Escaped":
    person_summary_df = person_summary_df[person_summary_df["escaped_count"] > 0].copy()
elif classification_mode == "Internal":
    person_summary_df = person_summary_df[person_summary_df["internal_count"] > 0].copy()
person_summary_df["details"] = person_summary_df["responsible_person"].map(
    lambda person: f"?person={quote_plus(str(person))}"
)
 
detail_icon = """
<svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
    <path d="M5 12h12" stroke-width="2" stroke-linecap="round"/>
    <path d="M13 6l6 6-6 6" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
</svg>
""".strip()
 
person_table_columns = [
    ("Responsible Person", "responsible_person", "16%"),
    ("Name", "name", "30%"),
]
 
if classification_mode in {"Escaped", "Both"}:
    person_table_columns.append(("Escaped", "escaped_count", "12%"))
if classification_mode in {"Internal", "Both"}:
    person_table_columns.append(("Internal", "internal_count", "12%"))
 
person_table_columns.extend(
    [
        ("Total Errors", "total_errors", "12%"),
        ("Details", "details", "84px"),
    ]
)
 
person_table_rows = []
for row in person_summary_df.itertuples(index=False):
    person_name = html.escape(str(row.responsible_person))
    full_name = html.escape(str(row.name))
    details_href = html.escape(str(row.details), quote=True)
    row_values = {
        "responsible_person": f'<span class="person-name">{person_name}</span>',
        "name": full_name,
        "escaped_count": f'<span class="count-badge escaped">{int(row.escaped_count)}</span>',
        "internal_count": f'<span class="count-badge internal">{int(row.internal_count)}</span>',
        "total_errors": f'<span class="count-badge total">{int(row.total_errors)}</span>',
        "details": (
            f'<a class="detail-link" href="{details_href}" target="_self" title="View {person_name} details">'
            f"{detail_icon}</a>"
        ),
    }
    row_cells = "".join(f"<td>{row_values[column_key]}</td>" for _, column_key, _ in person_table_columns)
    person_table_rows.append(
        f"""
        <tr>
            {row_cells}
        </tr>
        """.strip()
    )
 
person_table_headers = "".join(
    f'<th class="{"detail-col" if column_key == "details" else ""}">{label}</th>'
    for label, column_key, _ in person_table_columns
)
person_table_colgroup = "".join(
    f'<col class="{"detail-col" if column_key == "details" else ""}" style="width: {width};" />'
    for _, column_key, width in person_table_columns
)
 
st.markdown(
    textwrap.dedent(
        f"""
        <div class="person-table-wrap">
            <table class="person-table">
                <colgroup>
                    {person_table_colgroup}
                </colgroup>
                <thead>
                    <tr>
                        {person_table_headers}
                    </tr>
                </thead>
                <tbody>
                    {''.join(person_table_rows)}
                </tbody>
            </table>
        </div>
        """
    ).strip(),
    unsafe_allow_html=True,
)
 
with st.expander("Original Error Log", expanded=False):
    source_columns = [
        "error_id",
        "rollout_date",
        "monday_number",
        "ib_number",
        "deliverable_name",
        "type_of_deliverable",
        "owner",
        "peer_1",
        "peer_2",
        "pod",
        "error",
        "error_type",
        "error_classification",
        "error_description",
        "impact",
        "mitigation",
        "discussed_in_qh",
    ]
    source_columns = [column for column in source_columns if column in filtered_source_df.columns]
    st.dataframe(filtered_source_df[source_columns], width="stretch", hide_index=True)
 
 
 
 
 