#!/usr/bin/env python3
"""
LA Location Search Toolkit.
Gets a fresh Cognito token on each run, reads input CSV, calls location/search API per row,
and returns id and company_id from each response.
"""
import json
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import date, timedelta

import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CAPABILITY_NAME = "location_search"

# Strict CSV column mapping. alias_source is derived from template_flag, vendorno, description, monday_group_name, consignee.
REQUIRED_CSV_COLUMNS_PICKUP = {
    "alias_value": "vendorno",
    "street_address": "shipfrom_street",
}
REQUIRED_CSV_COLUMNS_DELIVERY = {
    "street_address": "shipto_street",
}
# Columns used to derive alias_source (PEPSI TENDER / SOBEYS TENDER logic)
ALIAS_SOURCE_COLUMNS = ("template_flag", "vendorno", "description", "monday_group_name", "consignee")
DEFAULT_INCLUDE_COMMODITIES = True
LOCATION_TYPE_PICKUP = "Pickup"
LOCATION_TYPE_DELIVERY = "Delivery"
LOCATION_TYPE_BOTH = "both"  # Run both Pickup and Delivery in one execution


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load config from path or default location next to main.py."""
    if config_path and Path(config_path).exists():
        path = Path(config_path)
    else:
        path = Path(__file__).resolve().parent / "config.json"
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_access_token(config: Dict[str, Any]) -> str:
    """
    Request a new OAuth2 client_credentials token from Cognito.
    Token expires in 1 hour; call this on every toolkit run.
    """
    cognito = config.get("cognito", {})
    token_url = cognito.get("token_url")
    if not token_url and cognito.get("cognito_domain"):
        token_url = cognito["cognito_domain"].rstrip("/")+ "/oauth2/token"
    client_id = cognito.get("client_id")
    client_secret = cognito.get("client_secret")
    scope = cognito.get("scope") or cognito.get("scopes", "default")

    if not all([token_url, client_id, client_secret]):
        raise ValueError("config.json must have cognito.token_url (or cognito_domain), client_id, client_secret")

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(token_url, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    access_token = body.get("access_token")
    if not access_token:
        raise ValueError("No access_token in Cognito response")
    logger.info("Obtained access token (expires_in=%s)", body.get("expires_in"))
    return access_token


def search_location(
    access_token: str,
    search_url: str,
    street_address: str,
    search_type: str,
    alias_source: Optional[str] = None,
    alias_value: Optional[str] = None,
    include_commodities: bool = True,
) -> Dict[str, Any]:
    """Call location/search API. Pickup: alias_source, alias_value, street_address, include_commodities. Delivery: street_address only."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    payload: Dict[str, Any] = {
        "street_address": street_address.strip(),
        "type": search_type,
    }
    if search_type == LOCATION_TYPE_PICKUP and alias_source is not None and alias_value is not None:
        payload["alias_source"] = alias_source
        payload["alias_value"] = str(alias_value)
        payload["include_commodities"] = include_commodities
    resp = requests.post(search_url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_location_results(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """From location/search response, return list of {id, company_id, province} for each location."""
    locations = api_response.get("locations") or []
    out = []
    for loc in locations:
        location_obj = loc.get("location") or {}
        province = location_obj.get("province")
        out.append({
            "id": loc.get("id"),
            "company_id": loc.get("company_id"),
            "province": province,
        })
    return out


def _norm(s: Any) -> str:
    return str(s or "").strip().casefold()


def get_alias_source_from_row(row: Any, _cell_str: Any) -> str:
    """
    Derive alias_source from CSV row. Used for both Pickup and Delivery.
    PEPSI TENDER: template_flag = "Pepsi" → vendorno 204047/21200Y/21200E → FOOD vendor / BEVERAGE / QUAKER.
    SOBEYS TENDER: template_flag = "template-1" or Null → description/monday_group_name/consignee → M&M / LA6 / OTR / ADMIN.
    """
    template = _cell_str(row.get("template_flag"))
    vendorno = _cell_str(row.get("vendorno"))
    description = _cell_str(row.get("description"))
    monday_group = _cell_str(row.get("monday_group_name"))
    consignee = _cell_str(row.get("consignee"))

    # PEPSI TENDER
    if _norm(template) == "pepsi":
        if vendorno == "204047":
            return "PEPSI TENDER - FOOD vendor"
        if vendorno == "21200Y":
            return "PEPSI TENDER - BEVERAGE"
        if vendorno == "21200E":
            return "PEPSI TENDER - QUAKER"
        return ""

    # SOBEYS TENDER (template_flag = "template-1" or Null/empty)
    if _norm(template) in ("", "template-1"):
        if "M&M" in description:
            return "SOBEYS TENDER - M&M"
        if "NPOP (LA6)" in monday_group and "MIFLAOPS" in monday_group:
            return "SOBEYS TENDER - LA6"
        # OTR: no M&M in description, monday_group_name does not have NPOP (LA6)/{MIFLAOPS}.pdf, consignee has RSC8/RSC9/RSC12/CFC3
        npop_in_monday = "NPOP (LA6)" in monday_group or "MIFLAOPS" in monday_group
        consignee_upper = consignee.upper()
        if "M&M" not in description and not npop_in_monday:
            if "RSC8" in consignee_upper or "RSC9" in consignee_upper or "RSC12" in consignee_upper or "CFC3" in consignee_upper:
                return "SOBEYS TENDER - OTR"
        return "SOBEYS TENDER - ADMIN"

    return ""


def load_holidays(transit_time_xlsx_path: str) -> Set[date]:
    """Load holiday dates from 'Holidays' sheet into a set of date objects."""
    df = pd.read_excel(transit_time_xlsx_path, sheet_name="Holidays", engine="openpyxl")
    best_col = None
    best_count = 0
    for col in df.columns:
        parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        count = int(parsed.notna().sum())
        if count > best_count:
            best_col = col
            best_count = count
    if best_col is None or best_count == 0:
        return set()
    parsed = pd.to_datetime(df[best_col], errors="coerce", dayfirst=True)
    return {d.date() for d in parsed.dropna().tolist()}


def parse_any_date(value: Any) -> Optional[date]:
    """Parse common date formats (and Excel serial) into a date."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Compact format e.g. "21726" = 21-7-26 → 2026-07-21 (DD-M-YY)
    if s.isdigit():
        n = int(s)
        if len(s) == 5:
            day = int(s[0:2])
            month = int(s[2:3])
            year = int(s[3:5])
            if year < 100:
                year += 2000
            try:
                return date(year, month, day)
            except ValueError:
                pass
        if len(s) == 6:
            day = int(s[0:2])
            month = int(s[2:4])
            year = int(s[4:6])
            if year < 100:
                year += 2000
            try:
                return date(year, month, day)
            except ValueError:
                pass
        if n > 30000:  # Excel serial days
            ts = pd.to_datetime(n, unit="D", origin="1899-12-30", errors="coerce")
            if pd.notna(ts):
                return ts.date()

    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(ts):
        return ts.date()
    return None


def adjust_to_previous_working_day(d: date, holidays: Set[date]) -> date:
    """Move back to previous non-weekend, non-holiday date."""
    while d.weekday() >= 5 or d in holidays:
        d = d - timedelta(days=1)
    return d


def _build_mode_maps(mat: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str]]:
    """Strip index/columns and build normalized lookup maps. Drop unnamed/empty labels."""
    mat = mat.copy()
    mat.index = [str(x).strip() if pd.notna(x) else "" for x in mat.index]
    mat.columns = [str(x).strip() if pd.notna(x) else "" for x in mat.columns]
    # Drop rows/cols with empty or "Unnamed" labels
    mat = mat[mat.index != ""]
    mat = mat[[c for c in mat.columns if c != ""]]
    if not mat.index.is_unique:
        mat = mat[~mat.index.duplicated(keep="first")]
    row_map = {_norm(x): x for x in mat.index if x}
    col_map = {_norm(x): x for x in mat.columns if x}
    return mat, row_map, col_map


def load_mode_matrix(origins_destinations_xlsx_path: str) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str]]:
    """
    Load 'Modes' sheet: row 2 = delivery provinces (columns), column B = pickup provinces (rows).
    Pickup = row index, Delivery = column header. Lookup: mat.loc[pickup_province, delivery_province] -> ROAD/RAIL.
    """
    # Row 2 (0-based index 1) is the header row with delivery provinces; column B (index 1) has pickup provinces
    raw = pd.read_excel(
        origins_destinations_xlsx_path,
        sheet_name="Modes",
        engine="openpyxl",
        header=1,
    )
    if raw.empty or len(raw.columns) < 2:
        raise ValueError("Modes sheet is empty or not a matrix")
    # Column B = index 1 = pickup/origin provinces (row labels)
    origin_col = raw.columns[1]
    mat = raw.set_index(origin_col)
    mat, row_map, col_map = _build_mode_maps(mat)
    if not row_map or not col_map:
        raise ValueError("Modes sheet has no valid province labels in column B (rows) or row 2 (columns)")
    return mat, row_map, col_map


def get_mode_from_matrix(
    pickup_province: Optional[str],
    delivery_province: Optional[str],
    mat: Optional[pd.DataFrame],
    row_map: Optional[Dict[str, str]],
    col_map: Optional[Dict[str, str]],
) -> Optional[str]:
    if not pickup_province or not delivery_province or mat is None or row_map is None or col_map is None:
        return None
    r_key = row_map.get(_norm(pickup_province))
    c_key = col_map.get(_norm(delivery_province))
    if not r_key or not c_key:
        return None
    try:
        val = mat.at[r_key, c_key]
    except (KeyError, TypeError):
        return None
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip()


# Pepsi: CN Customer Reference # column for customer_shipment and pickup_appointment (user may override later)
PEPSI_CN_REFERENCE_COLUMN = "pickApptNo"


def build_shipment_payload(
    row: Any,
    pickup_result: Optional[Dict[str, Any]],
    delivery_result: Optional[Dict[str, Any]],
    _cell_str: Any,
    holidays: Optional[Set[date]],
    mode_matrix: Optional[pd.DataFrame],
    mode_row_map: Optional[Dict[str, str]],
    mode_col_map: Optional[Dict[str, str]],
    is_pepsi: bool = False,
    pepsi_mode_matrix: Optional[pd.DataFrame] = None,
    pepsi_mode_row_map: Optional[Dict[str, str]] = None,
    pepsi_mode_col_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build shipment payload. If is_pepsi (template_flag=Pepsi): Pepsi payload shape and Pepsi mode sheet.
    Else: current (non-Pepsi) payload shape and origins_destinations mode sheet.
    """
    def _get_cell(col: str, default: Any = None) -> Any:
        val = _cell_str(row.get(col))
        return val if val else default

    def _get_float(col: str, default: float = 0.0) -> float:
        val = _get_cell(col)
        if not val:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def _get_int(col: str, default: int = 0) -> int:
        val = _get_cell(col)
        if not val:
            return default
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return default

    # pickup_date and delivery_date: parse and output YYYY-MM-DD
    pickup_date_raw = _get_cell("pickupDate", "")
    pickup_date_parsed = parse_any_date(pickup_date_raw)
    if pickup_date_parsed:
        if holidays is not None:
            pickup_date_parsed = adjust_to_previous_working_day(pickup_date_parsed, holidays)
        pickup_date_out = pickup_date_parsed.strftime("%Y-%m-%d")
    else:
        pickup_date_out = pickup_date_raw

    delivery_date_raw = _get_cell("delDate", "")
    delivery_date_parsed = parse_any_date(delivery_date_raw)
    delivery_date_out = delivery_date_parsed.strftime("%Y-%m-%d") if delivery_date_parsed else delivery_date_raw

    # Mode: Pepsi uses pepsi sheet, non-Pepsi uses origins_destinations sheet
    if is_pepsi and pepsi_mode_matrix is not None and pepsi_mode_row_map is not None and pepsi_mode_col_map is not None:
        mode_val = get_mode_from_matrix(
            pickup_result.get("province") if pickup_result else None,
            delivery_result.get("province") if delivery_result else None,
            pepsi_mode_matrix,
            pepsi_mode_row_map,
            pepsi_mode_col_map,
        ) or ""
    else:
        mode_val = get_mode_from_matrix(
            pickup_result.get("province") if pickup_result else None,
            delivery_result.get("province") if delivery_result else None,
            mode_matrix,
            mode_row_map,
            mode_col_map,
        ) or ""

    quantities_block = {
        "weight": _get_float("weight", 0.0),
        "weight_unit": "lbs",
        "cube": _get_float("cubes", 0.0),
        "cube_unit": "ft3",
        "cases": _get_int("cases", 0),
        "lifts": _get_int("lifts", 0),
        "pallets": _get_int("pallets", 0),
    }

    if is_pepsi:
        # Pepsi payload structure
        cn_ref = _get_cell(PEPSI_CN_REFERENCE_COLUMN, "")
        payload = {
            "description": "Shipment Creation by BOT (PEPSI)",
            "customer_shipment": cn_ref,
            "purchase_order": _get_cell("po", ""),
            "invoice_reference": _get_cell("invoiceRef", ""),
            "dates": {
                "pickup_date": pickup_date_out,
                "pickup_appointment": cn_ref,
                "delivery_date": delivery_date_out,
            },
            "quantities": {
                "declared": dict(quantities_block),
                "current": dict(quantities_block),
            },
            "locations": {
                "origin": str(pickup_result["id"]) if pickup_result and pickup_result.get("id") else "",
                "destination": str(pickup_result["company_id"]) if pickup_result and pickup_result.get("company_id") else "",
            },
            "parties": {
                "customer": str(delivery_result["id"]) if delivery_result and delivery_result.get("id") else "",
                "client": str(delivery_result["company_id"]) if delivery_result and delivery_result.get("company_id") else "",
            },
            "service": {
                "mode": mode_val,
                "service": "LTL",
                "temperature": "DRY",  # TODO: calculate based on logic to be provided
            },
        }
    else:
        # Non-Pepsi (current) payload structure
        payload = {
            "description": "Shipment Creation by BOT",
            "purchase_order": _get_cell("po", ""),
            "dates": {
                "pickup_date": pickup_date_out,
                "delivery_date": delivery_date_out,
            },
            "quantities": {
                "declared": dict(quantities_block),
                "current": dict(quantities_block),
            },
            "locations": {
                "origin": str(pickup_result["id"]) if pickup_result and pickup_result.get("id") else "",
                "destination": str(pickup_result["company_id"]) if pickup_result and pickup_result.get("company_id") else "",
            },
            "parties": {
                "customer": str(delivery_result["id"]) if delivery_result and delivery_result.get("id") else "",
                "client": str(delivery_result["company_id"]) if delivery_result and delivery_result.get("company_id") else "",
            },
            "service": {
                "mode": mode_val,
                "service": "LTL",
                "temperature": "FROZEN",  # TODO: calculate based on logic to be provided
            },
        }
    return payload


def build_shipment_payloads(
    df: pd.DataFrame,
    location_results: List[Dict[str, Any]],
    _cell_str: Any,
    holidays: Optional[Set[date]],
    mode_matrix: Optional[pd.DataFrame],
    mode_row_map: Optional[Dict[str, str]],
    mode_col_map: Optional[Dict[str, str]],
    pepsi_mode_matrix: Optional[pd.DataFrame] = None,
    pepsi_mode_row_map: Optional[Dict[str, str]] = None,
    pepsi_mode_col_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Group location results by row_index, match with CSV rows, build one payload per row.
    If template_flag = Pepsi: Pepsi payload + Pepsi mode sheet. Else: non-Pepsi payload + origins_destinations mode.
    """
    by_row: Dict[int, Dict[str, Optional[Dict[str, Any]]]] = {}
    for res in location_results:
        row_idx = res.get("row_index")
        if row_idx is None:
            continue
        if row_idx not in by_row:
            by_row[row_idx] = {"Pickup": None, "Delivery": None}
        loc_type = res.get("location_type")
        if loc_type in ("Pickup", "Delivery") and not res.get("error"):
            if by_row[row_idx][loc_type] is None:
                by_row[row_idx][loc_type] = res

    payloads = []
    for idx, row in df.iterrows():
        row_index = int(idx) + 1
        pickup_res = by_row.get(row_index, {}).get("Pickup")
        delivery_res = by_row.get(row_index, {}).get("Delivery")
        is_pepsi = _norm(_cell_str(row.get("template_flag"))) == "pepsi"

        errors = []
        if not pickup_res:
            errors.append("Missing Pickup location result")
        if not delivery_res:
            errors.append("Missing Delivery location result")

        payload = build_shipment_payload(
            row,
            pickup_res,
            delivery_res,
            _cell_str,
            holidays,
            mode_matrix,
            mode_row_map,
            mode_col_map,
            is_pepsi=is_pepsi,
            pepsi_mode_matrix=pepsi_mode_matrix,
            pepsi_mode_row_map=pepsi_mode_row_map,
            pepsi_mode_col_map=pepsi_mode_col_map,
        )
        payloads.append({
            "row_index": row_index,
            "payload": payload,
            "payload_type": "pepsi" if is_pepsi else "non_pepsi",
            "errors": errors if errors else None,
        })

    return payloads


def location_search(
    csv_path: str,
    config_path: Optional[str] = None,
    location_type: str = LOCATION_TYPE_BOTH,
    include_commodities: bool = True,
    transit_time_xlsx_path: Optional[str] = None,
    origins_destinations_xlsx_path: Optional[str] = None,
    pepsi_sheet_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read CSV, call location/search API per row. By default runs both Pickup and Delivery.
    - location_type "both" (default): CSV must have vendorno, shipfrom_street, shipto_street, template_flag, description, monday_group_name, consignee.
      For each row: call Pickup if alias_source/vendorno/shipfrom_street present, call Delivery if shipto_street present.
    - location_type "Pickup" or "Delivery": run only that type (requires only that type's columns).
    Returns id, company_id, province per location; each result has location_type "Pickup" or "Delivery".
    """
    run_both = location_type in (LOCATION_TYPE_BOTH, None, "")
    if not run_both and location_type not in (LOCATION_TYPE_PICKUP, LOCATION_TYPE_DELIVERY):
        return {
            "error": f"location_type must be '{LOCATION_TYPE_BOTH}', '{LOCATION_TYPE_PICKUP}', or '{LOCATION_TYPE_DELIVERY}'",
            "capability": CAPABILITY_NAME,
        }
    try:
        config = load_config(config_path)
    except FileNotFoundError as e:
        return {"error": str(e), "capability": CAPABILITY_NAME}

    csv_file = Path(csv_path)
    if not csv_file.exists():
        return {"error": f"CSV file not found: {csv_path}", "capability": CAPABILITY_NAME}

    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin-1")
    df = None
    last_error = None
    for enc in encodings:
        try:
            df = pd.read_csv(csv_file, encoding=enc, dtype=str)
            break
        except UnicodeDecodeError as e:
            last_error = e
            continue
    if df is None:
        return {
            "error": f"Failed to read CSV (tried {', '.join(encodings)}): {last_error}",
            "capability": CAPABILITY_NAME,
        }

    cols = config.get("csv_columns", {})
    req_pickup = {**REQUIRED_CSV_COLUMNS_PICKUP, **cols}
    req_delivery = {**REQUIRED_CSV_COLUMNS_DELIVERY, **cols}
    alias_value_col = req_pickup.get("alias_value", REQUIRED_CSV_COLUMNS_PICKUP["alias_value"])
    ship_from_col = req_pickup.get("street_address", REQUIRED_CSV_COLUMNS_PICKUP["street_address"])
    ship_to_col = cols.get("ship_to_street_address", REQUIRED_CSV_COLUMNS_DELIVERY["street_address"])

    # alias_source is derived from template_flag, vendorno, description, monday_group_name, consignee
    if run_both:
        required_cols = [alias_value_col, ship_from_col, ship_to_col] + list(ALIAS_SOURCE_COLUMNS)
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return {
                "error": f"CSV missing required column(s): {missing}. Required: vendorno, shipfrom_street, shipto_street, template_flag, description, monday_group_name, consignee. Available: {list(df.columns)}",
                "capability": CAPABILITY_NAME,
            }
    else:
        if location_type == LOCATION_TYPE_PICKUP:
            required_cols = [alias_value_col, ship_from_col] + list(ALIAS_SOURCE_COLUMNS)
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return {
                    "error": f"CSV missing required column(s) for Pickup: {missing}. Required: vendorno, shipfrom_street, template_flag, description, monday_group_name, consignee. Available: {list(df.columns)}",
                    "capability": CAPABILITY_NAME,
                }
        else:
            if ship_to_col not in df.columns:
                return {
                    "error": f"CSV missing required column for Delivery: '{ship_to_col}'. Required: ship_to_street_address. Available: {list(df.columns)}",
                    "capability": CAPABILITY_NAME,
                }

    try:
        access_token = get_access_token(config)
    except Exception as e:
        return {"error": f"Authentication failed: {e}", "capability": CAPABILITY_NAME}

    search_url = (
        config.get("location_api", {}).get("search_url")
        or "https://siwy6vb99l.execute-api.ca-central-1.amazonaws.com/qa/location-module/location/search"
    )

    def _cell_str(val: Any) -> str:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return str(val).strip()

    if run_both:
        def _row_has_any_key(row: Any) -> bool:
            return bool(
                _cell_str(row.get(alias_value_col))
                or _cell_str(row.get(ship_from_col))
                or _cell_str(row.get(ship_to_col))
            )
    elif location_type == LOCATION_TYPE_PICKUP:
        def _row_has_any_key(row: Any) -> bool:
            return bool(
                _cell_str(row.get(alias_value_col))
                or _cell_str(row.get(ship_from_col))
            )
    else:
        def _row_has_any_key(row: Any) -> bool:
            return bool(_cell_str(row.get(ship_to_col)))

    df = df[df.apply(_row_has_any_key, axis=1)].reset_index(drop=True)

    def _empty_result(row_index: int, error: str, loc_type: str) -> Dict[str, Any]:
        return {
            "row_index": row_index,
            "id": None,
            "company_id": None,
            "province": None,
            "location_type": loc_type,
            "error": error,
        }

    def _success_result(row_index: int, loc: Dict[str, Any], loc_type: str) -> Dict[str, Any]:
        return {
            "row_index": row_index,
            "id": loc["id"],
            "company_id": loc["company_id"],
            "province": loc["province"],
            "location_type": loc_type,
            "error": None,
        }

    results: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        row_index = int(idx) + 1
        alias_source_val = get_alias_source_from_row(row, _cell_str)
        alias_value_val = _cell_str(row.get(alias_value_col))
        ship_from_val = _cell_str(row.get(ship_from_col))
        ship_to_val = _cell_str(row.get(ship_to_col)) if run_both or location_type == LOCATION_TYPE_DELIVERY else ""

        types_to_run: List[str] = []
        if run_both:
            if alias_source_val and alias_value_val and ship_from_val:
                types_to_run.append(LOCATION_TYPE_PICKUP)
            if ship_to_val:
                types_to_run.append(LOCATION_TYPE_DELIVERY)
        else:
            if location_type == LOCATION_TYPE_PICKUP and alias_source_val and alias_value_val and ship_from_val:
                types_to_run.append(LOCATION_TYPE_PICKUP)
            elif location_type == LOCATION_TYPE_DELIVERY and ship_to_val:
                types_to_run.append(LOCATION_TYPE_DELIVERY)

        if not types_to_run:
            if run_both:
                results.append(_empty_result(row_index, "Missing required fields for both Pickup and Delivery", LOCATION_TYPE_BOTH))
            elif location_type == LOCATION_TYPE_PICKUP:
                results.append(_empty_result(row_index, "Missing required field (alias_source derived from template_flag/vendorno/description/monday_group_name/consignee, vendorno, or shipfrom_street)", LOCATION_TYPE_PICKUP))
            else:
                results.append(_empty_result(row_index, "Missing required field (ship_to_street_address)", LOCATION_TYPE_DELIVERY))
            continue

        for run_type in types_to_run:
            street_val = ship_from_val if run_type == LOCATION_TYPE_PICKUP else ship_to_val
            try:
                response = search_location(
                    access_token=access_token,
                    search_url=search_url,
                    street_address=street_val,
                    search_type=run_type,
                    alias_source=alias_source_val if run_type == LOCATION_TYPE_PICKUP else None,
                    alias_value=alias_value_val if run_type == LOCATION_TYPE_PICKUP else None,
                    include_commodities=include_commodities,
                )
                location_results = extract_location_results(response)
                if not location_results:
                    results.append(_empty_result(row_index, "No locations in response", run_type))
                else:
                    for loc in location_results:
                        results.append(_success_result(row_index, loc, run_type))
            except requests.RequestException as e:
                results.append(_empty_result(row_index, str(e), run_type))
            except Exception as e:
                results.append(_empty_result(row_index, str(e), run_type))

    result_data: Dict[str, Any] = {
        "locations": results,
        "total_rows": len(results),
        "location_type": LOCATION_TYPE_BOTH if run_both else location_type,
    }

    # Build shipment payloads when running both Pickup and Delivery
    if run_both:
        holidays: Optional[Set[date]] = None
        mode_matrix: Optional[pd.DataFrame] = None
        mode_row_map: Optional[Dict[str, str]] = None
        mode_col_map: Optional[Dict[str, str]] = None

        payload_errors: List[str] = []
        if transit_time_xlsx_path:
            try:
                holidays = load_holidays(transit_time_xlsx_path)
            except Exception as e:
                payload_errors.append(f"Failed to load holidays: {e}")
        else:
            payload_errors.append("Missing transit_time_xlsx_path (needed to adjust pickup_date for holidays/weekends)")

        if origins_destinations_xlsx_path:
            try:
                mode_matrix, mode_row_map, mode_col_map = load_mode_matrix(origins_destinations_xlsx_path)
            except Exception as e:
                payload_errors.append(f"Failed to load mode matrix: {e}")
        else:
            payload_errors.append("Missing origins_destinations_xlsx_path (needed to compute service.mode)")

        pepsi_mode_matrix: Optional[pd.DataFrame] = None
        pepsi_mode_row_map: Optional[Dict[str, str]] = None
        pepsi_mode_col_map: Optional[Dict[str, str]] = None
        if pepsi_sheet_path:
            try:
                pepsi_mode_matrix, pepsi_mode_row_map, pepsi_mode_col_map = load_mode_matrix(pepsi_sheet_path)
            except Exception as e:
                payload_errors.append(f"Failed to load Pepsi mode matrix: {e}")

        if payload_errors:
            result_data["payload_setup_errors"] = payload_errors

        payloads = build_shipment_payloads(
            df, results, _cell_str, holidays,
            mode_matrix, mode_row_map, mode_col_map,
            pepsi_mode_matrix, pepsi_mode_row_map, pepsi_mode_col_map,
        )
        result_data["payloads"] = payloads

    return {
        "result": result_data,
        "capability": CAPABILITY_NAME,
    }


def main() -> None:
    """Read JSON from stdin, dispatch by capability, write JSON to stdout."""
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(json.dumps({
            "error": f"Invalid JSON input: {e}",
            "capability": "unknown",
        }, indent=2))
        sys.exit(1)

    capability = input_data.get("capability")
    args = input_data.get("args", {})

    if capability == CAPABILITY_NAME:
        result = location_search(
            csv_path=args.get("csv_path", ""),
            config_path=args.get("config_path"),
            location_type=args.get("location_type") or args.get("type", LOCATION_TYPE_BOTH),
            include_commodities=args.get("include_commodities", DEFAULT_INCLUDE_COMMODITIES),
            transit_time_xlsx_path=args.get("transit_time_xlsx_path"),
            origins_destinations_xlsx_path=args.get("origins_destinations_xlsx_path"),
            pepsi_sheet_path=args.get("pepsi_sheet_path"),
        )
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps({
            "error": f"Unknown capability: {capability}",
            "capability": capability or "unknown",
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
