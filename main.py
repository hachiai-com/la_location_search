#!/usr/bin/env python3
"""
LA Location Search Toolkit.
Gets a fresh Cognito token on each run, reads input CSV, calls location/search API per row,
and returns id and company_id from each response.
"""
import csv
import json
import sys
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import date, timedelta

import requests
import pandas as pd
from requests_aws4auth import AWS4Auth

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


def create_shipment_via_api(payload: Dict[str, Any], shipment_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST payload to shipment create API (AWS4Auth + x-api-key). Same logic as ShipmentUtility.create_shipment.
    Returns {status_code, body, success, message} for the toolkit result.
    """
    url = (shipment_config.get("baseUrl") or "").strip()
    region = shipment_config.get("region") or ""
    service = shipment_config.get("service") or "execute-api"
    api_key = shipment_config.get("apiKey") or ""
    access_key = shipment_config.get("accessKey") or ""
    secret_key = shipment_config.get("secretKey") or ""

    if not all([url, region, service, api_key, access_key, secret_key]):
        return {
            "status_code": 0,
            "body": "",
            "success": False,
            "message": "Shipment API config incomplete (baseUrl, region, service, apiKey, accessKey, secretKey)",
        }

    try:
        auth = AWS4Auth(access_key, secret_key, region, service)
        headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
        }
        body_str = json.dumps(payload)
        resp = requests.post(url, data=body_str, headers=headers, auth=auth, timeout=60)
        body_text = resp.text

        # Parse message for result (same idea as ShipmentUtility.parse_api_call_result)
        message = "Error"
        try:
            if body_text.strip().startswith("{"):
                root = json.loads(body_text)
                if root.get("id") == "SHIP.PO.DUPLICATE" or "ALREADY EXISTS" in (root.get("message") or "").upper():
                    message = "Duplicate PO - Shipment already exists"
                elif "shipment_id" in root or "shipments" in root:
                    message = "Shipment created successfully by BOT"
            elif body_text.strip().startswith("["):
                arr = json.loads(body_text)
                if isinstance(arr, list) and len(arr) > 0:
                    message = "Shipment created successfully by BOT"
        except (json.JSONDecodeError, TypeError):
            pass

        return {
            "status_code": resp.status_code,
            "body": body_text,
            "success": 200 <= resp.status_code < 300,
            "message": message,
        }
    except Exception as e:
        logger.exception("Shipment API request failed")
        return {
            "status_code": 0,
            "body": str(e),
            "success": False,
            "message": str(e),
        }


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
    """From location/search response, return list of {id, company_id, province, commodities} for each location."""
    locations = api_response.get("locations") or []
    out = []
    for loc in locations:
        location_obj = loc.get("location") or {}
        province = location_obj.get("province")
        commodities_raw = loc.get("commodities") or []
        temperature_requirements = [
            str(c.get("temperature_requirement", "")).strip()
            for c in commodities_raw
            if c.get("temperature_requirement")
        ]
        out.append({
            "id": loc.get("id"),
            "company_id": loc.get("company_id"),
            "province": province,
            "commodities": temperature_requirements,
        })
    return out


def _norm(s: Any) -> str:
    return str(s or "").strip().casefold()


# Hardcoded mode lookup: Origin province × Destination province → ROAD or RAIL (same table for Pepsi and non-Pepsi).
# Eastern: NL, PEI, NS, NB, Quebec, Ontario. Western: Manitoba, Saskatchewan, Alberta, British Columbia.
# Same block → ROAD; cross block → RAIL.
_PROVINCES_EASTERN = (
    "Newfoundland and Labrador",
    "Prince Edward Island",
    "Nova Scotia",
    "New Brunswick",
    "Quebec",
    "Ontario",
)
_PROVINCES_WESTERN = (
    "Manitoba",
    "Saskatchewan",
    "Alberta",
    "British Columbia",
)
_MODE_LOOKUP: Dict[Tuple[str, str], str] = {}
for _o in _PROVINCES_EASTERN + _PROVINCES_WESTERN:
    for _d in _PROVINCES_EASTERN + _PROVINCES_WESTERN:
        _same_block = (_o in _PROVINCES_EASTERN and _d in _PROVINCES_EASTERN) or (
            _o in _PROVINCES_WESTERN and _d in _PROVINCES_WESTERN
        )
        _MODE_LOOKUP[(_norm(_o), _norm(_d))] = "ROAD" if _same_block else "RAIL"


def get_mode_hardcoded(origin_province: Optional[str], destination_province: Optional[str]) -> str:
    """Return ROAD or RAIL from hardcoded Origin×Destination table; empty string if unknown."""
    if not origin_province or not destination_province:
        return ""
    return _MODE_LOOKUP.get((_norm(origin_province), _norm(destination_province)), "")


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


# Pepsi: CN Customer Reference # column for customer_shipment and pickup_appointment (user may override later)
PEPSI_CN_REFERENCE_COLUMN = "pickApptNo"

# Non-Pepsi: shipment_type column code (short form) → payload temperature (Temp Range)
SHIPMENT_TYPE_TO_TEMPERATURE: Dict[str, str] = {
    "FROZ": "FROZEN",
    "GROC": "DRY",
    "DAIR": "FRESH",
    "MEAT": "FRESH",
    "YGRT": "FRESH",
    "REPK": "DRY",
    "GRPK": "DRY",
    "FSMT": "FRESH",
}

# monday_group_name values where shipment_type is NULL and temperature comes from pickup commodities
NPOP_MONDAY_GROUPS_TEMP_FROM_PICKUP = (
    "NPOP (LA3)/{SOBEYSMIF}.pdf",
    "NPOP (LA6)/{MIFLAOPS}.pdf",
)


def _shipment_type_code_to_temperature(code: str) -> str:
    """Map shipment_type column code (e.g. FROZ, (FROZ)) to Temp Range (FROZEN, DRY, FRESH)."""
    if not code:
        return ""
    raw = str(code).strip().upper().replace("(", "").replace(")", "")
    return SHIPMENT_TYPE_TO_TEMPERATURE.get(raw, "")


def _resolve_non_pepsi_temperature(
    shipment_type_val: str,
    monday_group_val: str,
    pickup_result: Optional[Dict[str, Any]],
) -> str:
    """
    Non-Pepsi temperature: from shipment_type column (code → Temp Range), or for NPOP LA3/LA6
    when shipment_type is NULL, from pickup location's commodities (one requirement → use it; multiple → empty).
    """
    monday_normalized = (monday_group_val or "").strip().lower()
    shipment_empty = not (shipment_type_val or "").strip()
    npop_match = any(monday_normalized == mg.strip().lower() for mg in NPOP_MONDAY_GROUPS_TEMP_FROM_PICKUP)

    if shipment_empty and npop_match and pickup_result:
        commodities = pickup_result.get("commodities") or []
        unique_temps = list(dict.fromkeys([t for t in commodities if t]))
        if len(unique_temps) == 1:
            return unique_temps[0]
        return ""

    if shipment_type_val:
        return _shipment_type_code_to_temperature(shipment_type_val)
    return ""


def build_shipment_payload(
    row: Any,
    pickup_result: Optional[Dict[str, Any]],
    delivery_result: Optional[Dict[str, Any]],
    _cell_str: Any,
    holidays: Optional[Set[date]],
    is_pepsi: bool = False,
) -> Dict[str, Any]:
    """
    Build shipment payload. If is_pepsi (template_flag=Pepsi): Pepsi payload shape.
    Else: non-Pepsi payload shape. service.mode uses hardcoded Origin×Destination table for both.
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

    # Mode: hardcoded Origin×Destination table (same for Pepsi and non-Pepsi)
    mode_val = get_mode_hardcoded(
        pickup_result.get("province") if pickup_result else None,
        delivery_result.get("province") if delivery_result else None,
    )

    quantities_block = {
        "weight": _get_float("weight", 0.0),
        "weight_unit": "lbs",
        "cube": _get_float("cubes", 0.0),
        "cube_unit": "ft3",
        "cases": _get_int("cases", 0),
        "lifts": _get_int("lifts", 0),
        "pallets": _get_int("pallets", 0),
    }

    # Temperature: Pepsi = CSV "temp" column; Non-Pepsi = shipment_type code map or pickup commodities (NPOP exception)
    if is_pepsi:
        temperature_val = _get_cell("temp", "")
    else:
        temperature_val = _resolve_non_pepsi_temperature(
            _get_cell("shipment_type", ""),
            _get_cell("monday_group_name", ""),
            pickup_result,
        )

    if is_pepsi:
        # Pepsi payload structure (customer_shipment and pickup_appointment from pickApptNo)
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
                "temperature": temperature_val,
            },
        }
    else:
        # Non-Pepsi (current) payload structure
        service_block: Dict[str, Any] = {
            "mode": mode_val,
            "service": "LTL",
        }
        if temperature_val:
            service_block["temperature"] = temperature_val
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
            "service": service_block,
        }
    return payload


def build_shipment_payloads(
    df: pd.DataFrame,
    location_results: List[Dict[str, Any]],
    _cell_str: Any,
    holidays: Optional[Set[date]],
) -> List[Dict[str, Any]]:
    """
    Group location results by row_index, match with CSV rows, build one payload per row.
    Mode is from hardcoded Origin×Destination table (same for Pepsi and non-Pepsi).
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
            is_pepsi=is_pepsi,
        )
        payloads.append({
            "row_index": row_index,
            "payload": payload,
            "payload_type": "pepsi" if is_pepsi else "non_pepsi",
            "errors": errors if errors else None,
        })

    return payloads


# Output CSV column names (order matches output_sheet.csv)
OUTPUT_CSV_COLUMNS = [
    "Item ID",
    "JSON Request",
    "JSON Response",
    "Description",
    "Purchase Order",
    "Vendor #",
    "Pick Up Date",
    "Delivery Date",
    "Weight",
    "Cube",
    "Lifts",
    "Pallets",
    "Origin / Load At",
    "Destination / Delivery Location",
    "Customer / PickUp Company",
    "Client / Consignee",
    "Mode",
    "Service",
    "Temperature",
    "API Call Result",
    "Status",
    "Load Number (Pepsi)",
    "Order # (Pepsi)",
]

# Status values for output CSV (normal, error, Pepsi-specific)
STATUS_IN_QUEUE = "In Queue"
STATUS_CREATED = "Created"
STATUS_UPDATED_SHIPMENT = "Updated Shipment"
STATUS_RETRY = "Retry"
STATUS_PO_ALREADY_EXISTS = "PO already exists in Altruos"
STATUS_VENDOR_MISSING = "Vendor # Missing (ERROR)"
STATUS_DEST_MISSING = "Dest. Missing (ERROR)"
STATUS_PICKUP_DATE_MISSING = "PICKUP Date Missing (ERROR)"
STATUS_DELIVERY_DATE_MISSING = "DELIVERY Date Missing (ERROR)"
STATUS_CUBE_WEIGHT_CASES_MISSING = "Cube, Weight or Cases Missing (ERROR)"
STATUS_MISSING_TEMP = "Missing Temp. (ERROR)"
STATUS_MISSING_MODE = "Missing Mode (ERROR)"
STATUS_JSON_NOT_SENT = "JSON not sent (ERROR)"
STATUS_RSC_NO_MATCH = "RSC # does NOT match (ERROR)"
STATUS_DEST_NAME_NO_MATCH = "Destination name does NOT match (ERROR)"
STATUS_PO_NOT_FOUND = "PO not FOUND (ERROR)"
STATUS_UNREADABLE_FILE = "Unreadable File"
STATUS_ALIAS_LOOKUP_FAILURE = "Alias Lookup failure (ERROR)"
STATUS_CUSTOMER_MISSING = "Customer Missing"
STATUS_ERROR_GENERIC = "ERROR (generic catch-all)"
STATUS_PALLETS_MISSING_PEPSI = "Pallets Missing (Pepsi)"
STATUS_LOAD_NUMBER_MISSING_PEPSI = "Load Number Missing (Pepsi)"
STATUS_CLIENT_MISSING_PEPSI = "Client Missing (Pepsi)"
STATUS_ORDER_MISSING_PEPSI = "Order # Missing (Pepsi)"


def _payload_get(payload: Dict[str, Any], *keys: str, default: str = "") -> str:
    """Get nested value from payload, e.g. _payload_get(p, 'dates', 'pickup_date')."""
    d = payload
    for k in keys:
        d = (d or {}).get(k)
        if d is None:
            return default
    return str(d) if d is not None else default


def _compute_output_status(
    item: Dict[str, Any],
    payload: Dict[str, Any],
    cell_fn: Any,
    create_response: Dict[str, Any],
) -> str:
    """
    Compute Status for output CSV: validation errors first, then Pepsi-specific, then API result.
    """
    errors = item.get("errors") or []
    is_pepsi = item.get("payload_type") == "pepsi"

    # Payload build errors (missing Pickup/Delivery location result)
    if errors:
        if any("Delivery" in e for e in errors):
            return STATUS_DEST_MISSING
        return STATUS_ALIAS_LOOKUP_FAILURE

    # Required fields (input or payload)
    if not (cell_fn("vendorno") or "").strip():
        return STATUS_VENDOR_MISSING
    dest = _payload_get(payload, "locations", "destination")
    if not (dest or "").strip():
        return STATUS_DEST_MISSING
    pickup_date = _payload_get(payload, "dates", "pickup_date")
    if not (pickup_date or "").strip():
        return STATUS_PICKUP_DATE_MISSING
    delivery_date = _payload_get(payload, "dates", "delivery_date")
    if not (delivery_date or "").strip():
        return STATUS_DELIVERY_DATE_MISSING

    weight = (cell_fn("weight") or "").strip()
    cube = (cell_fn("cubes") or "").strip()
    cases = (cell_fn("cases") or "").strip()
    if not weight and not cube and not cases:
        return STATUS_CUBE_WEIGHT_CASES_MISSING
    if not weight or not cube or not cases:
        return STATUS_CUBE_WEIGHT_CASES_MISSING

    service = payload.get("service") or {}
    temperature = service.get("temperature", "")
    # Pepsi always has temp from CSV; non-Pepsi we sometimes omit key when multiple commodities
    if is_pepsi and not (temperature or "").strip():
        return STATUS_MISSING_TEMP
    if not is_pepsi and "temperature" in service and not (temperature or "").strip():
        return STATUS_MISSING_TEMP
    mode = _payload_get(payload, "service", "mode")
    if not (mode or "").strip():
        return STATUS_MISSING_MODE

    customer = _payload_get(payload, "parties", "customer")
    if not (customer or "").strip():
        return STATUS_CUSTOMER_MISSING

    # Pepsi-specific
    if is_pepsi:
        pallets = (cell_fn("pallets") or "").strip()
        if not pallets:
            return STATUS_PALLETS_MISSING_PEPSI
        load_number = (cell_fn("invoiceRef") or "").strip()
        if not load_number:
            return STATUS_LOAD_NUMBER_MISSING_PEPSI
        client = _payload_get(payload, "parties", "client")
        if not (client or "").strip():
            return STATUS_CLIENT_MISSING_PEPSI
        order_po = (cell_fn("po") or "").strip()
        if not order_po:
            return STATUS_ORDER_MISSING_PEPSI

    # API call result
    if not create_response:
        return STATUS_JSON_NOT_SENT
    msg = (create_response.get("message") or "").upper()
    if create_response.get("success"):
        if "DUPLICATE" in msg or "ALREADY EXISTS" in msg:
            return STATUS_PO_ALREADY_EXISTS
        return STATUS_CREATED
    if "DUPLICATE" in msg or "ALREADY EXISTS" in msg:
        return STATUS_PO_ALREADY_EXISTS
    if "RETRY" in msg:
        return STATUS_RETRY
    return STATUS_ERROR_GENERIC


def write_output_csv(
    output_path: str,
    df: pd.DataFrame,
    payloads: List[Dict[str, Any]],
    _cell_str: Any,
) -> None:
    """
    Write one row per payload to output CSV. Column mapping from payload + input row + create_response.
    """
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for item in payloads:
            row_index = item.get("row_index", 1)
            payload = item.get("payload") or {}
            create_response = item.get("create_response") or {}
            # Input row: 1-based row_index -> 0-based iloc
            try:
                input_row = df.iloc[row_index - 1]
            except (IndexError, KeyError):
                input_row = {}
            def cell(col: str) -> str:
                v = input_row.get(col) if hasattr(input_row, "get") else ""
                return _cell_str(v) or ""

            json_response = create_response.get("body", "")
            api_result = create_response.get("message", "")
            if item.get("errors"):
                if not api_result:
                    api_result = "Skipped (payload has errors)"
            if not api_result and not item.get("errors"):
                api_result = "Not sent (shipment_api not configured)"

            service = payload.get("service") or {}
            temperature = service.get("temperature", "")

            status = _compute_output_status(item, payload, cell, create_response)
            is_pepsi = item.get("payload_type") == "pepsi"
            load_number_pepsi = cell("invoiceRef") if is_pepsi else ""
            order_pepsi = cell("po") if is_pepsi else ""

            writer.writerow({
                "Item ID": cell("item_id"),
                "JSON Request": json.dumps(payload, ensure_ascii=False),
                "JSON Response": json_response,
                "Description": cell("description"),
                "Purchase Order": cell("po"),
                "Vendor #": cell("vendorno"),
                "Pick Up Date": _payload_get(payload, "dates", "pickup_date"),
                "Delivery Date": _payload_get(payload, "dates", "delivery_date"),
                "Weight": cell("weight"),
                "Cube": cell("cubes"),
                "Lifts": cell("lifts"),
                "Pallets": cell("pallets"),
                "Origin / Load At": _payload_get(payload, "locations", "origin"),
                "Destination / Delivery Location": _payload_get(payload, "locations", "destination"),
                "Customer / PickUp Company": _payload_get(payload, "parties", "customer"),
                "Client / Consignee": _payload_get(payload, "parties", "client"),
                "Mode": _payload_get(payload, "service", "mode"),
                "Service": _payload_get(payload, "service", "service"),
                "Temperature": temperature,
                "API Call Result": api_result,
                "Status": status,
                "Load Number (Pepsi)": load_number_pepsi,
                "Order # (Pepsi)": order_pepsi,
            })
    logger.info("Wrote output CSV: %s (%d rows)", output_path, len(payloads))


def location_search(
    csv_path: str,
    config_path: Optional[str] = None,
    location_type: str = LOCATION_TYPE_BOTH,
    include_commodities: bool = True,
    transit_time_xlsx_path: Optional[str] = None,
    output_csv_path: Optional[str] = None,
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

        payload_errors: List[str] = []
        if transit_time_xlsx_path:
            try:
                holidays = load_holidays(transit_time_xlsx_path)
            except Exception as e:
                payload_errors.append(f"Failed to load holidays: {e}")
        else:
            payload_errors.append("Missing transit_time_xlsx_path (needed to adjust pickup_date for holidays/weekends)")

        if payload_errors:
            result_data["payload_setup_errors"] = payload_errors

        payloads = build_shipment_payloads(df, results, _cell_str, holidays)

        # Optional: call shipment create API for each payload if config has shipment_api
        shipment_config = config.get("shipment_api")
        if shipment_config and isinstance(shipment_config, dict):
            for item in payloads:
                if item.get("errors"):
                    item["create_response"] = {
                        "status_code": 0,
                        "body": "",
                        "success": False,
                        "message": "Skipped (payload has errors)",
                    }
                else:
                    item["create_response"] = create_shipment_via_api(
                        item.get("payload") or {}, shipment_config
                    )

        result_data["payloads"] = payloads

        if output_csv_path:
            try:
                write_output_csv(output_csv_path, df, payloads, _cell_str)
                result_data["output_csv_path"] = output_csv_path
            except Exception as e:
                logger.exception("Failed to write output CSV")
                result_data["output_csv_error"] = str(e)

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
            output_csv_path=args.get("output_csv_path"),
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
