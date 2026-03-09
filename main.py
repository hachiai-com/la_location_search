#!/usr/bin/env python3
"""
LA Location Search Toolkit.
Gets a fresh Cognito token on each run, reads input CSV, calls location/search API per row,
and returns id and company_id from each response.
"""
import csv
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import date, datetime, timedelta

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

# Delivery API: alias-based only (no street_address). alias_value = "shipto" column from CSV (all cases including RSC40/RSC50).
DELIVERY_SHIPTO_COLUMN = "shipto"  # CSV column for Delivery alias_value (full destination text)
SOBEYS_DELIVERY_ALIAS_SOURCE = "Sobeys Destination"
PEPSI_DELIVERY_ALIAS_SOURCE = "Pepsi Destination"

# Canonical shipto_street lookup for Delivery API: resolve input address to a known-good address from this list.
# Multi-digit leading number: search by number (e.g. "260199"). Single-digit: search by number + next word (e.g. "1 LEWIS").
SHIPTO_STREET_LOOKUP_NON_PEPSI = [
    "178 STOCKDALE RD",
    "100 GIBRALTAR RD",
    "2400 TRANSCANADIENNE AUT",
    "260140 HIGH PLAINS BLVD.",
    "3440 56 AVE SE",
    "100 NORDEAGLE AVE",
    "105 FOORD ST",
    "12827 MAIN ST",
    "950 GALILEE AV",
    "66 HARDY AVE",
    "63 GLENCOE DR",
    "11625 55E AV",
    "1 LEWIS ST",
    "1893 MILLS RD",
    "246 LANCASTER DR",
    "181 LEIL LANE",
    "1265 EMPRESS ST",
    "260199 HIGH PLAINS BLVD",
    "9001 DU PARCOURS RUE",
    "18890 22 AVE",
    "301 EXPORT BLVD",
    "1101 DE LA PINIERE BOUL",
    "12425 66 ST NW E",
    "3000 TEBBUTT RUE",
    "1800 INKSTER BLVD",
    "1 HOME ST",
    "1500 DE MONTARVILLE BOUL",
    "12910 156 ST NW",
    "8265 HUNTINGTON RD",
    "81 THORNHILL DR",
]
SHIPTO_STREET_LOOKUP_PEPSI = [
    "5445 8TH AVE NE",
    "7350 WILSON AVE",
    "14434 157 AVE NW",
    "2226 SOUTH SERVICE RD W",
    "27383 92 AVE",
    "6635 106 AVE SE",
    "1440 39TH AVE",
    "4189 SALISH SEA WAY",
    "450 DERWENT PL",
    "7100 44 ST SE",
    "19580 TELEGRAPH TRAIL",
    "100 GIBRALTAR RD",
    "2400 TRANSCANADIENNE AUT",
    "260140 HIGH PLAINS BLVD.",
    "7800 RIVERFRONT GATE",
    "8225 30 ST SE",
    "1003 HAMILTON GREEN",
    "1003 HAMILTON BLVD NE",
    "41 MAPLEVIEW DR E",
    "4438 KING ST E",
    "20313 100A AVE",
    "140 GRANITE DR",
    "693 WONDERLAND RD N",
    "75 DANNY DR",
    "870 MONTEE DES PIONNIERS",
    "3200 CHEMIN DE LA BARONNIE",
    "100 LINE DR",
    "100 BAIG BLVD",
    "35 CLYDE AVE",
    "19 LAKESIDE DR",
    "1724 115 AVE NE",
    "83 COMMERCE ST",
    "1400 CHURCH ST",
    "1105 FOUNTAIN ST N",
    "55 FREEPORT BLVD NE",
    "18800 LOUGHEED HWY",
    "2101 FLEMING ROAD",
    "355 KENT AVE NORTH E",
    "101 WESTON ST",
    "16104 121A AVE NW",
    "2755 190TH ST",
    "18574 WOODBINE AVE",
    "775 FRENETTE AVE",
    "500 BAYLY ST E",
    "12203 AIRPORT RD",
    "180 CHEMIN DU TREMBLAY",
    "7530 HOPSCOTT RD",
    "607 46TH ST E",
    "1615 KING EDWARD ST",
    "2200 TRANSCANADIENNE AUT",
    "1633 MEYERSIDE DR",
    "241 SNIDERCROFT RD",
    "80 MORTON AVE E",
    "1700 CLIVEDEN AVE",
    "13511 163 ST NW",
    "290212 TOWNSHIP ROAD 261",
    "310 STERLING LYON PKY",
    "1330 OPTIMUM DR",
    "10 DEWARE DR",
    "6941 KENNEDY RD",
    "2525 29 ST NE",
    "10931 177 ST NW",
    "11555 MAURICE-DUPLESSIS BOUL",
    "635 NEWTON AV",
    "490 INDUSTRIAL AVE",
    "1055 DE LA PINIERE BOUL",
    "5559 DUNDAS ST W",
    "75 VICKERS RD",
    "170 THE WEST MALL",
    "26308 TOWNSHIP ROAD 525A",
    "5111 242ND ST",
    "21 YORK RD",
    "1890 READING COURT",
    "101 HUTCHINGS ST",
    "291196 WAGON WHEEL RD",
    "3440 56 AVE SE",
    "100 NORDEAGLE AVE",
    "105 FOORD ST",
    "12827 MAIN ST",
    "950 GALILEE AV",
    "66 HARDY AVENUE",
    "63 GLENCOE DR",
    "11625 55E AV",
    "1 LEWIS ST",
    "1893 MILLS RD",
    "246 LANCASTER DRIVE",
    "181 LEIL LANE",
    "1265 EMPRESS ST",
    "260199 HIGH PLAINS BLVD.",
    "9001 DU PARCOURS RUE",
    "18890 22 AVE",
    "301 EXPORT BLVD",
    "1101 DE LA PINIERE BOUL",
    "12425 66 ST NW",
    "3000 TEBBUTT RUE",
    "1800 INKSTER BLVD",
    "1 HOME ST",
    "1500 DE MONTARVILLE BOUL",
    "12910 156 ST NW",
    "8265 HUNTINGTON RD",
    "81 THORNHILL DR",
    "5111 272 ST",
    "26875 96 AVE",
    "178 STOCKDALE RD",
    "26210 TOWNSHIP ROAD 531A",
    "1 DUCK POND RD",
    "460 MACNAUGHTON AVE",
    "266 DEWDNEY AVE E",
    "260081 NOSE CRK BLVD",
    "1515 COMMERCE WAY",
    "1346 KINGSWAY AVE",
    "1570 CLARENCE AVE",
    "13232 170 ST NW",
    "3400 39 AVE NE",
    "19525 24TH AVE",
    "261039 WAGON WHEEL CRES",
    "261046 WAGON WHEEL WAY",
    "2401 SCM WAY",
    "1501 INDUSTRIAL PARK DR",
    "5445 8 ST NE",
    "14434 157TH AVE NW",
    "1988 VERNON DR",
]


def resolve_shipto_street_for_delivery(input_address: str, is_pepsi: bool) -> str:
    """
    Resolve shipto_street from input to a canonical address from the lookup table for the Delivery API.
    - Multi-digit leading number (e.g. 260199): search lookup by that number, return matching address.
    - Single-digit leading number (e.g. 1): search by number + next word (e.g. '1 LEWIS') to disambiguate
      (e.g. '1 LEWIS ST' vs '1 HOME ST'). Return matching canonical address.
    If no match, returns the original input_address unchanged.
    """
    s = (input_address or "").strip()
    if not s:
        return s
    up = s.upper()
    parts = up.split()
    if not parts or not parts[0].isdigit():
        return s
    first = parts[0]
    lookup = SHIPTO_STREET_LOOKUP_PEPSI if is_pepsi else SHIPTO_STREET_LOOKUP_NON_PEPSI
    if len(first) == 1:
        search_key = (first + " " + parts[1]) if len(parts) >= 2 else first
        for addr in lookup:
            if addr.startswith(search_key):
                return addr
    else:
        search_key = first
        for addr in lookup:
            if addr.startswith(search_key + " ") or addr == search_key:
                return addr
    return s


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load config from the given path. config_path is required (config contains secrets and should not be in repo)."""
    if not (config_path and str(config_path).strip()):
        raise FileNotFoundError(
            "config_path is required. Pass the path to your config file (do not commit config.json; it contains secrets)."
        )
    path = Path(config_path)
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
    search_type: str,
    include_commodities: bool = True,
    street_address: Optional[str] = None,
    alias_source: Optional[str] = None,
    alias_value: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call location/search API.
    Pickup: alias_source + alias_value + include_commodities (no street_address on first try).
      If the caller gets multiple locations, they may retry with street_address for fallback.
    Delivery (default): alias_source + alias_value + include_commodities (no street_address).
    Delivery (RSC40/RSC50): street_address + alias_source + alias_value + include_commodities.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    payload: Dict[str, Any] = {
        "include_commodities": include_commodities,
        "type": search_type,
    }
    if street_address is not None and str(street_address).strip():
        payload["street_address"] = str(street_address).strip()
    if alias_source is not None and alias_value is not None:
        payload["alias_source"] = alias_source
        payload["alias_value"] = str(alias_value)
    resp = requests.post(search_url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def extract_location_results(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    From location/search response, return list of {id, company_id, province, commodities, location_name, alias_source, street_address} for each location.
    Each location's "commodities" is a list of temperature_requirement values (e.g. ["DRY"]) taken from the API's
    commodities[].temperature_requirement; used for LA3/LA6 payload temperature.
    street_address is from location.street_address for Pickup multi-location matching by first number.
    """
    locations = api_response.get("locations") or []
    out = []
    for loc in locations:
        location_obj = loc.get("location") or {}
        province = location_obj.get("province")
        street_address = (location_obj.get("street_address") or "").strip() or ""
        commodities_raw = loc.get("commodities") or []  # API: list of {temperature_requirement: "DRY", ...}
        temperature_requirements = [
            str(c.get("temperature_requirement", "")).strip()
            for c in commodities_raw
            if c.get("temperature_requirement")
        ]  # e.g. ["DRY"] → passed to payload as service.temperature for LA3/LA6
        location_name = (loc.get("location_name") or "").strip() or ""
        alias_source = ""
        location_alias = loc.get("location_alias") or []
        if location_alias and isinstance(location_alias, list) and len(location_alias) > 0:
            first_alias = location_alias[0]
            if isinstance(first_alias, dict) and first_alias.get("alias_source"):
                alias_source = str(first_alias.get("alias_source", "")).strip()
        out.append({
            "id": loc.get("id"),
            "company_id": loc.get("company_id"),
            "province": province,
            "commodities": temperature_requirements,
            "location_name": location_name,
            "alias_source": alias_source,
            "street_address": street_address,
        })
    return out


def _norm(s: Any) -> str:
    return str(s or "").strip().casefold()


def _is_street_null_or_blank(val: Any) -> bool:
    """True if street value is NULL (literal) or empty/missing; used for Cannot read address (ERROR)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip()
    return not s or s.upper() == "NULL"


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


# Single exact string for SOBEYS TENDER - LA6 (alias_source; monday_group_name must match this exactly)
MONDAY_GROUP_LA6_EXACT = "NPOP (LA6)_{MIFLAOPS}.pdf"

# Column that may contain full pickup address (e.g. "RYDER LOGISTICS 2 1890 READING CT MILTON, ON L9T2X8") for Pepsi BEVERAGE vs QUAKER
SHIP_FROM_COLUMN_FOR_PEPSI = "ship_from"


def get_alias_source_from_row(row: Any, _cell_str: Any) -> str:
    """
    Derive alias_source from CSV row for Pickup only.

    PEPSI TENDER (template_flag = "Pepsi"):
      - vendorno starts with "C" or "c" → "PEPSI TENDER - FOOD"
      - vendorno starts with "2" and ship_from contains "L9T2X8" → "PEPSI TENDER - BEVERAGE"
      - vendorno starts with "2" and ship_from does not contain "L9T2X8" → "PEPSI TENDER - QUAKER"

    SOBEYS TENDER (template_flag = "template-1" or Null):
      1. If monday_group_name exactly equals "NPOP (LA6)_{MIFLAOPS}.pdf" (single string) → "SOBEYS TENDER - LA6"
      2. Else if description contains "M&M" (exact substring) → "SOBEYS TENDER - M&M"
      3. Else if description does not have "M&M" and monday_group_name ≠ that exact string and consignee contains exact code RSC8, RSC9, RSC12, or CFC3 (whole token, e.g. not RSC92) → "SOBEYS TENDER - OTR"
      4. Else if description has neither "M&M" nor "OTR" → "SOBEYS TENDER - ADMIN"
    """
    template = _cell_str(row.get("template_flag"))
    vendorno = _cell_str(row.get("vendorno"))
    description = _cell_str(row.get("description"))
    monday_group = _cell_str(row.get("monday_group_name"))
    consignee = _cell_str(row.get("consignee"))

    # PEPSI TENDER: template_flag = "Pepsi" → vendorno (and ship_from for 2xxx) determines alias_source
    if _norm(template) == "pepsi":
        vendorno_stripped = vendorno.strip()
        if vendorno_stripped.upper().startswith("C"):
            return "PEPSI TENDER - FOOD"
        if vendorno_stripped.startswith("2"):
            ship_from_val = _cell_str(row.get(SHIP_FROM_COLUMN_FOR_PEPSI))
            if "L9T2X8" in (ship_from_val or "").upper():
                return "PEPSI TENDER - BEVERAGE"
            return "PEPSI TENDER - QUAKER"
        return ""

    # SOBEYS TENDER: template_flag = "template-1" or Null
    if _norm(template) in ("", "template-1"):
        monday_stripped = monday_group.strip()
        # 1. LA6: monday_group_name exactly this single string (not two separate checks)
        if monday_stripped == MONDAY_GROUP_LA6_EXACT:
            return "SOBEYS TENDER - LA6"
        # 2. M&M: description contains "M&M" (exact substring, e.g. "M&M 514 Stuffed Potato Skins 1")
        if "M&M" in description:
            return "SOBEYS TENDER - M&M"
        # 3. OTR: no M&M, monday_group_name not exactly LA6 string, consignee has exact code RSC8/RSC9/RSC12/CFC3 (whole token, not e.g. RSC92)
        consignee_upper = consignee.upper()
        if monday_stripped != MONDAY_GROUP_LA6_EXACT and re.search(r"\b(RSC8|RSC9|RSC12|CFC3)\b", consignee_upper):
            return "SOBEYS TENDER - OTR"
        # 4. ADMIN: description has neither "M&M" nor "OTR"
        if "M&M" not in description and "OTR" not in description:
            return "SOBEYS TENDER - ADMIN"
        return ""

    return ""


DESTINATION_ID_COLUMN = "Destination ID (Altruos)"

# Hardcoded destination IDs (Altruos) for resolving multiple Delivery API results.
# Sobeys tender lookup: "Destination ID (Altruos)" from Sobeys table.
SOBEYS_DESTINATION_IDS: Set[str] = {
    "959", "960", "961", "962", "963", "964", "965", "966", "967", "968", "969",
    "970", "971", "972", "973", "974", "975", "976", "977", "978", "979", "980",
    "981", "982", "983", "984", "985", "986", "988", "989", "1119",
}
# Pepsi tender lookup: "Destination ID (Altruos)" from Pepsi table.
PEPSI_DESTINATION_IDS: Set[str] = {
    "769", "841", "842", "843", "846", "847", "848", "849", "850", "852", "861", "862",
    "864", "865", "866", "867", "869", "875", "876", "881", "884", "885", "892", "893",
    "894", "895", "896", "897", "898", "899", "900", "901", "902", "903", "904", "905",
    "906", "907", "908", "909", "910", "924", "925", "926", "928", "929", "930", "931",
    "936", "937", "938", "939", "940", "942", "943", "944", "950", "951", "952", "953",
    "959", "960", "961", "962", "963", "964", "965", "966", "967", "968", "969", "970",
    "971", "972", "973", "974", "975", "976", "977", "978", "979", "980", "981", "982",
    "983", "984", "985", "986", "988", "989", "990", "992", "993", "994", "995", "996",
    "997", "1000", "1009", "1010", "1011", "1012", "1014", "1015", "1016", "1018",
    "1020", "1025", "1026", "1027", "1046", "1109", "1115", "1118", "1119", "1122", "1127",
    "1128", "1129", "1130", "1136", "1386", "1625", "1748", "1784", "1924", "2320", "2333",
}


def load_destination_lookup(file_path: str) -> Set[str]:
    """Load set of destination IDs from CSV or Excel. Column 'Destination ID (Altruos)'."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Destination lookup file not found: {file_path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str, encoding="utf-8")
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl" if suffix == ".xlsx" else None)
    else:
        raise ValueError(f"Destination lookup must be CSV or Excel, got {suffix}")
    # Find column by name (case-insensitive, strip)
    col = None
    for c in df.columns:
        if str(c).strip().lower() == DESTINATION_ID_COLUMN.lower():
            col = c
            break
    if col is None:
        raise ValueError(f"Column '{DESTINATION_ID_COLUMN}' not found in {file_path}. Columns: {list(df.columns)}")
    ids = set()
    for v in df[col].dropna().astype(str).str.strip():
        if v:
            ids.add(v)
    return ids


def _resolve_delivery_location(
    location_results: List[Dict[str, Any]],
    destination_id_set: Optional[Set[str]],
) -> Dict[str, Any]:
    """When Delivery API returns multiple locations, pick the first whose id is in the lookup set; else first."""
    if not location_results:
        raise ValueError("location_results is empty")
    if not destination_id_set or len(location_results) == 1:
        return location_results[0]
    for loc in location_results:
        lid = loc.get("id")
        if lid is not None and str(lid).strip() in destination_id_set:
            return loc
    return location_results[0]


def _first_number(s: Any) -> Optional[str]:
    """Extract the first contiguous sequence of digits from a string. Strict: no substring guessing."""
    s = str(s or "").strip()
    m = re.search(r"\d+", s)
    return m.group(0) if m else None


def _resolve_pickup_location_by_street_number(
    location_results: List[Dict[str, Any]],
    shipfrom_street: str,
) -> Optional[Dict[str, Any]]:
    """
    When Pickup API returns multiple locations, pick the one whose street_address first number
    exactly matches the first number from shipfrom_street (CSV). Strict match only.
    Returns None if no match or shipfrom_street has no leading number.
    """
    target_num = _first_number(shipfrom_street)
    if not target_num:
        return None
    for loc in location_results:
        addr = loc.get("street_address") or ""
        addr_num = _first_number(addr)
        if addr_num is not None and addr_num == target_num:
            return loc
    return None


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
    """Parse common date formats (and Excel serial) into a date. Input CSV uses DD-MM-YYYY."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Explicit DD-MM-YYYY / DD/MM/YYYY (input CSV may use this)
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ("%d-%m-%y", "%d/%m/%y") and dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.date()
        except ValueError:
            continue

    # YYYY-MM-DD / YYYY/MM/DD (e.g. 2026/02/09 → 9 Feb 2026; avoid pandas interpreting as Sep 2)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

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

def _is_la3_or_la6_group(monday_group_val: str) -> bool:
    """True if monday_group_name indicates LA3 or LA6 (NPOP). Use for temperature from pickup API only."""
    s = (monday_group_val or "").strip().lower()
    if not s:
        return False
    return "npop" in s and ("la3" in s or "la6" in s)


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
    Non-Pepsi temperature:
    - Pepsi: not used here (caller uses temp column).
    - LA3 and LA6: strictly use pickup API result's "temperature_requirement" from commodities (e.g. "DRY"). If exactly one, return it and it is passed as payload service.temperature. If 0 or multiple, return "".
    - Other non-Pepsi: use shipment_type column and map to full form (FROZEN, DRY, FRESH).
    """
    if _is_la3_or_la6_group(monday_group_val):
        # LA3/LA6: pick temperature_requirement from pickup API (e.g. commodities[0].temperature_requirement "DRY") → payload service.temperature
        if not pickup_result:
            logger.info("LA3/LA6 temperature: monday_group=%s, pickup result missing → temperature empty", (monday_group_val or "").strip())
            return ""
        commodities = pickup_result.get("commodities") or []
        unique_temps = list(dict.fromkeys([str(t).strip() for t in commodities if t]))
        location_name = (pickup_result.get("location_name") or "").strip()
        logger.info(
            "LA3/LA6 temperature: monday_group=%s, pickup location=%s, temperature_requirement(s)=%s → using %s",
            (monday_group_val or "").strip(),
            location_name or "(unknown)",
            unique_temps,
            unique_temps[0] if len(unique_temps) == 1 else "(empty: 0 or multiple)",
        )
        if len(unique_temps) == 1:
            return unique_temps[0]
        return ""

    # Other non-Pepsi: from shipment_type column (code → full form)
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

    # Temperature: Pepsi = CSV "temp" column. Non-Pepsi: LA3/LA6 = pickup API temperature_requirement only; other = shipment_type map. Fallback: delivery single temp if still empty.
    if is_pepsi:
        temperature_val = _get_cell("temp", "")
    else:
        temperature_val = _resolve_non_pepsi_temperature(
            _get_cell("shipment_type", ""),
            _get_cell("monday_group_name", ""),
            pickup_result,
        )
        if not (temperature_val or "").strip() and delivery_result:
            commodities = delivery_result.get("commodities") or []
            unique_temps = list(dict.fromkeys([t for t in commodities if t]))
            if len(unique_temps) == 1:
                temperature_val = unique_temps[0]

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
                "destination": str(delivery_result["id"]) if delivery_result and delivery_result.get("id") else "",
            },
            "parties": {
                "customer": str(pickup_result["company_id"]) if pickup_result and pickup_result.get("company_id") else "",
                "client": str(delivery_result["company_id"]) if delivery_result and delivery_result.get("company_id") else "",
            },
            "service": {
                "mode": mode_val,
                "service": "LTL",
                "temperature": temperature_val,
            },
        }
    else:
        # Non-Pepsi (current) payload structure; omit temperature when multiple/zero NPOP commodities (API rejects blank value)
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
                "destination": str(delivery_result["id"]) if delivery_result and delivery_result.get("id") else "",
            },
            "parties": {
                "customer": str(pickup_result["company_id"]) if pickup_result and pickup_result.get("company_id") else "",
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
        # Excluded vendors (priority over Cannot read address): do not create payload, do not run shipment API; custom status
        vendorno_raw = (_cell_str(row.get("vendorno")) or "").strip()
        if vendorno_raw in EXCLUDED_VENDOR_STATUS:
            payloads.append({
                "row_index": row_index,
                "payload": {},
                "payload_type": "pepsi" if _norm(_cell_str(row.get("template_flag"))) == "pepsi" else "non_pepsi",
                "errors": None,
                "cannot_read_address": False,
                "excluded_vendor_status": EXCLUDED_VENDOR_STATUS[vendorno_raw],
            })
            continue

        # Cannot read address: only when Delivery shipto (alias value) is NULL or blank. Pickup is alias-only so we do not block on shipfrom_street.
        if _is_street_null_or_blank(row.get(DELIVERY_SHIPTO_COLUMN)):
            payloads.append({
                "row_index": row_index,
                "payload": {},
                "payload_type": "pepsi" if _norm(_cell_str(row.get("template_flag"))) == "pepsi" else "non_pepsi",
                "errors": ["Cannot read address (street is NULL)"],
                "cannot_read_address": True,
            })
            continue

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
            "cannot_read_address": False,
            "pickup_location_name": (pickup_res.get("location_name") or "").strip() if pickup_res else "",
            "delivery_location_name": (delivery_res.get("location_name") or "").strip() if delivery_res else "",
            "pickup_alias_source": (pickup_res.get("alias_source") or "").strip() if pickup_res else "",
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
    "monday_group_name",
    "Pickup Alias Lookup Request",
    "Pickup Alias Lookup Response",
    "Dest. Alias Lookup Request",
    "Dest. Alias Lookup Response",
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
STATUS_CANNOT_READ_ADDRESS = "Cannot read address (ERROR)"

# Error message stored in location result when API returns {"locations": []}
ERROR_EMPTY_LOCATIONS_ADDRESS = "Cannot read address"

# Vendors for which we do not create payload or call shipment API; custom status per vendor (row still written to output CSV).
EXCLUDED_VENDOR_STATUS: Dict[str, str] = {
    "201286": "Haleon , shipment is not to be created",
    "212346": "Lindt, shipment is not to be created",
}


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
    Required-field statuses use input CSV (null/empty). Alias Lookup failure when Pickup or Delivery API returns empty (shipment create API not called).
    """
    errors = item.get("errors") or []
    is_pepsi = item.get("payload_type") == "pepsi"

    # Excluded vendor (priority): no payload, no shipment API; custom status (Haleon / Lindt)
    if item.get("excluded_vendor_status"):
        return item["excluded_vendor_status"]

    # Street was NULL → placeholder row; no payload, no shipment API; Status = Cannot read address (ERROR)
    if item.get("cannot_read_address"):
        return STATUS_CANNOT_READ_ADDRESS

    # Pickup or Delivery API returned empty → Alias Lookup failure (do not call shipment create API)
    if errors:
        return STATUS_ALIAS_LOOKUP_FAILURE

    # No destination in payload (edge case)
    dest = _payload_get(payload, "locations", "destination")
    if not (dest or "").strip():
        return STATUS_DEST_MISSING

    # Required fields from input CSV (null or empty)
    if not (cell_fn("vendorno") or "").strip():
        return STATUS_VENDOR_MISSING
    if not (cell_fn("pickupDate") or "").strip():
        return STATUS_PICKUP_DATE_MISSING
    if not (cell_fn("delDate") or "").strip():
        return STATUS_DELIVERY_DATE_MISSING
    if not (cell_fn("po") or "").strip():
        return STATUS_PO_NOT_FOUND

    weight = (cell_fn("weight") or "").strip()
    cube = (cell_fn("cubes") or "").strip()
    cases = (cell_fn("cases") or "").strip()
    if not weight or not cube or not cases:
        return STATUS_CUBE_WEIGHT_CASES_MISSING

    service = payload.get("service") or {}
    temperature = service.get("temperature", "")
    # Pepsi always has temp from CSV; non-Pepsi we sometimes omit key when multiple commodities
    if is_pepsi and not (temperature or "").strip():
        return STATUS_MISSING_TEMP
    if not is_pepsi and "temperature" in service and not (temperature or "").strip():
        return STATUS_MISSING_TEMP
    # Mode not found from lookup table
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
    api_request_response_by_row: Optional[Dict[int, Dict[str, Dict[str, Any]]]] = None,
) -> None:
    """
    Write one row per payload to output CSV. Column mapping from payload + input row + create_response.
    """
    api_by_row = api_request_response_by_row or {}

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

            def cell_lifts_pallets(col: str) -> str:
                """Return cell value; use '0' when value is NULL or empty (for output CSV)."""
                v = cell(col)
                if not (v or "").strip() or str(v).strip().upper() == "NULL":
                    return "0"
                return v

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

            # Description: success → "Shipment created successfully by BOT"; duplicate PO → "Duplicate Po"; else blank
            description_val = ""
            if create_response:
                body_str = (create_response.get("body") or "").strip()
                msg_upper = (create_response.get("message") or "").upper()
                is_duplicate = (
                    "SHIP.PO.DUPLICATE" in body_str
                    or "ALREADY EXISTS" in msg_upper
                    or "DUPLICATE" in msg_upper
                )
                if create_response.get("success") and not is_duplicate:
                    description_val = "Shipment created successfully by BOT"
                elif is_duplicate:
                    description_val = "Duplicate Po"

            row_api = api_by_row.get(row_index, {})


            writer.writerow({
                "Item ID": cell("item_id"),
                "JSON Request": json.dumps(payload, ensure_ascii=False),
                "JSON Response": json_response,
                "Description": description_val,
                "Purchase Order": cell("po"),
                "Vendor #": cell("vendorno"),
                "Pick Up Date": _payload_get(payload, "dates", "pickup_date"),
                "Delivery Date": _payload_get(payload, "dates", "delivery_date"),
                "Weight": cell("weight"),
                "Cube": cell("cubes"),
                "Lifts": cell_lifts_pallets("lifts"),
                "Pallets": cell_lifts_pallets("pallets"),
                "Origin / Load At": (item.get("pickup_location_name") or "").strip(),
                "Destination / Delivery Location": (item.get("delivery_location_name") or "").strip(),
                "Customer / PickUp Company": (item.get("pickup_alias_source") or "").strip(),
                "Client / Consignee": "PEPSI" if is_pepsi else "SOBEYS",
                "Mode": _payload_get(payload, "service", "mode"),
                "Service": _payload_get(payload, "service", "service"),
                "Temperature": temperature,
                "API Call Result": api_result,
                "Status": status,
                "Load Number (Pepsi)": load_number_pepsi,
                "Order # (Pepsi)": order_pepsi,
                "monday_group_name": cell("monday_group_name"),
                "Pickup Alias Lookup Request": json.dumps(row_api.get("Pickup", {}).get("request", {}), ensure_ascii=False) if row_api else "",
                "Pickup Alias Lookup Response": json.dumps(row_api.get("Pickup", {}).get("response", {}), ensure_ascii=False) if row_api else "",
                "Dest. Alias Lookup Request": json.dumps(row_api.get("Delivery", {}).get("request", {}), ensure_ascii=False) if row_api else "",
                "Dest. Alias Lookup Response": json.dumps(row_api.get("Delivery", {}).get("response", {}), ensure_ascii=False) if row_api else "",
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
    - location_type "both" (default): CSV must have vendorno, shipfrom_street, shipto, template_flag, description, monday_group_name, consignee.
      Pickup if alias_source/vendorno/shipfrom_street present. Delivery if shipto present (alias_value = shipto column for all cases).
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
    alias_value_col = req_pickup.get("alias_value", REQUIRED_CSV_COLUMNS_PICKUP["alias_value"])
    ship_from_col = req_pickup.get("street_address", REQUIRED_CSV_COLUMNS_PICKUP["street_address"])

    # alias_source is derived from template_flag, vendorno, description, monday_group_name, consignee
    if run_both:
        required_cols = [alias_value_col, ship_from_col, DELIVERY_SHIPTO_COLUMN] + list(ALIAS_SOURCE_COLUMNS)
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return {
                "error": f"CSV missing required column(s): {missing}. Required: vendorno, shipfrom_street, shipto, template_flag, description, monday_group_name, consignee. Available: {list(df.columns)}",
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
            if DELIVERY_SHIPTO_COLUMN not in df.columns:
                return {
                    "error": f"CSV missing required column for Delivery: '{DELIVERY_SHIPTO_COLUMN}'. Available: {list(df.columns)}",
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

    # Hardcoded destination lookups to resolve multiple Delivery locations (pick first whose id is in table)
    sobeys_dest_set: Optional[Set[str]] = SOBEYS_DESTINATION_IDS if run_both else None
    pepsi_dest_set: Optional[Set[str]] = PEPSI_DESTINATION_IDS if run_both else None

    def _cell_str(val: Any) -> str:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return str(val).strip()

    if run_both:
        def _row_has_any_key(row: Any) -> bool:
            return bool(
                _cell_str(row.get(alias_value_col))
                or _cell_str(row.get(ship_from_col))
                or _cell_str(row.get(DELIVERY_SHIPTO_COLUMN))
            )
    elif location_type == LOCATION_TYPE_PICKUP:
        def _row_has_any_key(row: Any) -> bool:
            return bool(
                _cell_str(row.get(alias_value_col))
                or _cell_str(row.get(ship_from_col))
            )
    else:
        def _row_has_any_key(row: Any) -> bool:
            return bool(_cell_str(row.get(DELIVERY_SHIPTO_COLUMN)))

    df = df[df.apply(_row_has_any_key, axis=1)].reset_index(drop=True)

    def _empty_result(
        row_index: int,
        error: str,
        loc_type: str,
        street_address: Optional[str] = None,
        alias_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "row_index": row_index,
            "id": None,
            "company_id": None,
            "province": None,
            "location_type": loc_type,
            "street_address": street_address or "",
            "error": error,
        }
        if alias_source is not None:
            out["alias_source"] = alias_source
        return out

    def _success_result(
        row_index: int,
        loc: Dict[str, Any],
        loc_type: str,
        street_address: Optional[str] = None,
        alias_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "row_index": row_index,
            "id": loc["id"],
            "company_id": loc["company_id"],
            "province": loc.get("province"),
            "location_type": loc_type,
            "street_address": street_address or "",
            "error": None,
            "location_name": loc.get("location_name") or "",
            "alias_source": loc.get("alias_source") or alias_source or "",
            "commodities": loc.get("commodities") or [],  # temperature_requirement values for LA3/LA6 payload temperature
        }
        return out

    results: List[Dict[str, Any]] = []
    api_request_response_by_row: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for idx, row in df.iterrows():
        row_index = int(idx) + 1
        api_request_response_by_row[row_index] = {}
        alias_source_val = get_alias_source_from_row(row, _cell_str)
        alias_value_val = _cell_str(row.get(alias_value_col))
        ship_from_val = _cell_str(row.get(ship_from_col))
        shipto_val = _cell_str(row.get(DELIVERY_SHIPTO_COLUMN)) if run_both or location_type == LOCATION_TYPE_DELIVERY else ""

        # Cannot read address: only when Delivery needs shipto (alias value) and it is NULL/blank. Pickup is alias-only so we do not block on shipfrom_street.
        if run_both and _is_street_null_or_blank(shipto_val):
            results.append(_empty_result(row_index, "Cannot read address (street is NULL)", LOCATION_TYPE_BOTH, street_address="", alias_source=None))
            continue
        if not run_both and location_type == LOCATION_TYPE_DELIVERY and _is_street_null_or_blank(shipto_val):
            results.append(_empty_result(row_index, "Cannot read address (street is NULL)", LOCATION_TYPE_DELIVERY, street_address=shipto_val or "", alias_source=None))
            continue

        types_to_run: List[str] = []
        if run_both:
            if alias_source_val and alias_value_val:
                types_to_run.append(LOCATION_TYPE_PICKUP)
            if shipto_val:
                types_to_run.append(LOCATION_TYPE_DELIVERY)
        else:
            if location_type == LOCATION_TYPE_PICKUP and alias_source_val and alias_value_val:
                types_to_run.append(LOCATION_TYPE_PICKUP)
            elif location_type == LOCATION_TYPE_DELIVERY and shipto_val:
                types_to_run.append(LOCATION_TYPE_DELIVERY)

        if not types_to_run:
            if run_both:
                results.append(_empty_result(row_index, "Missing required fields for both Pickup and Delivery", LOCATION_TYPE_BOTH, street_address="", alias_source=None))
            elif location_type == LOCATION_TYPE_PICKUP:
                results.append(_empty_result(row_index, "Missing required field (alias_source derived from template_flag/vendorno/description/monday_group_name/consignee, or vendorno)", LOCATION_TYPE_PICKUP, street_address=ship_from_val or "", alias_source=alias_source_val or None))
            else:
                results.append(_empty_result(row_index, f"Missing required field ({DELIVERY_SHIPTO_COLUMN})", LOCATION_TYPE_DELIVERY, street_address=shipto_val or "", alias_source=None))
            continue

        for run_type in types_to_run:
            is_pepsi_row = _norm(_cell_str(row.get("template_flag"))) == "pepsi"
            if run_type == LOCATION_TYPE_PICKUP:
                street_val = ship_from_val
            else:
                street_val = shipto_val  # for result display only; Delivery request uses only alias
            alias_for_result = alias_source_val if run_type == LOCATION_TYPE_PICKUP else None
            request_dict: Dict[str, Any] = {
                "include_commodities": include_commodities,
                "type": run_type,
            }
            delivery_alias_source: Optional[str] = None
            delivery_alias_value: Optional[str] = None

            if run_type == LOCATION_TYPE_PICKUP and alias_source_val and alias_value_val:
                # First request: alias only (no street_address). If multiple locations, fallback adds street_address below.
                request_dict["alias_source"] = alias_source_val
                request_dict["alias_value"] = str(alias_value_val)
            elif run_type == LOCATION_TYPE_DELIVERY:
                # All Delivery (including RSC40/RSC50): alias only. alias_value = shipto column.
                delivery_alias_source = PEPSI_DELIVERY_ALIAS_SOURCE if is_pepsi_row else SOBEYS_DELIVERY_ALIAS_SOURCE
                delivery_alias_value = shipto_val
                request_dict["alias_source"] = delivery_alias_source
                request_dict["alias_value"] = delivery_alias_value
            try:
                # Pickup: first request is alias-only (no street_address). Delivery: no street.
                response = search_location(
                    access_token=access_token,
                    search_url=search_url,
                    search_type=run_type,
                    include_commodities=include_commodities,
                    street_address=None,
                    alias_source=alias_source_val if run_type == LOCATION_TYPE_PICKUP else delivery_alias_source,
                    alias_value=alias_value_val if run_type == LOCATION_TYPE_PICKUP else delivery_alias_value,
                )
                api_request_response_by_row[row_index][run_type] = {"request": request_dict, "response": response}
                location_results = extract_location_results(response)
                result_street = street_val
                # Pickup: if multiple locations, match first number of shipfrom_street to street_address first number (no fallback API)
                if run_type == LOCATION_TYPE_PICKUP and len(location_results) > 1:
                    resolved = _resolve_pickup_location_by_street_number(location_results, street_val or "")
                    if resolved is not None:
                        location_results = [resolved]
                    else:
                        location_results = []
                if not location_results:
                    err_msg = ERROR_EMPTY_LOCATIONS_ADDRESS if (response.get("locations") or []) == [] else "No locations in response"
                    if run_type == LOCATION_TYPE_PICKUP and (response.get("locations") or []):
                        if len(response.get("locations", [])) > 1:
                            err_msg = "Multiple Pickup locations; no match for shipfrom_street first number"
                    results.append(_empty_result(row_index, err_msg, run_type, street_address=result_street, alias_source=alias_for_result))
                else:
                    if run_type == LOCATION_TYPE_DELIVERY and len(location_results) > 1:
                        dest_set = pepsi_dest_set if is_pepsi_row else sobeys_dest_set
                        loc = _resolve_delivery_location(location_results, dest_set)
                    else:
                        loc = location_results[0]
                    results.append(_success_result(row_index, loc, run_type, street_address=result_street, alias_source=alias_for_result))
            except requests.RequestException as e:
                results.append(_empty_result(row_index, str(e), run_type, street_address=street_val, alias_source=alias_for_result))
            except Exception as e:
                results.append(_empty_result(row_index, str(e), run_type, street_address=street_val, alias_source=alias_for_result))

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
                if item.get("cannot_read_address"):
                    item["create_response"] = {
                        "status_code": 0,
                        "body": "",
                        "success": False,
                        "message": "Skipped (Cannot read address - street is NULL)",
                    }
                elif item.get("excluded_vendor_status"):
                    item["create_response"] = {
                        "status_code": 0,
                        "body": "",
                        "success": False,
                        "message": f"Skipped ({item['excluded_vendor_status']})",
                    }
                elif item.get("errors"):
                    # Alias Lookup failure: Pickup or Delivery API returned empty → do not call shipment create API
                    item["create_response"] = {
                        "status_code": 0,
                        "body": "",
                        "success": False,
                        "message": "Skipped (Alias Lookup failure - Pickup or Delivery API returned empty)",
                    }
                else:
                    item["create_response"] = create_shipment_via_api(
                        item.get("payload") or {}, shipment_config
                    )

        result_data["payloads"] = payloads

        if output_csv_path:
            try:
                write_output_csv(output_csv_path, df, payloads, _cell_str, api_request_response_by_row)
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
