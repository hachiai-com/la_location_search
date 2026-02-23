# LA Location Search Toolkit

Reads an input CSV, obtains a fresh Cognito OAuth2 token on each run, calls the location/search API for each row, and returns **id**, **company_id**, and **province** for each location. **By default, one execution runs both Pickup and Delivery** for every row (when the CSV has the required columns). You can still run only Pickup or only Delivery by setting `location_type`.

## Project structure

```
la_location_search/
├── config.json      # Credentials and API URLs (Cognito + location/search)
├── main.py          # Toolkit entrypoint (stdin JSON → stdout JSON)
├── requirements.txt # Python dependencies
├── toolkit.json     # Toolkit metadata and capabilities
└── README.md        # This file
```

## Configuration (config.json)

- **cognito**: `token_url` (or `cognito_domain`), `client_id`, `client_secret`, `scope` for OAuth2 client_credentials.
- **location_api**: `search_url` for the location/search API.
- **csv_columns** (optional): Override CSV column names (see mapping below).

The token is requested **on every run** because it expires in 1 hour.

## Two cases

### Case 1: Pickup

- **Parameters sent to API**: `alias_source`, `alias_value`, `street_address`, `include_commodities: true`, `type: "Pickup"`.
- **alias_source** is **derived** from CSV (not read from a single column). **Required CSV columns**: `template_flag`, `vendorno`, `description`, `monday_group_name`, `consignee`, `shipfrom_street`.
- **alias_value** = `vendorno`.
- **alias_source logic**:
  - **PEPSI TENDER** (template_flag = "Pepsi"): vendorno 204047 → "PEPSI TENDER - FOOD vendor"; 21200Y → "PEPSI TENDER - BEVERAGE"; 21200E → "PEPSI TENDER - QUAKER".
  - **SOBEYS TENDER** (template_flag = "template-1" or Null): description has "M&M" → "SOBEYS TENDER - M&M"; monday_group_name has "NPOP (LA6)" and "MIFLAOPS" → "SOBEYS TENDER - LA6"; else consignee has RSC8/RSC9/RSC12/CFC3 (and no M&M, no NPOP in monday_group_name) → "SOBEYS TENDER - OTR"; else → "SOBEYS TENDER - ADMIN".
- **Returned from response**: `id`, `company_id`, `province` (from `location.province`).

### Case 2: Delivery

- **Parameters sent to API**: `street_address`, `type: "Delivery"`.
- **Required CSV column** (strict):

| Parameter       | CSV column name             |
|-----------------|-----------------------------|
| street_address  | shipto_street               |

- **Returned from response**: `id`, `company_id`, `province` (from `location.province`).

If any required column is missing from the CSV or empty for a row, the toolkit returns an error (or a per-row error for empty values).

### Default: run both (Pickup and Delivery)

When `location_type` is omitted or set to `"both"`, **one execution runs both Pickup and Delivery**. The CSV must have: `vendorno`, `ship_from_street_address`, `ship_to_street_address`, `template_flag`, `description`, `monday_group_name`, `consignee`. For each row:

- **alias_source** is derived from template_flag, vendorno, description, monday_group_name, consignee (see Pickup logic above). If the row has a non-empty alias_source, vendorno, and ship_from_street_address, the Pickup API is called and results are tagged `"location_type": "Pickup"`.
- If the row has ship_to_street_address, the Delivery API is called and results are tagged `"location_type": "Delivery"`.
- A row can have both, one, or neither (rows with neither get a single error entry).

## Usage

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure**  
   Edit `config.json` with your Cognito and location API settings.

3. **Run both Pickup and Delivery (default)**

   CSV must have: `vendorno`, `shipfrom_street`, `shipto_street`, `template_flag`, `description`, `monday_group_name`, `consignee`.

   ```powershell
   echo '{"capability": "location_search", "args": {"csv_path": "C:\\Users\\neeha\\Downloads\\test_data.csv", "transit_time_xlsx_path": "C:\\path\\to\\Transit Time - BOT (PROD).xlsx", "origins_destinations_xlsx_path": "C:\\path\\to\\Origins & Destinations - BOT (PROD).xlsx"}}' | python main.py
   ```

   For **Pepsi** rows (when `template_flag` is "Pepsi"), pass the Pepsi mode sheet so `service.mode` is looked up from it:

   ```powershell
   echo '{"capability": "location_search", "args": {"csv_path": "C:\\Users\\neeha\\Downloads\\test_data.csv", "transit_time_xlsx_path": "C:\\path\\to\\Transit Time - BOT (PROD).xlsx", "origins_destinations_xlsx_path": "C:\\path\\to\\Origins & Destinations - BOT (PROD).xlsx", "pepsi_sheet_path": "C:\\path\\to\\Pepsi Altruos (PROD).xlsx"}}' | python main.py
   ```

4. **Run only Pickup or only Delivery**

   ```powershell
   echo '{"capability": "location_search", "args": {"csv_path": "C:\\Users\\neeha\\Downloads\\test_data.csv", "location_type": "Pickup"}}' | python main.py
   echo '{"capability": "location_search", "args": {"csv_path": "C:\\Users\\neeha\\Downloads\\test_data.csv", "location_type": "Delivery"}}' | python main.py
   ```

   Optional args: `config_path`, `include_commodities` (Pickup only, default `true`).

**Note:** You must pipe the JSON into `python main.py` (the `| python main.py` part). Replace the CSV path with your file path.

## Output

- **Success**: JSON with `result.locations` (list of objects per location):
  - `row_index`: 1-based CSV row number
  - `id`: location id from API
  - `company_id`: company_id from API
  - `province`: province from API `location.province` (e.g. `"Nova Scotia"`)
  - `location_type`: `"Pickup"` or `"Delivery"` (or `"both"` for a row-level error when running both)
  - `error`: `null` or an error message for that row
- **Failure**: JSON with `error` and `capability` (e.g. missing config, CSV not found, wrong columns, auth failure).
- When running both, output also includes:
  - `result.payloads`: one shipment payload per CSV row
  - `result.payload_setup_errors`: present if the holiday list or mode matrix could not be loaded

## Example response (run both)

When running both Pickup and Delivery, the response includes both `locations` and `payloads`:

```json
{
  "result": {
    "locations": [
      { "row_index": 1, "id": 1932, "company_id": 228, "province": "Ontario", "location_type": "Pickup", "error": null },
      { "row_index": 1, "id": 989, "company_id": 240, "province": "Nova Scotia", "location_type": "Delivery", "error": null }
    ],
    "total_rows": 2,
    "location_type": "both",
    "payloads": [
      {
        "row_index": 1,
        "payload": {
          "description": "Shipment Creation by BOT",
          "purchase_order": "4525290970",
          "dates": {
            "pickup_date": "2025-11-28",
            "delivery_date": "2025-12-05"
          },
          "quantities": {
            "declared": {
              "weight": 1406.8,
              "weight_unit": "lbs",
              "cube": 105.4,
              "cube_unit": "ft3",
              "cases": 150,
              "lifts": 0,
              "pallets": 0
            }
          },
          "locations": {
            "origin": "1932",
            "destination": "228"
          },
          "parties": {
            "customer": "989",
            "client": "240"
          },
          "service": {
            "mode": "ROAD",
            "service": "LTL",
            "temperature": "FROZEN"
          }
        },
        "errors": null
      }
    ]
  },
  "capability": "location_search"
}
```

**Two payload types:** Rows with `template_flag` = "Pepsi" get a **Pepsi** payload (description "Shipment Creation by BOT (PEPSI)", `customer_shipment`, `invoice_reference`, `dates.pickup_appointment`, `service.mode` from Pepsi Modes sheet when `pepsi_sheet_path` is provided). All other rows get the **non-Pepsi** payload (description "Shipment Creation by BOT", `service.mode` from Origins & Destinations Modes sheet). Each payload entry includes `payload_type`: `"pepsi"` or `"non_pepsi"`.

**Payload mapping (non-Pepsi):**
- `purchase_order`: CSV column `po`
- `dates.pickup_date`: CSV column `pickupDate` (output always **YYYY-MM-DD**; compact input e.g. `21726` parsed as 21-7-26 → `2026-07-21`)
- `dates.delivery_date`: CSV column `delDate` (output always **YYYY-MM-DD**; same compact parsing)
- `quantities.declared.*`: CSV columns `weight`, `cubes`, `cases`, `lifts`, `pallets`
- `locations.origin`: `id` from Pickup result; `locations.destination`: `company_id` from Pickup result
- `parties.customer`: `id` from Delivery result; `parties.client`: `company_id` from Delivery result
- `service.mode`: from Origins & Destinations Modes sheet (pickup province × delivery province)
- `service.temperature`: "FROZEN" (placeholder; calculation TBD)

**Payload mapping (Pepsi):** Same as above, plus: `description` = "Shipment Creation by BOT (PEPSI)"; `customer_shipment` and `dates.pickup_appointment` from CSV column `pickApptNo` (CN Customer Reference #; column name may be updated); `invoice_reference` from CSV column `invoiceRef`; `service.mode` from **Pepsi Altruos (PROD).xlsx** sheet "Modes" when `pepsi_sheet_path` is provided; `service.temperature` = "DRY" (placeholder; calculation TBD).

## Checklist

- [x] toolkit.json has correct slug and capability
- [x] main.py reads JSON from stdin and writes JSON to stdout
- [x] capability returns JSON with `result` or `error`
- [x] requirements.txt lists dependencies
- [x] Credentials in config.json; token obtained on each run (1 hr expiry)
