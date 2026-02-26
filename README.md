# LA Location Search Toolkit

Reads an input CSV, obtains a fresh Cognito OAuth2 token on each run, calls the location/search API for each row, and returns **id**, **company_id**, and **province** for each location. **By default, one execution runs both Pickup and Delivery** for every row (when the CSV has the required columns). You can still run only Pickup or only Delivery by setting `location_type`.

## Project structure

```
la_location_search/
├── config.example.json  # Example config structure (no secrets); copy and fill your values
├── main.py              # Toolkit entrypoint (stdin JSON → stdout JSON)
├── requirements.txt     # Python dependencies
├── toolkit.json         # Toolkit metadata and capabilities
└── README.md            # This file
```

**Do not commit `config.json`** — it contains secrets. Use `config_path` to point to your config file (e.g. outside the repo). `.gitignore` excludes `config.json`.

## Configuration (config_path required)

**`config_path` is a required argument.** Pass the path to your config file on every run. The config file contains secrets (Cognito, API keys) and must not be committed to the repo.

- **cognito**: `token_url` (or `cognito_domain`), `client_id`, `client_secret`, `scope` for OAuth2 client_credentials.
- **location_api**: `search_url` for the location/search API.
- **shipment_api** (optional): When present, each built payload is POSTed to the shipment create API. Use `region`, `service`, `baseUrl`, `apiKey`, `accessKey`, `secretKey` (AWS4Auth + x-api-key).
- **csv_columns** (optional): Override the CSV column names the toolkit expects. Use this when your CSV uses different headers than the defaults:
  - **alias_value**: column for vendor number (default `vendorno`). Used for Pickup.
  - **street_address**: column for pickup street address (default `shipfrom_street`).
  - **ship_to_street_address**: column for delivery street address (default `shipto_street`).
  - `alias_source` is **not** a column — it is derived in code from `template_flag`, `vendorno`, `description`, `monday_group_name`, `consignee`.

See `config.example.json` for the full structure (no real secrets).

The token is requested **on every run** because it expires in 1 hour.

## Two cases

### Case 1: Pickup

- **Parameters sent to API**: `alias_source`, `alias_value`, `street_address`, `include_commodities: true`, `type: "Pickup"`.
- **alias_source** is **derived** from CSV (not read from a single column). **Required CSV columns**: `template_flag`, `vendorno`, `description`, `monday_group_name`, `consignee`, `shipfrom_street`.
- **alias_value** = `vendorno`.
- **alias_source logic**:
  - **PEPSI TENDER** (template_flag = "Pepsi"): vendorno 204047 → "PEPSI TENDER - FOOD vendor"; 21200Y → "PEPSI TENDER - BEVERAGE"; 21200E → "PEPSI TENDER - QUAKER".
  - **SOBEYS TENDER** (template_flag = "template-1" or Null): (1) If **monday_group_name** exactly equals the single string `NPOP (LA6)_{MIFLAOPS}.pdf` → "SOBEYS TENDER - LA6". (2) Else if **description** contains "M&M" (exact substring) → "SOBEYS TENDER - M&M". (3) Else if description does not have "M&M" and monday_group_name ≠ that exact string and **consignee** has RSC8/RSC9/RSC12/CFC3 → "SOBEYS TENDER - OTR". (4) Else if description has neither "M&M" nor "OTR" → "SOBEYS TENDER - ADMIN".
- **Returned from response**: `id`, `company_id`, `province` (from `location.province`).

### Case 2: Delivery

- **Parameters sent to API**: `street_address`, `type: "Delivery"`. The **street_address** value is strictly the **shipto_street** column from the input CSV.
- **Required CSV column** (strict):

| Parameter       | CSV column name             |
|-----------------|-----------------------------|
| street_address  | shipto_street               |

- **Returned from response**: `id`, `company_id`, `province` (from `location.province`).

If any required column is missing from the CSV or empty for a row, the toolkit returns an error (or a per-row error for empty values).

### Default: run both (Pickup and Delivery)

When `location_type` is omitted or set to `"both"`, **one execution runs both Pickup and Delivery**. The CSV must have: `vendorno`, `shipfrom_street`, `shipto_street`, `template_flag`, `description`, `monday_group_name`, `consignee`. For each row:

- **alias_source** is derived from template_flag, vendorno, description, monday_group_name, consignee (see Pickup logic above). If the row has a non-empty alias_source, vendorno, and shipfrom_street, the Pickup API is called and results are tagged `"location_type": "Pickup"`.
- If the row has shipto_street, the Delivery API is called with that value as street_address and results are tagged `"location_type": "Delivery"`.
- A row can have both, one, or neither (rows with neither get a single error entry).

## Usage

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure**  
   Copy `config.example.json` to a config file (e.g. outside the repo), fill in your Cognito and API settings, and pass its path as `config_path`. Do not commit that file.

3. **Run both Pickup and Delivery (default)**

   CSV must have: `vendorno`, `shipfrom_street`, `shipto_street`, `template_flag`, `description`, `monday_group_name`, `consignee`. **Required:** `csv_path`, `config_path`.

   ```powershell
   echo '{"capability":"location_search","args":{"csv_path":"C:\\Users\\neeha\\Downloads\\test_data.csv","config_path":"C:\\path\\to\\your\\config.json","transit_time_xlsx_path":"C:\\path\\to\\Transit Time - BOT (PROD).xlsx"}}' | python main.py
   ```

   To write an **output CSV** with one row per payload (request/response, description, PO, vendor, dates, weight, cube, origin/destination, customer/client, mode, service, temperature, API call result):

   ```powershell
   echo '{"capability":"location_search","args":{"csv_path":"C:\\...\\test_data.csv","config_path":"C:\\...\\your_config.json","transit_time_xlsx_path":"C:\\...\\Transit Time - BOT (PROD).xlsx","output_csv_path":"C:\\...\\output_sheet.csv"}}' | python main.py
   ```

4. **Run only Pickup or only Delivery**

   ```powershell
   echo '{"capability":"location_search","args":{"csv_path":"C:\\Users\\neeha\\Downloads\\test_data.csv","config_path":"C:\\path\\to\\your\\config.json","location_type":"Pickup"}}' | python main.py
   echo '{"capability":"location_search","args":{"csv_path":"C:\\Users\\neeha\\Downloads\\test_data.csv","config_path":"C:\\path\\to\\your\\config.json","location_type":"Delivery"}}' | python main.py
   ```

   Optional args: `include_commodities` (Pickup only, default `true`).

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
  - `result.payloads`: one shipment payload per CSV row. If **shipment_api** is in config, each item has `create_response`: `{ status_code, body, success, message }` from the shipment create API.
  - `result.payload_setup_errors`: present if the holiday list could not be loaded
  - `result.output_csv_path`: set when `output_csv_path` was provided and the file was written successfully
  - `result.output_csv_error`: set if writing the output CSV failed

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

**Two payload types:** Rows with `template_flag` = "Pepsi" get a **Pepsi** payload (description "Shipment Creation by BOT (PEPSI)", `customer_shipment`, `invoice_reference`, `dates.pickup_appointment`). All other rows get the **non-Pepsi** payload (description "Shipment Creation by BOT"). Both use the same hardcoded Origin×Destination table for `service.mode`. Each payload entry includes `payload_type`: `"pepsi"` or `"non_pepsi"`.

**Payload mapping (non-Pepsi):**
- `purchase_order`: CSV column `po`
- `dates.pickup_date`: CSV column `pickupDate` (output always **YYYY-MM-DD**; compact input e.g. `21726` parsed as 21-7-26 → `2026-07-21`)
- `dates.delivery_date`: CSV column `delDate` (output always **YYYY-MM-DD**; same compact parsing)
- `quantities.declared.*`: CSV columns `weight`, `cubes`, `cases`, `lifts`, `pallets`
- `locations.origin`: `id` from Pickup result; `locations.destination`: `company_id` from Pickup result
- `parties.customer`: `id` from Delivery result; `parties.client`: `company_id` from Delivery result
- `service.mode`: from hardcoded Origin×Destination table (ROAD within Eastern or within Western; RAIL between Eastern and Western)
- `service.temperature`: From CSV column **shipment_type** (short code) mapped to Temp Range: (FROZ)→FROZEN, (GROC)→DRY, (DAIR)/(MEAT)/(YGRT)/(FSMT)→FRESH, (REPK)/(GRPK)→DRY. **Exception:** for `monday_group_name` = "NPOP (LA3)/{SOBEYSMIF}.pdf" or "NPOP (LA6)/{MIFLAOPS}.pdf" when `shipment_type` is NULL, temperature is taken from the **Pickup** API result’s commodities: if the location has exactly one `temperature_requirement`, that value is used; if multiple, the temperature key is omitted from the payload.

**Payload mapping (Pepsi):** Same as above, plus: `description` = "Shipment Creation by BOT (PEPSI)"; `customer_shipment` and `dates.pickup_appointment` from CSV column **pickApptNo**; `invoice_reference` from CSV column **invoiceRef**; `service.mode` from same hardcoded Origin×Destination table; `service.temperature` from CSV column **temp**.

## Output CSV (output_sheet.csv)

When `output_csv_path` is provided, one row per payload is written with these columns:

| Output column | Source |
|---------------|--------|
| Item ID | Input CSV `item_id` |
| JSON Request | Payload sent to shipment create API (JSON string) |
| JSON Response | Response body from shipment create API |
| Description | Input CSV `description` |
| Purchase Order | Input CSV `po` |
| Vendor # | Input CSV `vendorno` |
| Pick Up Date | Payload `dates.pickup_date` |
| Delivery Date | Payload `dates.delivery_date` |
| Weight | Input CSV `weight` |
| Cube | Input CSV `cubes` |
| Lifts | Input CSV `lifts` |
| Pallets | Input CSV `pallets` |
| Origin / Load At | Payload `locations.origin` |
| Destination / Delivery Location | Payload `locations.destination` |
| Customer / PickUp Company | Payload `parties.customer` |
| Client / Consignee | Payload `parties.client` |
| Mode | Payload `service.mode` |
| Service | Payload `service.service` |
| Temperature | Payload `service.temperature` |
| API Call Result | Shipment API result message (success, error, skipped, or "Not sent" if API not configured) |
| Status | One of: In Queue, Created, PO already exists in Altruos, Retry; or error statuses (Vendor # Missing, Dest. Missing, PICKUP/DELIVERY Date Missing, Cube/Weight/Cases Missing, Missing Temp, Missing Mode, JSON not sent, Customer Missing, Cannot read address (ERROR), ERROR); or Pepsi-specific (Pallets/Load Number/Client/Order # Missing (Pepsi)). **Cannot read address (ERROR)** is set when the location/search API returns `{"locations": []}` for the given street_address (Pickup or Delivery). |
| Load Number (Pepsi) | Input CSV `invoiceRef` (Pepsi rows only; empty for non-Pepsi) |
| Order # (Pepsi) | Input CSV `po` (Pepsi rows only; empty for non-Pepsi) |
