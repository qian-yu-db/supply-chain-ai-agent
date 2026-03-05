# Synthetic Data Generation Prompt

## Supply Chain Disruption Response Agent — Mock Data

> Use this prompt to generate interconnected mock datasets for the Supply Chain Visibility & Disruption Response Agent demo on Databricks.

---

### Prompt

Write a Python script using `faker` and `random` to generate interconnected CSV datasets for a supply chain disruption demo. Save all files to a `mock_data/` directory.

**Datasets needed:**

1. **`suppliers.csv`** (30 rows) — `supplier_id`, `supplier_name`, `tier` (1 or 2), `country`, `lat`, `lon`, `lead_time_days`, `reliability_score` (0.0–1.0). Include suppliers in Southeast Asia, Europe, and North America. Tier 2 suppliers should feed into Tier 1 suppliers via a `parent_supplier_id` column (null for Tier 1).

2. **`parts_catalog.csv`** (50 SKUs) — `sku_id`, `part_name`, `category` (e.g., semiconductor, motor, housing, connector), `unit_cost`, `supplier_id` (FK), `alt_supplier_id` (FK, a different supplier for the same part), `is_critical_path` (boolean, ~30% true), `cost_air_freight_multiplier` (2x–8x, for calculating Cost-to-Serve of air alternatives).

3. **`supplier_inventory.csv`** (one row per SKU per supplier) — `supplier_id`, `sku_id`, `quantity_on_hand`, `reorder_point`, `days_of_supply`. Some critical-path parts should have dangerously low inventory (<5 days of supply).

4. **`production_lines.csv`** (5 rows) — `production_line_id`, `product_name`, `daily_output_units`, `daily_revenue` ($500K–$2M/day), `shutdown_cost_per_day`. These represent real manufacturing lines that depend on parts from purchase orders.

5. **`purchase_orders.csv`** (100 rows) — `po_id`, `sku_id`, `supplier_id`, `quantity`, `order_date`, `expected_delivery_date`, `production_line_id` (one of 5 lines: `PL-001` to `PL-005`), `status` (open/in_transit/delivered/delayed). ~20% should be `delayed`.

6. **`sea_freight_tracking.csv`** (200 rows, time-series) — `shipment_id`, `po_id`, `vessel_name`, `transport_mode` (sea/air/rail), `origin_port`, `destination_port`, `current_lat`, `current_lon`, `timestamp` (hourly over last 7 days), `speed_knots`, `eta`. Create realistic shipping lanes (e.g., Shanghai→Long Beach, Rotterdam→Savannah). A few shipments should be near the South China Sea or Gulf of Mexico (storm zones).

7. **`storm_alerts.csv`** (3 rows) — `alert_id`, `storm_name`, `center_lat`, `center_lon`, `radius_miles`, `severity` (category 1–5), `start_date`, `end_date`. Place one storm in the South China Sea and one in the Gulf of Mexico, overlapping with active shipment routes.

**Key constraints:**
- All foreign keys must be valid and consistent across files.
- At least 3 shipments should be within a storm's radius to create obvious disruption scenarios.
- Include enough data to demonstrate multi-tier tracing: a delayed Tier 2 supplier should cascade to a Tier 1 supplier that feeds a production line.
- Use realistic port names and shipping lanes, not random lat/lon values.
- Seed the random generator for reproducibility.

**Downstream usage (context for the generator):**
- Data will be uploaded to a Unity Catalog Volume under `Supply_Chain_Intelligence`.
- A Lakeflow Spark Declarative Pipeline will join `sea_freight_tracking` with `purchase_orders` to flag shipments within storm zones.
- A Lakebase (Serverless Postgres) table will store live shipment state and risk levels for sub-second alerting.
- A Reasoning Agent will use Vector Search on `parts_catalog` + `suppliers` to find alternative suppliers by proximity and reliability.
- A Databricks App ("Supply Chain Control Tower") will let users "Chat with the Shipment" to generate recovery plans with cost analysis.

---

### Original Prompt (from use case doc)

For comparison, here is the original prompt from the use case document:

> "Claude, write a Python script to generate a mock dataset for a supply chain. I need sea_freight_tracking with lat/long coordinates, supplier_inventory for 20 SKUs, and purchase_orders linked to a production line."

### What the improved prompt adds:
- **Referential integrity** — explicit foreign keys across all 7 tables
- **Built-in disruption scenarios** — storms overlapping shipment routes, low inventory on critical parts
- **Multi-tier tracing** — Tier 2 → Tier 1 parent relationships for cascade analysis
- **Cost-aware mitigation** — air freight multipliers + production line shutdown costs
- **Realistic geography** — named ports and shipping lanes instead of random coordinates
- **Downstream context** — tells the generator how the data will be used, so it can optimize for demo scenarios
