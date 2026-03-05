"""
Generate interconnected mock datasets for the Supply Chain Disruption Response Agent demo.
Saves CSV files to mock_data/ directory.
"""

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent / "mock_data"
OUTPUT_DIR.mkdir(exist_ok=True)

# --- Helper data ---
COUNTRIES_WITH_COORDS = {
    "China": (31.2, 121.5),
    "Vietnam": (10.8, 106.6),
    "Taiwan": (25.0, 121.5),
    "South Korea": (37.6, 127.0),
    "Japan": (35.7, 139.7),
    "Germany": (51.2, 6.8),
    "Netherlands": (51.9, 4.5),
    "Poland": (52.2, 21.0),
    "USA - Michigan": (42.3, -83.0),
    "USA - Texas": (29.8, -95.4),
    "USA - California": (34.1, -118.2),
    "Mexico": (19.4, -99.1),
    "India": (19.1, 72.9),
    "Thailand": (13.8, 100.5),
    "Malaysia": (3.1, 101.7),
}

PORTS = {
    "Shanghai": (31.23, 121.47),
    "Shenzhen": (22.54, 114.06),
    "Busan": (35.10, 129.04),
    "Rotterdam": (51.92, 4.48),
    "Hamburg": (53.55, 9.99),
    "Long Beach": (33.77, -118.19),
    "Savannah": (32.08, -81.09),
    "Houston": (29.76, -95.36),
    "Singapore": (1.26, 103.84),
    "Laem Chabang": (13.08, 100.88),
}

SHIPPING_LANES = [
    ("Shanghai", "Long Beach"),
    ("Shanghai", "Savannah"),
    ("Shenzhen", "Long Beach"),
    ("Busan", "Long Beach"),
    ("Rotterdam", "Savannah"),
    ("Hamburg", "Houston"),
    ("Singapore", "Long Beach"),
    ("Laem Chabang", "Houston"),
    ("Shenzhen", "Rotterdam"),
    ("Shanghai", "Hamburg"),
]

PART_CATEGORIES = {
    "semiconductor": ["MCU Controller", "Power IC", "MOSFET Driver", "FPGA Module", "Sensor IC",
                       "ADC Converter", "Voltage Regulator", "Memory Chip", "DSP Processor",
                       "GPU Compute Module"],
    "motor": ["Brushless DC Motor", "Stepper Motor", "Servo Actuator", "Linear Actuator",
              "Cooling Fan Assembly", "Micro Motor Unit", "Hydraulic Pump"],
    "housing": ["Aluminum Enclosure", "Steel Bracket", "Plastic Casing", "Heat Sink Assembly",
                "Mounting Plate", "Titanium Frame", "Composite Shell"],
    "connector": ["USB-C Port", "Power Connector", "Ribbon Cable", "Terminal Block",
                   "RF Connector", "Board-to-Board Connector", "Fiber Optic Coupler"],
    "mechanical": ["Bearing Assembly", "Gear Set", "Spring Mechanism", "Shaft Coupling",
                    "Gasket Kit", "O-Ring Set", "Linear Rail Guide"],
    "optical": ["LED Module", "Lens Assembly", "Light Guide", "Optical Sensor",
                "Laser Diode", "IR Emitter"],
    "passive": ["Capacitor Bank", "Inductor Coil", "Resistor Array", "Transformer Unit",
                "Fuse Module", "Crystal Oscillator"],
}

VESSEL_NAMES = [
    "Ever Given", "MSC Oscar", "COSCO Shipping Universe", "HMM Algeciras",
    "CMA CGM Jacques Saade", "ONE Apus", "Maersk Eindhoven", "Yang Ming Witness",
    "Evergreen Triton", "Hapag-Lloyd Express", "ZIM Samson", "PIL Aries",
    "OOCL Hong Kong", "Wan Hai 316", "Pacific Voyager",
]

PRODUCT_NAMES = [
    "EV Battery Pack Assembly",
    "Industrial Robot Controller",
    "Smart HVAC Control Unit",
    "Medical Imaging Module",
    "Autonomous Sensor Array",
]


def write_csv(filename, rows, fieldnames):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows)} rows -> {path}")


# ============================================================
# 1. SUPPLIERS (30 rows)
# ============================================================
def generate_suppliers():
    suppliers = []
    countries = list(COUNTRIES_WITH_COORDS.keys())

    # 20 Tier-1 suppliers
    for i in range(1, 21):
        country = countries[i % len(countries)]
        lat, lon = COUNTRIES_WITH_COORDS[country]
        suppliers.append({
            "supplier_id": f"SUP-{i:03d}",
            "supplier_name": f"{country.split(' - ')[0]} Parts Co. {i}",
            "tier": 1,
            "country": country,
            "lat": round(lat + random.uniform(-1, 1), 4),
            "lon": round(lon + random.uniform(-1, 1), 4),
            "lead_time_days": random.randint(7, 45),
            "reliability_score": round(random.uniform(0.6, 0.99), 2),
            "parent_supplier_id": "",
        })

    # 10 Tier-2 suppliers that feed into Tier-1
    for i in range(21, 31):
        country = random.choice(countries)
        lat, lon = COUNTRIES_WITH_COORDS[country]
        parent = f"SUP-{random.randint(1, 20):03d}"
        suppliers.append({
            "supplier_id": f"SUP-{i:03d}",
            "supplier_name": f"{country.split(' - ')[0]} Components Ltd. {i}",
            "tier": 2,
            "country": country,
            "lat": round(lat + random.uniform(-1, 1), 4),
            "lon": round(lon + random.uniform(-1, 1), 4),
            "lead_time_days": random.randint(14, 60),
            "reliability_score": round(random.uniform(0.5, 0.95), 2),
            "parent_supplier_id": parent,
        })

    write_csv("suppliers.csv", suppliers,
              ["supplier_id", "supplier_name", "tier", "country", "lat", "lon",
               "lead_time_days", "reliability_score", "parent_supplier_id"])
    return suppliers


# ============================================================
# 2. PARTS CATALOG (50 SKUs)
# ============================================================
def generate_parts_catalog(suppliers):
    tier1_ids = [s["supplier_id"] for s in suppliers if s["tier"] == 1]
    parts = []
    sku_num = 0

    for category, names in PART_CATEGORIES.items():
        for name in names:
            sku_num += 1
            if sku_num > 50:
                break
            primary = random.choice(tier1_ids)
            alt = random.choice([s for s in tier1_ids if s != primary])
            parts.append({
                "sku_id": f"SKU-{sku_num:04d}",
                "part_name": name,
                "category": category,
                "unit_cost": round(random.uniform(0.50, 500.00), 2),
                "supplier_id": primary,
                "alt_supplier_id": alt,
                "is_critical_path": sku_num % 3 == 0,  # ~33% critical
                "cost_air_freight_multiplier": round(random.uniform(2.0, 8.0), 1),
            })
        if sku_num > 50:
            break

    parts = parts[:50]
    write_csv("parts_catalog.csv", parts,
              ["sku_id", "part_name", "category", "unit_cost", "supplier_id",
               "alt_supplier_id", "is_critical_path", "cost_air_freight_multiplier"])
    return parts


# ============================================================
# 3. SUPPLIER INVENTORY (one row per SKU per supplier)
# ============================================================
def generate_supplier_inventory(parts):
    rows = []
    for part in parts:
        for sid in [part["supplier_id"], part["alt_supplier_id"]]:
            is_critical = part["is_critical_path"]
            # Critical parts sometimes have dangerously low inventory
            if is_critical and random.random() < 0.4:
                qty = random.randint(5, 50)
                days = random.randint(1, 4)  # dangerously low
            else:
                qty = random.randint(100, 10000)
                days = random.randint(10, 90)

            rows.append({
                "supplier_id": sid,
                "sku_id": part["sku_id"],
                "quantity_on_hand": qty,
                "reorder_point": random.randint(50, 500),
                "days_of_supply": days,
            })

    write_csv("supplier_inventory.csv", rows,
              ["supplier_id", "sku_id", "quantity_on_hand", "reorder_point", "days_of_supply"])
    return rows


# ============================================================
# 4. PRODUCTION LINES (5 rows)
# ============================================================
def generate_production_lines():
    lines = []
    for i, name in enumerate(PRODUCT_NAMES, 1):
        daily_rev = random.randint(500_000, 2_000_000)
        lines.append({
            "production_line_id": f"PL-{i:03d}",
            "product_name": name,
            "daily_output_units": random.randint(50, 500),
            "daily_revenue": daily_rev,
            "shutdown_cost_per_day": round(daily_rev * random.uniform(0.8, 1.5)),
        })

    write_csv("production_lines.csv", lines,
              ["production_line_id", "product_name", "daily_output_units",
               "daily_revenue", "shutdown_cost_per_day"])
    return lines


# ============================================================
# 5. PURCHASE ORDERS (100 rows)
# ============================================================
def generate_purchase_orders(parts, production_lines):
    now = datetime.now()
    statuses = ["open", "in_transit", "delivered", "delayed"]
    status_weights = [0.25, 0.35, 0.20, 0.20]
    pl_ids = [pl["production_line_id"] for pl in production_lines]
    orders = []

    for i in range(1, 101):
        part = random.choice(parts)
        order_date = now - timedelta(days=random.randint(5, 60))
        lead = random.randint(7, 45)
        expected = order_date + timedelta(days=lead)
        status = random.choices(statuses, weights=status_weights, k=1)[0]

        orders.append({
            "po_id": f"PO-{i:05d}",
            "sku_id": part["sku_id"],
            "supplier_id": part["supplier_id"],
            "quantity": random.randint(100, 5000),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "expected_delivery_date": expected.strftime("%Y-%m-%d"),
            "production_line_id": random.choice(pl_ids),
            "status": status,
        })

    write_csv("purchase_orders.csv", orders,
              ["po_id", "sku_id", "supplier_id", "quantity", "order_date",
               "expected_delivery_date", "production_line_id", "status"])
    return orders


# ============================================================
# 6. STORM ALERTS (3 rows)
# ============================================================
def generate_storm_alerts():
    now = datetime.now()
    storms = [
        {
            "alert_id": "STORM-001",
            "storm_name": "Typhoon Hailong",
            "center_lat": 18.5,
            "center_lon": 115.0,  # South China Sea
            "radius_miles": 300,
            "severity": 3,
            "start_date": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": (now + timedelta(days=3)).strftime("%Y-%m-%d"),
        },
        {
            "alert_id": "STORM-002",
            "storm_name": "Hurricane Marcus",
            "center_lat": 26.0,
            "center_lon": -90.0,  # Gulf of Mexico
            "radius_miles": 250,
            "severity": 4,
            "start_date": (now - timedelta(days=2)).strftime("%Y-%m-%d"),
            "end_date": (now + timedelta(days=2)).strftime("%Y-%m-%d"),
        },
        {
            "alert_id": "STORM-003",
            "storm_name": "Tropical Storm Nari",
            "center_lat": 14.0,
            "center_lon": 120.0,  # Philippine Sea
            "radius_miles": 200,
            "severity": 2,
            "start_date": now.strftime("%Y-%m-%d"),
            "end_date": (now + timedelta(days=4)).strftime("%Y-%m-%d"),
        },
    ]

    write_csv("storm_alerts.csv", storms,
              ["alert_id", "storm_name", "center_lat", "center_lon",
               "radius_miles", "severity", "start_date", "end_date"])
    return storms


# ============================================================
# 7. SEA FREIGHT TRACKING (200 rows, time-series)
# ============================================================
def interpolate_position(origin, dest, progress, jitter=0.5):
    """Linearly interpolate between origin and dest with some jitter."""
    lat = origin[0] + (dest[0] - origin[0]) * progress + random.uniform(-jitter, jitter)
    lon = origin[1] + (dest[1] - origin[1]) * progress + random.uniform(-jitter, jitter)
    return round(lat, 4), round(lon, 4)


def generate_sea_freight_tracking(orders, storms):
    in_transit = [o for o in orders if o["status"] in ("in_transit", "delayed")]
    if len(in_transit) < 15:
        in_transit = in_transit + random.sample(orders, 15 - len(in_transit))

    now = datetime.now()
    transport_modes = ["sea", "sea", "sea", "sea", "air", "rail"]  # mostly sea
    rows = []
    shipment_counter = 0

    # Create ~25 shipments, each with multiple tracking points
    for order in in_transit[:25]:
        shipment_counter += 1
        shipment_id = f"SHP-{shipment_counter:04d}"
        lane = random.choice(SHIPPING_LANES)
        origin_port, dest_port = lane
        origin_coords = PORTS[origin_port]
        dest_coords = PORTS[dest_port]
        vessel = random.choice(VESSEL_NAMES)
        mode = random.choice(transport_modes)

        # Generate 6-10 tracking points over last 7 days
        num_points = random.randint(6, 10)
        start_time = now - timedelta(days=7)

        for j in range(num_points):
            progress = (j + 1) / (num_points + 1)
            timestamp = start_time + timedelta(hours=j * (168 / num_points))

            # Force some shipments near storm zones
            if shipment_counter <= 3:
                # Near South China Sea storm
                storm = storms[0]
                current_lat = storm["center_lat"] + random.uniform(-2, 2)
                current_lon = storm["center_lon"] + random.uniform(-2, 2)
            elif shipment_counter <= 5:
                # Near Gulf of Mexico storm
                storm = storms[1]
                current_lat = storm["center_lat"] + random.uniform(-2, 2)
                current_lon = storm["center_lon"] + random.uniform(-2, 2)
            else:
                current_lat, current_lon = interpolate_position(
                    origin_coords, dest_coords, progress
                )

            eta = now + timedelta(days=random.randint(1, 14))
            rows.append({
                "shipment_id": shipment_id,
                "po_id": order["po_id"],
                "vessel_name": vessel,
                "transport_mode": mode,
                "origin_port": origin_port,
                "destination_port": dest_port,
                "current_lat": current_lat,
                "current_lon": current_lon,
                "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "speed_knots": round(random.uniform(5, 22), 1),
                "eta": eta.strftime("%Y-%m-%d"),
            })

    # Pad to 200 if needed
    while len(rows) < 200:
        row = random.choice(rows).copy()
        row["timestamp"] = (now - timedelta(hours=random.randint(1, 168))).strftime("%Y-%m-%dT%H:%M:%SZ")
        row["current_lat"] = round(row["current_lat"] + random.uniform(-0.5, 0.5), 4)
        row["current_lon"] = round(row["current_lon"] + random.uniform(-0.5, 0.5), 4)
        rows.append(row)

    rows = rows[:200]
    write_csv("sea_freight_tracking.csv", rows,
              ["shipment_id", "po_id", "vessel_name", "transport_mode",
               "origin_port", "destination_port", "current_lat", "current_lon",
               "timestamp", "speed_knots", "eta"])
    return rows


# ============================================================
# MAIN
# ============================================================
def main():
    print(f"Generating mock data in {OUTPUT_DIR}/\n")

    suppliers = generate_suppliers()
    parts = generate_parts_catalog(suppliers)
    generate_supplier_inventory(parts)
    production_lines = generate_production_lines()
    orders = generate_purchase_orders(parts, production_lines)
    storms = generate_storm_alerts()
    generate_sea_freight_tracking(orders, storms)

    print(f"\nDone! All files saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
