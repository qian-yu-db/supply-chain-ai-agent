"""
Create Lakebase tables for the Supply Chain Disruption Response Agent.
Stores live shipment state, impact scores, and ETA variances for sub-second alerting.
"""

import psycopg
from databricks.sdk import WorkspaceClient

PROJECT = "supply-chain-ops-qyu"
BRANCH = "production"
ENDPOINT = f"projects/{PROJECT}/branches/{BRANCH}/endpoints/primary"

w = WorkspaceClient(profile="DEFAULT")

# Get connection details
endpoint_info = w.postgres.get_endpoint(name=ENDPOINT)
host = endpoint_info.status.hosts.host
user = w.current_user.me().user_name
cred = w.postgres.generate_database_credential(endpoint=ENDPOINT)

conn_string = (
    f"host={host} "
    f"dbname=databricks_postgres "
    f"user={user} "
    f"password={cred.token} "
    f"sslmode=require"
)

DDL = """
-- Schema for supply chain operational data
CREATE SCHEMA IF NOT EXISTS supply_chain;

-- Live shipment state with health status (synced from Delta)
CREATE TABLE IF NOT EXISTS supply_chain.shipment_health (
    shipment_id TEXT PRIMARY KEY,
    po_id TEXT,
    vessel_name TEXT,
    transport_mode TEXT,
    origin_port TEXT,
    destination_port TEXT,
    current_lat DOUBLE PRECISION,
    current_lon DOUBLE PRECISION,
    speed_knots DOUBLE PRECISION,
    eta DATE,
    last_tracked_at TIMESTAMP,
    sku_id TEXT,
    supplier_id TEXT,
    quantity INTEGER,
    expected_delivery_date DATE,
    production_line_id TEXT,
    po_status TEXT,
    storm_name TEXT,
    storm_severity INTEGER,
    distance_to_storm_miles DOUBLE PRECISION,
    storm_exposure TEXT,
    health_status TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Disruption impact scores per production line (synced from Delta)
CREATE TABLE IF NOT EXISTS supply_chain.disruption_impact (
    production_line_id TEXT PRIMARY KEY,
    product_name TEXT,
    daily_revenue BIGINT,
    shutdown_cost_per_day BIGINT,
    impacted_shipments INTEGER,
    impacted_skus INTEGER,
    low_inventory_skus INTEGER,
    overall_risk_level TEXT,
    estimated_financial_exposure BIGINT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ETA variances for alerting (computed during sync)
CREATE TABLE IF NOT EXISTS supply_chain.eta_variance (
    shipment_id TEXT PRIMARY KEY,
    po_id TEXT,
    production_line_id TEXT,
    sku_id TEXT,
    expected_delivery_date DATE,
    current_eta DATE,
    variance_days INTEGER,
    impact_score DOUBLE PRECISION,
    alert_level TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_shipment_health_status ON supply_chain.shipment_health(health_status);
CREATE INDEX IF NOT EXISTS idx_shipment_production_line ON supply_chain.shipment_health(production_line_id);
CREATE INDEX IF NOT EXISTS idx_disruption_risk ON supply_chain.disruption_impact(overall_risk_level);
CREATE INDEX IF NOT EXISTS idx_eta_alert ON supply_chain.eta_variance(alert_level);
CREATE INDEX IF NOT EXISTS idx_eta_production_line ON supply_chain.eta_variance(production_line_id);
"""

with psycopg.connect(conn_string) as conn:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("Tables created successfully.")

    # Verify
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'supply_chain'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"\nTables in supply_chain schema:")
        for t in tables:
            print(f"  - {t[0]}")
