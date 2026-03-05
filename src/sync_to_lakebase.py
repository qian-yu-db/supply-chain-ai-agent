"""
Sync data from Delta tables (gold/silver) into Lakebase for sub-second alerting.
Upserts shipment health, disruption impact scores, and computes ETA variances.
"""

import psycopg
from databricks.sdk import WorkspaceClient

PROJECT = "supply-chain-ops-qyu"
ENDPOINT = f"projects/{PROJECT}/branches/production/endpoints/primary"

w = WorkspaceClient(profile="DEFAULT")

# --- Connect to Lakebase ---
endpoint_info = w.postgres.get_endpoint(name=ENDPOINT)
host = endpoint_info.status.hosts.host
user = w.current_user.me().user_name
cred = w.postgres.generate_database_credential(endpoint=ENDPOINT)

pg_conn_string = (
    f"host={host} "
    f"dbname=databricks_postgres "
    f"user={user} "
    f"password={cred.token} "
    f"sslmode=require"
)

# --- Read from Delta via SQL warehouse ---
warehouse_id = None  # auto-select


def query_delta(sql):
    """Query Delta tables via Databricks SQL."""
    result = w.statement_execution.execute_statement(
        warehouse_id=get_warehouse_id(),
        statement=sql,
        wait_timeout="50s",
    )
    columns = [col.name for col in result.manifest.schema.columns]
    rows = []
    if result.result and result.result.data_array:
        for row in result.result.data_array:
            rows.append(dict(zip(columns, row)))
    return rows


def get_warehouse_id():
    global warehouse_id
    if warehouse_id is None:
        warehouses = list(w.warehouses.list())
        running = [wh for wh in warehouses if str(wh.state) == "State.RUNNING"]
        if running:
            warehouse_id = running[0].id
        else:
            warehouse_id = warehouses[0].id
    return warehouse_id


def sync_shipment_health(pg_conn):
    print("Syncing shipment_health...")
    rows = query_delta("""
        SELECT shipment_id, po_id, vessel_name, transport_mode, origin_port,
               destination_port, current_lat, current_lon, speed_knots, eta,
               last_tracked_at, sku_id, supplier_id, quantity,
               expected_delivery_date, production_line_id, po_status,
               storm_name, storm_severity, distance_to_storm_miles,
               storm_exposure, health_status
        FROM tko_mtv_goup5.supply_chain_qyu.silver_shipment_health
    """)

    with pg_conn.cursor() as cur:
        for r in rows:
            cur.execute("""
                INSERT INTO supply_chain.shipment_health
                    (shipment_id, po_id, vessel_name, transport_mode, origin_port,
                     destination_port, current_lat, current_lon, speed_knots, eta,
                     last_tracked_at, sku_id, supplier_id, quantity,
                     expected_delivery_date, production_line_id, po_status,
                     storm_name, storm_severity, distance_to_storm_miles,
                     storm_exposure, health_status, updated_at)
                VALUES (%(shipment_id)s, %(po_id)s, %(vessel_name)s, %(transport_mode)s,
                        %(origin_port)s, %(destination_port)s, %(current_lat)s, %(current_lon)s,
                        %(speed_knots)s, %(eta)s, %(last_tracked_at)s, %(sku_id)s,
                        %(supplier_id)s, %(quantity)s, %(expected_delivery_date)s,
                        %(production_line_id)s, %(po_status)s, %(storm_name)s,
                        %(storm_severity)s, %(distance_to_storm_miles)s,
                        %(storm_exposure)s, %(health_status)s, NOW())
                ON CONFLICT (shipment_id) DO UPDATE SET
                    po_id = EXCLUDED.po_id,
                    current_lat = EXCLUDED.current_lat,
                    current_lon = EXCLUDED.current_lon,
                    speed_knots = EXCLUDED.speed_knots,
                    eta = EXCLUDED.eta,
                    last_tracked_at = EXCLUDED.last_tracked_at,
                    storm_name = EXCLUDED.storm_name,
                    storm_severity = EXCLUDED.storm_severity,
                    distance_to_storm_miles = EXCLUDED.distance_to_storm_miles,
                    storm_exposure = EXCLUDED.storm_exposure,
                    health_status = EXCLUDED.health_status,
                    updated_at = NOW()
            """, r)
    pg_conn.commit()
    print(f"  Upserted {len(rows)} shipment records.")


def sync_disruption_impact(pg_conn):
    print("Syncing disruption_impact...")
    rows = query_delta("""
        SELECT production_line_id, product_name, daily_revenue,
               shutdown_cost_per_day, impacted_shipments, impacted_skus,
               low_inventory_skus, overall_risk_level, estimated_financial_exposure
        FROM tko_mtv_goup5.supply_chain_qyu.gold_disruption_impact
    """)

    with pg_conn.cursor() as cur:
        for r in rows:
            cur.execute("""
                INSERT INTO supply_chain.disruption_impact
                    (production_line_id, product_name, daily_revenue,
                     shutdown_cost_per_day, impacted_shipments, impacted_skus,
                     low_inventory_skus, overall_risk_level,
                     estimated_financial_exposure, updated_at)
                VALUES (%(production_line_id)s, %(product_name)s, %(daily_revenue)s,
                        %(shutdown_cost_per_day)s, %(impacted_shipments)s, %(impacted_skus)s,
                        %(low_inventory_skus)s, %(overall_risk_level)s,
                        %(estimated_financial_exposure)s, NOW())
                ON CONFLICT (production_line_id) DO UPDATE SET
                    impacted_shipments = EXCLUDED.impacted_shipments,
                    impacted_skus = EXCLUDED.impacted_skus,
                    low_inventory_skus = EXCLUDED.low_inventory_skus,
                    overall_risk_level = EXCLUDED.overall_risk_level,
                    estimated_financial_exposure = EXCLUDED.estimated_financial_exposure,
                    updated_at = NOW()
            """, r)
    pg_conn.commit()
    print(f"  Upserted {len(rows)} production line records.")


def compute_eta_variances(pg_conn):
    print("Computing ETA variances...")
    rows = query_delta("""
        SELECT shipment_id, po_id, production_line_id, sku_id,
               expected_delivery_date, eta AS current_eta,
               DATEDIFF(eta, expected_delivery_date) AS variance_days,
               health_status
        FROM tko_mtv_goup5.supply_chain_qyu.silver_shipment_health
        WHERE eta IS NOT NULL AND expected_delivery_date IS NOT NULL
    """)

    with pg_conn.cursor() as cur:
        for r in rows:
            variance = int(r["variance_days"]) if r["variance_days"] else 0
            # Impact score: higher variance + critical status = higher score
            base_score = max(0, variance) * 10
            if r["health_status"] == "CRITICAL":
                impact_score = min(100, base_score + 50)
                alert_level = "RED"
            elif r["health_status"] == "WARNING":
                impact_score = min(100, base_score + 25)
                alert_level = "AMBER" if variance > 3 else "YELLOW"
            else:
                impact_score = min(100, base_score)
                alert_level = "GREEN"

            cur.execute("""
                INSERT INTO supply_chain.eta_variance
                    (shipment_id, po_id, production_line_id, sku_id,
                     expected_delivery_date, current_eta, variance_days,
                     impact_score, alert_level, updated_at)
                VALUES (%(shipment_id)s, %(po_id)s, %(production_line_id)s, %(sku_id)s,
                        %(expected_delivery_date)s, %(current_eta)s, %(variance_days)s,
                        %(impact_score)s, %(alert_level)s, NOW())
                ON CONFLICT (shipment_id) DO UPDATE SET
                    current_eta = EXCLUDED.current_eta,
                    variance_days = EXCLUDED.variance_days,
                    impact_score = EXCLUDED.impact_score,
                    alert_level = EXCLUDED.alert_level,
                    updated_at = NOW()
            """, {**r, "variance_days": variance, "impact_score": impact_score, "alert_level": alert_level})
    pg_conn.commit()
    print(f"  Upserted {len(rows)} ETA variance records.")


def main():
    print("Connecting to Lakebase...")
    with psycopg.connect(pg_conn_string) as conn:
        sync_shipment_health(conn)
        sync_disruption_impact(conn)
        compute_eta_variances(conn)

        # Summary
        with conn.cursor() as cur:
            cur.execute("SELECT health_status, COUNT(*) FROM supply_chain.shipment_health GROUP BY health_status ORDER BY health_status")
            print("\nShipment Health Summary:")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

            cur.execute("SELECT alert_level, COUNT(*) FROM supply_chain.eta_variance GROUP BY alert_level ORDER BY alert_level")
            print("\nETA Variance Alerts:")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

            cur.execute("SELECT overall_risk_level, COUNT(*) FROM supply_chain.disruption_impact GROUP BY overall_risk_level ORDER BY overall_risk_level")
            print("\nProduction Line Risk:")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

    print("\nSync complete!")


if __name__ == "__main__":
    main()
