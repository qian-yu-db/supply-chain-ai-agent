-- Silver: Join sea freight tracking with purchase orders and storm alerts
-- to create a live "Shipment Health" view that flags at-risk shipments.

CREATE OR REFRESH MATERIALIZED VIEW silver_shipment_health
CLUSTER BY (shipment_id, po_id)
AS
WITH latest_positions AS (
  SELECT
    sft.*,
    ROW_NUMBER() OVER (PARTITION BY sft.shipment_id ORDER BY sft.timestamp DESC) AS rn
  FROM tko_mtv_goup5.supply_chain_qyu.sea_freight_tracking sft
),
current_positions AS (
  SELECT * FROM latest_positions WHERE rn = 1
),
storm_distances AS (
  SELECT
    cp.shipment_id,
    sa.alert_id,
    sa.storm_name,
    sa.severity AS storm_severity,
    sa.radius_miles,
    ROUND(
      3959 * ACOS(
        LEAST(1.0, GREATEST(-1.0,
          COS(RADIANS(cp.current_lat)) * COS(RADIANS(sa.center_lat)) *
          COS(RADIANS(sa.center_lon) - RADIANS(cp.current_lon)) +
          SIN(RADIANS(cp.current_lat)) * SIN(RADIANS(sa.center_lat))
        ))
      ), 1
    ) AS distance_to_storm_miles,
    ROW_NUMBER() OVER (
      PARTITION BY cp.shipment_id
      ORDER BY
        3959 * ACOS(
          LEAST(1.0, GREATEST(-1.0,
            COS(RADIANS(cp.current_lat)) * COS(RADIANS(sa.center_lat)) *
            COS(RADIANS(sa.center_lon) - RADIANS(cp.current_lon)) +
            SIN(RADIANS(cp.current_lat)) * SIN(RADIANS(sa.center_lat))
          ))
        )
    ) AS storm_rank
  FROM current_positions cp
  CROSS JOIN tko_mtv_goup5.supply_chain_qyu.storm_alerts sa
),
nearest_storm AS (
  SELECT * FROM storm_distances WHERE storm_rank = 1
)
SELECT
  cp.shipment_id,
  cp.po_id,
  cp.vessel_name,
  cp.transport_mode,
  cp.origin_port,
  cp.destination_port,
  cp.current_lat,
  cp.current_lon,
  cp.speed_knots,
  cp.eta,
  cp.timestamp AS last_tracked_at,
  po.sku_id,
  po.supplier_id,
  po.quantity,
  po.expected_delivery_date,
  po.production_line_id,
  po.status AS po_status,
  ns.storm_name,
  ns.storm_severity,
  ns.distance_to_storm_miles,
  CASE
    WHEN ns.distance_to_storm_miles <= ns.radius_miles THEN 'IN_STORM_ZONE'
    WHEN ns.distance_to_storm_miles <= ns.radius_miles * 1.5 THEN 'APPROACHING_STORM'
    ELSE 'CLEAR'
  END AS storm_exposure,
  CASE
    WHEN ns.distance_to_storm_miles <= ns.radius_miles THEN 'CRITICAL'
    WHEN ns.distance_to_storm_miles <= ns.radius_miles * 1.5 THEN 'WARNING'
    WHEN po.status = 'delayed' THEN 'WARNING'
    ELSE 'NORMAL'
  END AS health_status
FROM current_positions cp
LEFT JOIN tko_mtv_goup5.supply_chain_qyu.purchase_orders po
  ON cp.po_id = po.po_id
LEFT JOIN nearest_storm ns
  ON cp.shipment_id = ns.shipment_id
WHERE cp.rn = 1;
