-- Gold: Aggregate disruption impact per production line.
-- Combines shipment health + supplier risk + production line financials.

CREATE OR REFRESH MATERIALIZED VIEW gold_disruption_impact
CLUSTER BY (production_line_id)
AS
WITH impacted_shipments AS (
  SELECT
    sh.production_line_id,
    sh.sku_id,
    sh.supplier_id,
    sh.shipment_id,
    sh.po_id,
    sh.health_status,
    sh.storm_name,
    sh.storm_exposure,
    sh.distance_to_storm_miles,
    sh.expected_delivery_date,
    sh.eta
  FROM silver_shipment_health sh
  WHERE sh.health_status IN ('CRITICAL', 'WARNING')
),
supplier_risks AS (
  SELECT
    sr.sku_id,
    sr.supplier_id,
    sr.inventory_risk_level,
    sr.days_of_supply,
    sr.part_name,
    sr.unit_cost,
    sr.cost_air_freight_multiplier,
    sr.alt_supplier_id,
    sr.cascade_risk_flag
  FROM silver_supplier_risk sr
  WHERE sr.inventory_risk_level IN ('CRITICAL', 'LOW', 'BELOW_REORDER')
)
SELECT
  pl.production_line_id,
  pl.product_name,
  pl.daily_revenue,
  pl.shutdown_cost_per_day,
  COUNT(DISTINCT isp.shipment_id) AS impacted_shipments,
  COUNT(DISTINCT isp.sku_id) AS impacted_skus,
  COUNT(DISTINCT sr.sku_id) AS low_inventory_skus,
  COLLECT_SET(isp.storm_name) AS active_storms,
  COLLECT_SET(
    NAMED_STRUCT(
      'sku_id', sr.sku_id,
      'part_name', sr.part_name,
      'days_of_supply', sr.days_of_supply,
      'alt_supplier_id', sr.alt_supplier_id,
      'air_freight_cost', ROUND(sr.unit_cost * sr.cost_air_freight_multiplier, 2)
    )
  ) AS at_risk_parts,
  CASE
    WHEN COUNT(DISTINCT isp.shipment_id) > 0 AND COUNT(DISTINCT sr.sku_id) > 0 THEN 'CRITICAL'
    WHEN COUNT(DISTINCT isp.shipment_id) > 0 OR COUNT(DISTINCT sr.sku_id) > 0 THEN 'WARNING'
    ELSE 'NORMAL'
  END AS overall_risk_level,
  -- Estimated financial exposure: shutdown cost * avg days of potential delay
  ROUND(pl.shutdown_cost_per_day * COALESCE(
    GREATEST(DATEDIFF(MAX(isp.eta), MAX(isp.expected_delivery_date)), 0), 0
  ), 0) AS estimated_financial_exposure
FROM tko_mtv_goup5.supply_chain_qyu.production_lines pl
LEFT JOIN impacted_shipments isp
  ON pl.production_line_id = isp.production_line_id
LEFT JOIN supplier_risks sr
  ON isp.sku_id = sr.sku_id
  AND isp.supplier_id = sr.supplier_id
GROUP BY
  pl.production_line_id,
  pl.product_name,
  pl.daily_revenue,
  pl.shutdown_cost_per_day;
