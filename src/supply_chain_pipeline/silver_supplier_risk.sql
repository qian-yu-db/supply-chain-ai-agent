-- Silver: Identify at-risk suppliers with low inventory on critical-path parts.
-- Traces Tier 2 → Tier 1 cascade risk.

CREATE OR REFRESH MATERIALIZED VIEW silver_supplier_risk
CLUSTER BY (supplier_id, sku_id)
AS
SELECT
  s.supplier_id,
  s.supplier_name,
  s.tier,
  s.country,
  s.reliability_score,
  s.parent_supplier_id,
  pc.sku_id,
  pc.part_name,
  pc.category,
  pc.unit_cost,
  pc.is_critical_path,
  pc.cost_air_freight_multiplier,
  pc.alt_supplier_id,
  si.quantity_on_hand,
  si.reorder_point,
  si.days_of_supply,
  CASE
    WHEN pc.is_critical_path = true AND si.days_of_supply < 5 THEN 'CRITICAL'
    WHEN si.days_of_supply < 10 THEN 'LOW'
    WHEN si.quantity_on_hand < si.reorder_point THEN 'BELOW_REORDER'
    ELSE 'ADEQUATE'
  END AS inventory_risk_level,
  -- Tier-2 cascade flag: if this is a Tier 2 supplier with low stock,
  -- the parent Tier 1 supplier is also at risk
  CASE
    WHEN s.tier = 2 AND si.days_of_supply < 5 THEN true
    ELSE false
  END AS cascade_risk_flag
FROM tko_mtv_goup5.supply_chain_qyu.suppliers s
INNER JOIN tko_mtv_goup5.supply_chain_qyu.supplier_inventory si
  ON s.supplier_id = si.supplier_id
INNER JOIN tko_mtv_goup5.supply_chain_qyu.parts_catalog pc
  ON si.sku_id = pc.sku_id;
