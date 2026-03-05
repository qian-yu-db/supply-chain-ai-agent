Supply Chain Visibility & Disruption Response Agent [AI] (Industry: Manufacturing)
Business Challenge
Modern manufacturing is plagued by "Blind Spot Disruption." A single delayed shipment of a $5 part can shut down a $50M production line.
Fragmented Visibility: Supply chain managers are overwhelmed by data trapped in emails, PDF manifests, and siloed ERP screens, making it impossible to see a crisis until it's too late.
The Coordination Gap: When a disruption occurs (e.g., a port strike or storm), finding an alternative supplier or rerouting freight takes days of manual coordination, leading to massive expediting costs and missed customer commitments.
How Databricks Helps
Databricks transforms supply chains from reactive to proactive by unifying the entire logistics lifecycle into a Single Source of Truth. By leveraging Generative AI as a reasoning engine, the platform can "read" external signals—like weather feeds and shipping manifests—to simulate the downstream impact of a delay on specific production lines, reducing response times by 80%. Using AI-driven pipelines, the platform bridges the gap between legacy ERP systems and real-time transit data, ensuring that every recovery plan is powered by the most up-to-date global inventory.
Build Ask
Build a "Disruption Response Agent" using mock datasets:
Simulate: Use Claude to generate mock datasets (CSV/JSON) representing multi-modal transit data (sea/air/rail), supplier inventory levels, and open purchase orders. Upload these to a Unity Catalog Volume.
Evaluate: Generate a simulated "Risk Agent" that monitors external signals (e.g., a mock "Storm Path" file) and maps them to specific purchase orders in your Volume.
Triage: Write a service to upsert "Impact Scores" and "ETA Variances" from your simulated transit stream into Lakebase for instant alerting.
Serve: Build a Databricks App ("Supply Chain Control Tower") that allows a logistics lead to "Chat with the Shipment" to generate a recovery plan (e.g., "Find the nearest alternative supplier for Part X").

Customer Requirements
Multi-Tier Visibility: The agent must be able to trace impact from the primary supplier down to simulated "Tier 2" component shortages.
Cost-Aware Mitigation: Any suggested alternative (e.g., switching to Air freight) must include a calculated "Cost-to-Serve" vs. the financial hit of a line shutdown.
High Concurrency: The system must support simultaneous SKU-level lookups via an integrated operational store.
Technical Requirements
Unity Catalog: Define the Supply_Chain_Intelligence catalog. Use Volumes to store your mock transit and inventory files and apply Tags to identify critical-path components.
Lakeflow Spark Declarative Pipelines: Build a declarative pipeline to ingest your mock transit data from the Volume into Silver Delta tables, joining it with simulated ERP records to create a live "Shipment Health" view.
Lakebase (Serverless Postgres): Use this for the operational tier. Store the "Live Shipment State" and risk levels here to enable sub-second updates to the map and disruption alerts.
Model Serving: Deploy a "Reasoning Agent" that uses Vector Search to find alternative suppliers in your mock catalog based on proximity and historical performance.
Databricks Connect: Develop the simulation and rerouting logic in Cursor, using AI-assisted code to run complex "What-If" logistics scenarios against the workspace compute.
Helpful Tips and Prompts
The "Mock Data" Prompt: "Claude, write a Python script to generate a mock dataset for a supply chain. I need sea_freight_tracking with lat/long coordinates, supplier_inventory for 20 SKUs, and purchase_orders linked to a production line."
The Lakeflow Prompt: "Claude, help me build a Lakeflow Spark Declarative Pipeline that joins my mock vessel tracking with my purchase_orders table to flag any shipment currently within 100 miles of a 'Simulated Storm Zone'."
The Architect Prompt: "Claude, design a Lakebase schema for a supply_chain_disruption table that links shipment_id, risk_level, and impacted_production_line_id."
The Cursor 'Vibe Coding' Prompt (CMD+K): "Using Databricks Connect, write a function that calculates the 'Cost-to-Serve' for an air-freight alternative compared to the $1M/day cost of a line shutdown for my simulated delay."
    
