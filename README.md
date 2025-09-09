# ServiceNow Ticket Analysis (Batch & Real-time) with Microsoft Fabric  
## Table of Contents  
- [Project Overview](#project-overview)  
- [Data Sources](#data-sources)  
- [Tools](#tools)  
- [Architecture & Steps](#architecture--steps)  
- [Results/Findings](#resultsfindings)  
- [Recommendations](#recommendations)  
- [Limitations](#limitations)  
- [References](#references)  
---
### Project Overview  
This project focuses on analyzing IT Service Management (ITSM) tickets from **ServiceNow**, using both **batch** and **real-time** data processing in **Microsoft Fabric**.  
The goal is to provide timely and actionable insights to improve incident resolution, resource allocation, and service efficiency.  
---
### Data Sources  
- **ServiceNow REST API** – Used to gather ticket data directly from the ServiceNow platform.  
- **Azure Event Hubs** – For real-time event streaming of ServiceNow data.  
---
### Tools 
- **ServiceNow REST API** – Data extraction and ingestion from ServiceNow platform.
- **Microsoft Fabric Lakehouse** – Storage & Medallion architecture (Bronze, Silver, Gold).  
- **Microsoft Fabric Notebooks (PySpark)** – Data ingestion, cleaning, and transformation.  
- **SQL Analytics Endpoint** – Querying and modeling data.  
- **Power BI** – Dashboarding and visualization.  
- **Data Activator** – Trigger-based alerts and actions.  
- **Microsoft Purview** – Governance and data catalog.  
---
### Architecture & Steps  
1. **Data Ingestion**  
   - Batch ingestion from ServiceNow REST API into Fabric Lakehouse (Bronze).  
   - Real-time ingestion via Azure Event Hubs.  
2. **Data Transformation**  
   - Cleaned and standardized tickets in **Silver** layer using PySpark.  
   - Created fact and dimension tables in **Gold** layer (semantic model, star schema).  
3. **Data Analysis & Visualization**  
   - Built automated dashboards in **Power BI**.  
   - Key KPIs: average resolution time, open vs. closed tickets, SLA compliance.  
4. **Automation & Governance**  
   - Implemented lineage & governance with **Microsoft Purview**.  
   - Configured **Data Activator** for real-time triggers (e.g., SLA breach alerts).  
#### Medallion Architecture Diagram  
```mermaid
flowchart LR
    A[ServiceNow REST API] -->|Batch Ingestion| B[Bronze Layer: Raw Data in Fabric Lakehouse]
    A2[Azure Event Hubs] -->|Real-time Streaming| B
    B --> C[Silver Layer: Cleaned and Standardized Data via PySpark Notebooks]
    C --> D[Gold Layer: Aggregated Data - Fact and Dimension Tables - Star Schema]
    D --> E[Power BI Dashboards and Reports]
    D --> F[Data Activator: Real-time Alerts]
    D --> G[Purview: Governance and Lineage]
```
---
### Results/Findings
* Enabled **real-time monitoring** of ServiceNow incidents.
* Reduced reporting latency from hours to near real-time.
* Provided clear visibility into SLA performance and ticket workload distribution.
---
### Recommendations
* Scale the pipeline to include multiple ServiceNow modules (e.g., change requests, problem management).
* Enhance real-time dashboards with predictive analytics (e.g., ticket resolution forecasting).
* Integrate proactive notifications for high-priority incidents.
---
### Limitations
* Requires stable API access and ServiceNow credentials.
* Real-time streaming depends on Azure Event Hub throughput configuration.
* Some historical data gaps may exist depending on API retention policies.
---
### References
1. Microsoft Fabric Documentation
2. ServiceNow REST API Documentation
