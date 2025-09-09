# ServiceNow Ticket Analysis with Microsoft Fabric

## 📑 Table of Contents
- [Project Overview](#-project-overview)
- [Workflow](#-workflow)
- [Key Metrics & Insights](#-key-metrics--insights)
- [Dashboard Highlights](#-dashboard-highlights)
- [Technologies Used](#-technologies-used)

- [Results](#-results)

---

## 📌 Project Overview
This project demonstrates how to extract, process, and analyze **ServiceNow incidents data** using the **ServiceNow REST API** and **Microsoft Fabric**.  
The goal is to provide clear insights into incident management through dashboards and KPIs such as ticket evolution, escalation rate, priorities, and resolution times.

---

## 🚀 Workflow
1. **Data Extraction**  
   - Connected to **ServiceNow API** to extract incident data.  
   - Stored raw data in Microsoft Fabric Lakehouse.  

2. **Data Transformation**  
   - Used **Data Engineering** in Fabric (Spark / Notebooks) to clean and transform data.  
   - Standardized ticket states, priorities, and timestamps.  

3. **Data Orchestration**  
   - Built pipelines in **Data Factory** to automate data refresh from ServiceNow API.  

4. **Data Visualization**  
   - Created interactive dashboards in **Power BI** to monitor incident lifecycle and support teams’ performance.  

---

## 📊 Key Metrics & Insights
- **18360** tickets reported  
- **5** tickets resolved  
- **18142** tickets closed  
- **213** tickets still in progress  
- **3435** escalated tickets  
- KPIs such as **MTTR (373.91h)** and **workload per support team**  
- Breakdown of tickets by **priority, team, and assignee**  

---

## 📈 Dashboard Highlights
The Power BI dashboard includes:  
- Ticket trends over time (opened, closed, in progress)  
- Escalated vs. treated tickets  
- Average resolution time by month and year  
- Tickets by **priority level** (Critical, High, Moderate, Low, Planned)  
- Workload distribution by **team and collaborator**  

---

## 🛠️ Technologies Used
- **Microsoft Fabric**  
  - Data Factory (pipelines & orchestration)  
  - Data Engineering (transformation & cleaning)  
  - Data Warehouse / Lakehouse (storage)  
  - Power BI (visualization)  
- **ServiceNow REST API** (data extraction)  
- **Python / Spark** (data preparation)  

---


## ✅ Results
This project provides a **complete end-to-end pipeline** to analyze IT incidents, helping teams to:  
- Monitor workload and priorities  
- Improve resolution time  
- Optimize resource allocation  
- Ensure better visibility on escalated tickets

