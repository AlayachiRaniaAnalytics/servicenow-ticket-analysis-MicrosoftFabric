# ServiceNow MEP & Demandes Analysis with Microsoft Fabric

## 📑 Table of Contents
- [Project Overview](#-project-overview)
- [Workflow](#-workflow)
- [Key Metrics & Insights](#-key-metrics--insights)
- [Dashboard Highlights](#-dashboard-highlights)
- [Technologies Used](#-technologies-used)
- [Results](#-results)

---

## 📌 Project Overview
This project analyzes **MEP (Mises en Production)** and **Demandes (Requests)** extracted from **ServiceNow** using its REST API.  
The data is processed and visualized with **Microsoft Fabric** and **Power BI** to provide insights into workload distribution, execution times, and collaborator performance.  

The goal is to optimize production deployments and requests handling while tracking efficiency and delays.

---

## 🚀 Workflow
1. **Data Extraction**  
   - Pulled MEP and Demandes data from **ServiceNow API**.  
   - Stored the raw datasets in Microsoft Fabric Lakehouse.  

2. **Data Transformation**  
   - Cleaned and structured the data using **Data Engineering (Spark/Notebooks)**.  
   - Normalized ticket types, execution times, and collaborator names.  

3. **Data Orchestration**  
   - Automated refresh pipelines with **Data Factory** to ensure up-to-date dashboards.  

4. **Data Visualization**  
   - Built dashboards in **Power BI** with KPIs and drill-down views for both **MEP** and **Demandes**.  

---

## 📊 Key Metrics & Insights
- **462** total tickets  
  - **296** Demandes  
  - **166** MEP  
- **10.83h** average duration for Demandes  
- **166.21h** average duration for MEP  
- Clear workload distribution by **collaborator** and **shifts**  
- Classification of top contributors  

---

## 📈 Dashboard Highlights
The Power BI dashboard provides:  
- Evolution of **MEP & Demandes over time** (monthly trend)  
- Tickets per **collaborator** and **shift**  
- Breakdown of MEP tickets by **HO (Heures Ouvrées)** vs **HNO (Heures Non Ouvrées)**  
- Average duration of tickets by type (MEP vs Demandes)  
- Ranking of collaborators by number of tickets handled and average resolution time  

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
This project delivers a **comprehensive monitoring solution** for ServiceNow MEP and Demandes, allowing organizations to:  
- Track **deployment efficiency** and **request handling times**  
- Optimize workload allocation between collaborators and shifts  
- Identify performance bottlenecks  
- Ensure better visibility on long-running or critical tasks  

---

