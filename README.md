# 🚴‍♂️ CitiBikes NYC Data Platform (Fabric Lakehouse Project)

This repository contains the full implementation of a modern **CitiBike NYC Lakehouse**, designed and built using **Microsoft Fabric**, **Delta Lake**, and **Kimball Data Modeling** principles.

> 📍 Project Title: `dimfact`  
> 🧱 Architecture: Medallion (Bronze → Silver → Gold)  
> 📊 Modeling: Dimensional (Star Schema)  
> ☁️ Platform: Microsoft Fabric (Lakehouse, Pipelines, Notebooks)

---

## 📁 Project Structure

```bash
├── ingestion/           # Raw data ingestion logic (bronze layer)
├── pipelines/           # Fabric Data Pipelines for orchestration
├── utils/               # Reusable helper functions & constants
├── dimfact/             # Dimensional modeling (dim/fact table logic)
└── README.md            # Project overview and documentation
