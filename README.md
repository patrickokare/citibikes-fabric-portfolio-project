# 🚴‍♂️ CitiBikes NYC Data Platform (Fabric Lakehouse Project)

This repository contains the full implementation of a modern **CitiBike NYC Lakehouse**, designed and built using **Microsoft Fabric**, **Delta Lake**, and **Kimball Data Modeling** principles.

> 📍 Project Title: `Citibikes-Integration`  
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

# 🚴 CitiBike NYC Fabric Lakehouse Project (`dimfact`)

This project demonstrates how to design and implement a modern **Lakehouse pipeline** using Microsoft Fabric with real-world data from the [NYC CitiBike Program](https://ride.citibikenyc.com/system-data).  
It uses Delta Lake, PySpark, and Fabric Pipelines to build a scalable ingestion + transformation workflow based on **dimensional modeling**.

---

## 📐 Features
- ✅ **Incremental ingestion** using `input_file_name()` & `MERGE`
- ✅ **Metadata enrichment** (SourceSystem, IngestionDate, HASH_ID)
- ✅ **Data Quality Checks** (trip duration, valid station IDs)
- ✅ **Star Schema modeling** for BI consumption (dim/fact)

---

