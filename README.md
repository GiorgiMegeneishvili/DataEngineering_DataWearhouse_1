# PySpark ETL Data Lakehouse on PostgreSQL


![Pipeline](Data_LakeHouse_Diagram.png)



🚀 **End-to-End PySpark ETL pipeline** for building analytical **Data Lakehouse** on **PostgreSQL**, following modern **Data Engineering** and **Dimensional Modeling (Galaxy Schema)** principles.

This project demonstrates how to:
- Process data using **PySpark**
- Apply structured **ETL pipelines**
- Load curated data into **PostgreSQL**
- Build **Data Lakehouse** optimized for analytics
- Design **Galaxy Schema** for reporting and BI tools



## 🧠 Architecture Overview

The project follows a layered ETL&ELT approach inspired by modern data architectures:

Source Data (MSSQL)
  ↓
  ↓
PySpark ETL&ELT 
          ↓
          ↓
    PostgreSQL   
          ↓
    Bronze Layer - (Copy/Past tables
          ↓
    Silver Layer - Data Lakehouse (Galaxy Schema) 
          ↓
    Gold Layer - (Data marts )

