# Household Affordability Index – Data Pipeline

A Python-based data pipeline that scrapes monthly **Household Food Basket** PDF
reports published by the *Pietermaritzburg Economic Justice & Dignity Group (PMBEJD)*,
extracts regional food price comparison tables, and loads the data into a
Pandas DataFrame (with optional persistence to Postgres).

---

## Overview

Each month, PMBEJD publishes a PDF report containing multiple tables.
This pipeline automatically:

- Locates the most recent report
- Downloads the PDF
- Identifies the **“Household Food Basket: Per area, compared”** table  

---

## Running the project in Google Colab

This project can be executed directly in **Google Colab**, with no local setup
required.

<details>
<summary><strong>Step-by-step instructions</strong></summary>

<br>

**1. Clone the repository**
!git clone https://github.com/MayGreen94/DataPipelin_HouseHoldAffordabilityIndex.git

**2. Navigate into the repository**
%cd DataPipelin_HouseHoldAffordabilityIndex

**3. Install dependencies**
!pip install -r requirements.txt

**4. Run the scraper**
!python src/scraper.py
