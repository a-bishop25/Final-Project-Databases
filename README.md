# Final-Project-Databases

Municipal Bond Market Database & Analysis

Author: Mark Crisci, Muhammed Altindal, and Avery Bishop
Course: Databases (Final Project)
Repository: https://github.com/mcrisci24/Final-Project-Databases
Dashboard Link:
Network URL: http://192.168.1.124:8505

1. Project Overview

This project implements a robust relational database system for analyzing the US Municipal Bond Market. It integrates demographic data, financial trade records, credit ratings, and macroeconomic indicators into a unified PostgreSQL warehouse.

The goal is to enable complex financial analysis—such as calculating yield spreads, assessing credit risk, and identifying liquidity trends—that would be impossible using disjointed spreadsheets.

Key Features

Relational Schema: A normalized database structure (Star Schema inspired) with a central Fact table (bonds) and supporting Dimensions (issuers, bond_purposes).

Python ETL Pipeline: A custom load.py script that cleanses and loads raw CSV data using pandas and SQLAlchemy.

Financial Analysis: SQL queries that calculate Tax-Equivalent Yields (TEY), trade volumes, and risk premiums vs. US Treasury rates.

2. Database Architecture

The database municipal_bonds is designed to answer questions about risk and return in the public sector.

Schema Diagram

(You can insert an image of your ERD here)

Table Descriptions

issuers (Dimension)
Demographic info on the borrowing entity (State, Population, Tax Base).

bonds (Fact)
Core instrument data (CUSIP, Coupon Rate, Maturity Date, Face Value).

bond_purposes (Dimension)
Categorization of funding use (e.g., 'Education', 'Public Safety').

trades (Transaction)
Time-series data of individual bond transactions (Price, Yield, Quantity).

credit_ratings (Dimension)
History of credit ratings and outlooks (Moody's, S&P) for each bond.

macro_economic_data (External)
State-level economic health indicators (Unemployment, 10Y Treasury Rates).

3. Repository Contents

Core System Files

municipal_schema.sql: The DDL script. Creates tables, primary keys, foreign keys, indexes, and constraints.

load.py: The ETL script. Connects to the database and bulk-loads the CSV files in the correct dependency order to avoid constraint violations.

.gitignore: Ensures system files (like .idea/ or __pycache__) are not committed to the repo.

Data Files (Raw Source)

issuers.csv

bonds.csv

bond_purposes.csv

trades.csv

credit_ratings.csv

economic_indicators.csv

Assignment Deliverables (Week 11)

queries_week11.sql: The final set of 5 advanced SQL queries (Joins, Subqueries, Financial Metrics) required for the project submission.

analysis_week11.md / .pdf: The written report explaining the business insights derived from the SQL analysis.

municipal_bonds_analysis.sql: A collection of exploratory and troubleshooting queries used during development.

4. Setup & Installation

Follow these steps to replicate the environment locally.

Prerequisites

PostgreSQL (v13 or higher)

Python (v3.8 or higher)

PgAdmin 4 (Optional, for GUI management)

Step 1: Database Creation

Open your terminal or SQL tool and create the database:

CREATE DATABASE municipal_bonds;


Step 2: Create Schema

Run the schema script to build the tables.

psql -U postgres -d municipal_bonds -f municipal_schema.sql


(Or open municipal_schema.sql in PgAdmin and execute it.)

Step 3: Install Python Dependencies

Ensure you have the required libraries:

pip install pandas sqlalchemy psycopg2-binary


Step 4: Load Data

Run the Python loader. Note: Ensure DB_PORT in load.py matches your local Postgres port (default is 5432, project uses 5433).

python load.py


You should see success messages for connecting and loading each table.

5. How to Run the Analysis

Open your SQL client (e.g., PgAdmin, DBeaver, or VS Code).

Connect to the municipal_bonds database.

Open queries_week11.sql.

Highlight and execute individual queries to see the results.

Example Analysis Included:

Yield Spread Calculation: Compares municipal yields against the "risk-free" 10-Year Treasury rate.

Liquidity Analysis: Identifies the most actively traded long-duration bonds.


Credit Risk Trends: Analyzing rating outlook changes (Positive vs. Negative) over time.

Professor Evaluation:

For the final project, Avery worked as part of a three-person team (with Mark Crisci and Muhammed Altindal) to deliver an exceptional municipal bonds database analysis that demonstrated professional-level database engineering skills. The team implemented a star schema architecture with proper 3NF normalization across 6 main tables plus audit logs, showing mastery of data warehousing principles.


The database design was exemplary, featuring advanced constraints including regex validation for state codes, business rule enforcement for date ordering, and range validation for rates. Most impressively, the team implemented PL/pgSQL triggers for audit logging, demonstrating understanding of database features. 
