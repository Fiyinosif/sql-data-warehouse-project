# 📘 Data Catalog — Gold Layer

## Overview

The **Gold layer** represents the business-ready data model used for analytics and reporting.  
It follows a **star schema design**, consisting of:

- ⭐ Dimension tables — descriptive attributes  
- 📊 Fact tables — measurable events  

This layer is built from the **Silver layer** and applies business logic, surrogate keys, and final transformations.

Tables in the Gold layer:

- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

---

## 👤 gold.dim_customers

**Purpose**  
Stores customer information enriched with demographic and geographic data.

**Source tables**

- `silver.crm_cust_info`
- `silver.erp_cust_az12`
- `silver.erp_loc_a101`

### Columns

| Column Name | Data Type | Description |
|-----------|----------|------------|
| customer_key | INT | Surrogate key generated using ROW_NUMBER |
| customer_id | INT | Customer ID from CRM |
| customer_number | NVARCHAR | Business customer number |
| first_name | NVARCHAR | Customer first name |
| last_name | NVARCHAR | Customer last name |
| country | NVARCHAR | Customer country |
| marital_status | NVARCHAR | Marital status |
| gender | NVARCHAR | Gender from CRM, fallback to ERP if unknown |
| birthdate | DATE | Customer birthdate |
| create_date | DATE | Record creation date |

### Business Rules

- Surrogate key generated using `ROW_NUMBER()`
- CRM gender is treated as the master value
- If CRM gender = 'Unknown', ERP gender is used
- LEFT JOIN may produce NULL when no ERP record exists

---

## 📦 gold.dim_products

**Purpose**  
Stores product details used for reporting and sales analysis.

**Source tables**

- `silver.crm_prd_info`
- `silver.erp_px_cat_g1v2`

### Columns

| Column Name | Data Type | Description |
|-----------|----------|------------|
| product_key | INT | Surrogate key generated using ROW_NUMBER |
| product_id | INT | Product ID from CRM |
| product_number | NVARCHAR | Business product code |
| product_name | NVARCHAR | Product name |
| category_id | NVARCHAR | Category ID |
| category | NVARCHAR | Category name |
| subcategory | NVARCHAR | Product subcategory |
| maintenance | NVARCHAR | Maintenance classification |
| cost | INT | Product cost |
| product_line | NVARCHAR | Product line |
| start_date | DATE | Product start date |

### Business Rules

- Only current products included
- Historical products removed using `prd_end_dt IS NULL`
- Surrogate key generated using `ROW_NUMBER()`

---

## 💰 gold.fact_sales

**Purpose**  
Stores sales transactions for analytics and reporting.

**Source tables**

- `silver.crm_sales_details`
- `gold.dim_customers`
- `gold.dim_products`

### Columns

| Column Name | Data Type | Description |
|-----------|----------|------------|
| order_number | NVARCHAR | Sales order number |
| product_key | INT | Foreign key → dim_products |
| customer_key | INT | Foreign key → dim_customers |
| order_date | DATE | Order date |
| ship_date | DATE | Ship date |
| due_date | DATE | Due date |
| sales_amount | DECIMAL | Sales amount |
| quantity | INT | Quantity sold |
| price | INT | Unit price |

### Business Rules

- One row per order line
- Linked to dimension tables using surrogate keys
- Built from cleaned Silver layer data

---

## ⭐ Schema Design

Star schema structure

```
          dim_customers
                │
                │
           fact_sales
                │
                │
           dim_products
```

Relationships

- dim_customers.customer_key → fact_sales.customer_key  
- dim_products.product_key → fact_sales.product_key  

---

## Notes

- Gold layer is optimized for analytics
- Uses surrogate keys
- Built from Silver layer
- Contains only cleaned, business-ready data
