# 📚 Data Catalog: Olist Data Warehouse (Gold Layer)

## 📌 Overview
The **Gold Layer** serves as the primary interface for Business Intelligence (BI) and Analytics. It follows a **Star Schema** architecture tailored for high-performance reporting on the Olist E-Commerce dataset.

This layer consists of **Dimension Tables** (Context) and **Fact Tables** (Transactions & Metrics).

---

## 🔵 Dimension Tables

### 1. **Gold.dim_customers**
- **Purpose:** Contains enriched customer profiles, including their unique identifiers and geographical locations.
- **Source:** `Silver.olist_customers` + `Silver.olist_geolocation`
- **Columns:**

| Column Name        | Data Type      | Description |
|--------------------|----------------|-------------|
| **customer_sk** | INT (PK)       | Surrogate Key. Unique internal identifier for the customer dimension. |
| customer_id        | VARCHAR(32)    | The original key from the order system (Note: one customer can have multiple IDs per order). |
| customer_unique_id | VARCHAR(32)    | The unique identifier for a real person. Use this to count unique customers. |
| city               | NVARCHAR(100)  | The city where the customer resides. |
| state              | VARCHAR(5)     | The state code (UF) of the customer (e.g., SP, RJ). |
| latitude           | FLOAT          | Geographical latitude coordinate for map visualizations. |
| longitude          | FLOAT          | Geographical longitude coordinate for map visualizations. |

---

### 2. **Gold.dim_products**
- **Purpose:** Stores detailed product attributes and their English category translations.
- **Source:** `Silver.olist_products` + `Silver.product_category_name_translation`
- **Columns:**

| Column Name       | Data Type      | Description |
|-------------------|----------------|-------------|
| **product_sk** | INT (PK)       | Surrogate Key. Unique internal identifier for the product dimension. |
| product_id        | VARCHAR(32)    | The original unique identifier of the product. |
| category          | NVARCHAR(100)  | The product category name translated to English (e.g., 'Health & Beauty'). |
| photos_qty        | INT            | Number of photos published for the product. |
| weight_g          | INT            | Product weight in grams. |
| length_cm         | INT            | Product length in centimeters. |
| height_cm         | INT            | Product height in centimeters. |
| width_cm          | INT            | Product width in centimeters. |

---

### 3. **Gold.dim_sellers**
- **Purpose:** Provides information about the sellers (partners) and their locations.
- **Source:** `Silver.olist_sellers` + `Silver.olist_geolocation`
- **Columns:**

| Column Name       | Data Type      | Description |
|-------------------|----------------|-------------|
| **seller_sk** | INT (PK)       | Surrogate Key. Unique internal identifier for the seller dimension. |
| seller_id         | VARCHAR(32)    | The original unique identifier of the seller. |
| city              | NVARCHAR(100)  | The city where the seller is based. |
| state             | VARCHAR(5)     | The state code (UF) of the seller. |
| latitude          | FLOAT          | Geographical latitude coordinate of the seller. |
| longitude         | FLOAT          | Geographical longitude coordinate of the seller. |

---

## 🔴 Fact Tables

### 4. **Gold.fact_sales**
- **Purpose:** The central table for sales analysis. It aggregates order details, items, and payment information at the line-item level.
- **Source:** `Silver.olist_orders` + `Silver.olist_order_items` + `Silver.olist_order_payments`
- **Columns:**

| Column Name              | Data Type      | Description |
|--------------------------|----------------|-------------|
| **order_id** | VARCHAR(32)    | Degenerate Dimension. The unique identifier of the order. |
| customer_sk              | INT (FK)       | Foreign Key linking to `dim_customers`. |
| product_sk               | INT (FK)       | Foreign Key linking to `dim_products`. |
| seller_sk                | INT (FK)       | Foreign Key linking to `dim_sellers`. |
| status                   | VARCHAR(20)    | Current status of the order (e.g., delivered, shipped, canceled). |
| purchase_date            | DATE           | The date the purchase was made. |
| approved_date            | DATE           | The date payment was approved. |
| delivered_customer_date  | DATE           | The actual date the customer received the order. |
| estimated_delivery_date  | DATE           | The estimated delivery date provided at purchase. |
| payment_type             | VARCHAR(20)    | The primary method of payment (e.g., credit_card, boleto). |
| installments             | INT            | Number of installments chosen for payment. |
| item_price               | DECIMAL(10,2)  | The price of a single item unit. |
| shipping_cost            | DECIMAL(10,2)  | The freight/shipping cost allocated to this item. |
| **total_item_value** | DECIMAL(10,2)  | Calculated Measure: `item_price` + `shipping_cost`. |

---

### 5. **Gold.fact_order_reviews**
- **Purpose:** Contains customer feedback and ratings for orders. Used for sentiment and quality analysis.
- **Source:** `Silver.olist_order_reviews`
- **Columns:**

| Column Name    | Data Type       | Description |
|----------------|-----------------|-------------|
| review_id      | VARCHAR(32)     | Unique identifier of the review. |
| order_id       | VARCHAR(32)     | Foreign Key linking to the Sales Fact (via Order ID). |
| review_score   | INT             | Customer rating from 1 (Lowest) to 5 (Highest). |
| review_title   | NVARCHAR(255)   | The title/header of the review comment (if provided). |
| review_message | NVARCHAR(MAX)   | The detailed text feedback from the customer. |
| creation_date  | DATE            | The date the review survey was sent to the customer. |
| answer_date    | DATE            | The date the customer submitted the review answer. |

---
*Generated for Olist Data Warehouse Project.*
