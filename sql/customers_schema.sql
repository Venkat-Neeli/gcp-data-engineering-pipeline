-- BigQuery target table schema
-- Retail-style customer dataset

CREATE TABLE IF NOT EXISTS `project_id.analytics.customers`
(
    customer_id INT64,
    customer_name STRING,
    email STRING,
    city STRING,
    updated_at DATE
);
