// mongo_kpis.js - Generates all KPI tables for PowerBI

print("Starting MongoDB Aggregations...");

// 1. Global KPIs
db.master_table.aggregate([
  { $group: { _id: null, total_gmv: { $sum: "$price" }, total_orders: { $addToSet: "$order_id" }, avg_delivery_time: { $avg: { $divide: [ { $subtract: ["$delivered_date", "$purchase_date"] }, 86400000 ] } }, avg_review_score: { $avg: "$review_score" }, late_deliveries: { $sum: "$is_late" } } },
  { $project: { total_gmv: 1, total_order_volume: { $size: "$total_orders" }, aov: { $divide: ["$total_gmv", { $size: "$total_orders" }] }, avg_delivery_time: 1, avg_review_score: 1, late_delivery_rate: { $multiply: [ { $divide: ["$late_deliveries", { $size: "$total_orders" }] }, 100 ] } } },
  { $out: "kpi_global_summary" }
]);
print("Created: kpi_global_summary");

// 2. Q1 - Operations: Geography, Size, Freight vs. Delays
db.master_table.aggregate([
  { $match: { customer_state: { $ne: null } } },
  { $group: { _id: "$customer_state", avg_freight: { $avg: "$freight_value" }, avg_volume_cm3: { $avg: "$product_volume_cm3" }, avg_delay_days: { $avg: "$delivery_delay_days" }, late_order_count: { $sum: "$is_late" } } },
  { $sort: { avg_delay_days: -1 } },
  { $out: "kpi_ops_delays" }
]);
print("Created: kpi_ops_delays");

// 3. Q2 - Marketing: Customer Value & Reviews by Location
db.master_table.aggregate([
  { $group: { _id: "$customer_state", total_customer_spend: { $sum: { $add: ["$price", "$freight_value"] } }, avg_review_score: { $avg: "$review_score" }, unique_customers: { $addToSet: "$customer_id" } } },
  { $project: { total_customer_spend: 1, avg_review_score: 1, customer_count: { $size: "$unique_customers" }, clv_by_state: { $divide: ["$total_customer_spend", { $size: "$unique_customers" }] } } },
  { $sort: { total_customer_spend: -1 } },
  { $out: "kpi_mkt_geo_value" }
]);
print("Created: kpi_mkt_geo_value");

// 4. Q3 - Sales: Category Revenue vs. Ratings
db.master_table.aggregate([
  { $match: { product_category_name: { $ne: null } } },
  { $group: { _id: "$product_category_name", total_revenue: { $sum: "$price" }, avg_review: { $avg: "$review_score" }, items_sold: { $sum: 1 } } },
  { $sort: { total_revenue: -1 } },
  { $limit: 15 },
  { $out: "kpi_sales_categories" }
]);
print("Created: kpi_sales_categories");

// 5. Trend Analysis (Revenue over Time)
db.master_table.aggregate([
  { $match: { purchase_year: { $ne: null } } },
  { $group: { _id: { year: "$purchase_year", month: "$purchase_month" }, monthly_revenue: { $sum: "$price" }, order_count: { $addToSet: "$order_id" } } },
  { $project: { year: "$_id.year", month: "$_id.month", monthly_revenue: 1, total_orders: { $size: "$order_count" } } },
  { $sort: { "year": 1, "month": 1 } },
  { $out: "kpi_trend_analysis" }
]);
print("Created: kpi_trend_analysis");

print("All MongoDB Aggregations Completed Successfully!");