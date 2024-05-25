import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(
    page_title="Brazilian Ecommerce Dashboard",
    page_icon="🇧🇷",
    layout="centered",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
# Olist Brazilian Ecommerce Dataset Dashboard

## Overview

Welcome to the **Olist Brazilian Ecommerce Dataset Dashboard**. This dashboard provides insights into the sales 
performance and geographical distribution of sellers and customers. It is designed to help you understand key metrics 
and trends that can drive strategic decision-making.

## Key Features

- **Sales Performance**: Track and analyze sales data over different time periods.
- **Geolocation Analysis**: Visualize the distribution of sellers and customers on a geographical map.
- **Year-over-Year Comparison**: Compare sales and customer growth between different years.
- **Interactive Filters**: Customize your view with dynamic filters to focus on specific regions, time frames, or sales categories.

## Sections

### 1. Sales Overview
   - **Total Sales**: See the total sales volume and revenue.
   - **Sales by Month**: Monthly breakdown of sales to identify trends and seasonality.
   - **Top Products**: Highlight the best-selling products.

### 2. Geolocation Analysis
   - **Seller Distribution**: Map showing the location of sellers.
   - **Customer Distribution**: Map showing the location of customers.

### 3. Comparative Analysis
   - **Year-over-Year Sales**: Compare sales performance year-over-year.

## How to Use This Dashboard

1. **Navigate** through different sections using the sidebar.
2. **Apply Filters** to refine the data displayed according to your needs.
3. **Hover Over Charts and Maps** for detailed information and tooltips.

## Data Sources

The data used in this dashboard is sourced from:
- **warehouse.dim_sellers**: Contains information about sellers data.
- **warehouse.dim_customers**: Contains customers data.
- **warehouse.dim_geolocation**: Contains geological data.
- **warehouse.dim_products**: Contains products data.
- **warehouse.fact_sales**: Includes sales transactions and related metrics.


## Conclusion

This dashboard is a powerful tool for analyzing sales and customer data with a geospatial context. By leveraging this information, you can make informed decisions to optimize your sales strategies and better understand your market.

For any questions or further assistance, please contact the analytics team.

---

*Happy analyzing!*
    """
)