import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(
    layout="wide",
)

st.title("Analytics on Sales Performance")

st.markdown(
    """
### Fact Table Overview

This document provides an introduction and detailed explanation of the attributes in the fact table.

#### Attributes

##### 1. order_id
- **Description**: Unique identifier for each order.
- **Data Type**: String
- **Example**: "00010242fe8c5a6d1ba2dd792cb16214"

##### 2. customer_id
- **Description**: Unique identifier for each customer who placed the order.
- **Data Type**: String
- **Example**: "3ce436f183e68e07877b285a838db11a"

##### 3. order_purchase_timestamp
- **Description**: The timestamp when the order was placed.
- **Data Type**: Timestamp
- **Example**: "2017-09-13 08:59:02.000"

##### 4. order_delivered_carrier_date
- **Description**: The date when the order was delivered to the carrier.
- **Data Type**: Timestamp
- **Example**: "2017-09-19 18:34:16.000"

##### 5. order_delivered_customer_date
- **Description**: The date when the order was delivered to the customer.
- **Data Type**: Timestamp
- **Example**: "2017-09-29 00:00:00.000"

##### 6. order_estimated_delivery_date
- **Description**: The estimated delivery date of the order.
- **Data Type**: Timestamp
- **Example**: "2017-09-29 00:00:00.000"

##### 7. product_id
- **Description**: Unique identifier for each product in the order.
- **Data Type**: String
- **Example**: "4244733e06e7ecb4970a6e2683c13e61"

##### 8. seller_id
- **Description**: Unique identifier for each seller who sold the product.
- **Data Type**: String
- **Example**: "48436dade18ac8b2bce089ec2a041202"

##### 9. shipping_limit_date
- **Description**: The last date by which the seller should ship the order.
- **Data Type**: Timestamp
- **Example**: "2017-09-19 09:45:35.000"

##### 10. price
- **Description**: The total price of the order, calculated by sum of the product price and freight value.
- **Data Type**: Float
- **Example**: 150.75

##### 11. payment_type
- **Description**: The method of payment used for the order.
- **Data Type**: String
- **Example**: "credit_card"

##### 12. payment_installments
- **Description**: The number of installments the payment is divided into.
- **Data Type**: Integer
- **Example**: 3

##### 13. payment_value
- **Description**: The total value of the payment.
- **Data Type**: Float
- **Example**: 150.75

##### 14. order_status
- **Description**: The current status of the order.
- **Data Type**: String
- **Example**: "delivered"

#### Conclusion

This fact table combines data from the orders, order items, and order payments datasets to provide a comprehensive view 
of each order. By joining these datasets, we can analyze the complete lifecycle of an order, from purchase to delivery, 
along with payment details and product-specific information. This integrated view is crucial for deriving actionable 
insights and making informed business decisions.
    """
)

components.iframe("http://localhost:3000/public/dashboard/6bd2092a-9e9f-4d6a-b50b-dddd0635dc61", height=800, width=1000)
