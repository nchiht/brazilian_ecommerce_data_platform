import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(
    layout="wide",
)

st.title("Analytics on Dimensional Tables")

st.markdown(
    """
    ### List of dimensional tables
    
    #### 1. ```dim_products```:
    
    Contains product_id, product_category_name_english
    
    #### 2. ```dim_customers```:
    
    Contains customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
    
    #### 3. ```dim_sellers```: 
    
    Contains seller_id, seller_city, seller_state, geolocation_lat, geolocation_lng
    
    #### 4. ```dim_geolocation```: 
    
    Contains geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
    """
)

components.iframe("http://localhost:3000/public/dashboard/f8fa0376-7495-4a94-86fa-0f956abf9831", height=800, width=1000)