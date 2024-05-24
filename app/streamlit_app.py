import streamlit as st
import streamlit.components.v1 as components

# Title of the Streamlit app
st.title("Embedded Metabase Dashboard")

# Render the iframe in Streamlit
components.iframe("http://0.0.0.0:3000/public/dashboard/44b9a805-7e13-4c99-b180-ba519b8abd1f", height=800, width=1000   )
