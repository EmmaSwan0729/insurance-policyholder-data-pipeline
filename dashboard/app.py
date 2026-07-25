"""
Streamlit dashboard.

Two views:
1. Data Quality Overview — PASS/DEGRADED/BLOCKED breakdown, issue
   distribution by field.
2. Business Metrics — lapse rate trends, segment breakdowns
   (product line, age band, distribution channel).

TODO: implement once Gold layer tables exist.
"""

import streamlit as st

st.set_page_config(page_title="Policyholder Data Pipeline", layout="wide")
st.title("Insurance Policyholder Data Pipeline")
st.info("Dashboard under construction.")
