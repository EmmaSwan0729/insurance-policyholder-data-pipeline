"""
Streamlit dashboard for the policyholder data pipeline.
 
Reads the lightweight CSV extracts produced by scripts/run_pipeline.py
(see that module's docstring for why the dashboard reads pre-aggregated
extracts rather than running Spark itself).
 
Three views:
1. Data Quality -- PASS/DEGRADED/BLOCKED breakdown and issue frequency,
   giving a data steward visibility into what the DQ Gate is catching.
2. Business Metrics -- lapse rate by segment and policy duration profile,
   the metrics an actuarial assumption-setting exercise would consume.
3. Audit Trail -- every value correction the DQ Gate applied, searchable
   by policy_id, supporting internal and external audit queries.
 
Run:
    streamlit run dashboard/app.py
"""

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Policyholder Data Pipeline", layout="wide")

DQ_STATUS_COLOURS = {"PASS": "#6BAF92", "DEGRADED": "#E0A458", "BLOCKED": "#C97064"}

@st.cache_data
def load_data():
   enriched = pd.read_csv(
      "data/gold/policyholders_enriched.csv", parse_dates=["policy_start_date"]
    )
   dq_summary = pd.read_csv("data/gold/dq_summary.csv")
   dq_reason_summary = pd.read_csv("data/gold/dq_reason_summary.csv")
   audit_log = pd.read_csv("data/audit/dq_corrections.csv", parse_dates=["transformed_at"])
   return enriched, dq_summary, dq_reason_summary, audit_log



enriched, dq_summary, dq_reason_summary, audit_log = load_data()

st.title("Insurance Policyholder Data Pipeline")
st.caption(
   "Validation, transformation, and enrichment of UK life insurance "
   "policyholder data for actuarial modelling and assumption setting."
)

tab_dq, tab_business, tab_audit = st.tabs(["Data Quality", "Business Metrics", "Audit Trail"])

# --- Data Quality

with tab_dq:
   total_records = int(dq_summary["count"].sum())
   counts_by_status = dq_summary.set_index("dq_status")["count"].to_dict()
 
   col1, col2, col3, col4 = st.columns(4)
   col1.metric("Total records", f"{total_records:,}")
   for col, status in zip((col2, col3, col4), ("PASS", "DEGRADED", "BLOCKED")):
      count = counts_by_status.get(status, 0)
      col.metric(status, f"{count:,}", f"{count / total_records:.1%}")

   left, right = st.columns(2)

   with left:
      fig = px.bar(
         dq_summary,
         x="dq_status",
         y="count",
         color="dq_status",
         color_discrete_map=DQ_STATUS_COLOURS,
         title="Records by DQ status",
      )
      fig.update_layout(showlegend=False, bargap=0.6)
      st.plotly_chart(fig, use_container_width=True)

   with right:
      fig = px.bar(
         dq_reason_summary,
         x="count",
         y="reason",
         orientation="h",
         title="Frequency by DQ reason",
         color_discrete_sequence=["#6B8CAE"],
      )
      fig.update_layout(yaxis={"categoryorder": "total ascending"})
      st.plotly_chart(fig, use_container_width=True)

# --- Business Metrics 
 
with tab_business:
   eligible = enriched[enriched["policy_status"].isin(["Active", "Lapsed"])]

   def lapse_rate_by(column: str) -> pd.DataFrame:
      grouped = eligible.groupby(column)["is_lapsed"].agg(["sum", "count"])
      grouped["lapse_rate"] = grouped["sum"] / grouped["count"]
      return grouped.reset_index().sort_values("lapse_rate", ascending=False)

   left, right = st.columns(2)

   with left:
      fig = px.bar(
         lapse_rate_by("policy_type"),
         x="policy_type",
         y="lapse_rate",
         title="Lapse rate by policy type",
         color_discrete_sequence=["#6B8CAE"],
      )
      fig.update_layout(yaxis_tickformat=".1%",bargap=0.3)
      st.plotly_chart(fig, use_container_width=True)

   with right:
      fig = px.bar(
         lapse_rate_by("distribution_channel"),
         x="distribution_channel",
         y="lapse_rate",
         title="Lapse rate by distribution channel",
         color_discrete_sequence=["#6B8CAE"],
      )
      fig.update_layout(yaxis_tickformat=".1%",bargap=0.3)
      st.plotly_chart(fig, use_container_width=True)

   fig = px.histogram(
      enriched,
      x="policy_duration_years",
      nbins=30,
      title="Policy duration profile (years in force)",
      color_discrete_sequence=["#6B8CAE"],
   )
   fig.update_layout(bargap=0.3)
   st.plotly_chart(fig, use_container_width=True)
 
# --- Audit Trail 
 
with tab_audit:
   st.subheader("DQ Gate corrections")
   st.caption(
      f"{len(audit_log):,} corrections applied. Search by policy ID to trace "
      "a specific record's correction history."
   )

   search = st.text_input("Search by policy_id")
   if search:
      filtered = audit_log[audit_log["policy_id"].str.contains(search, case=False, na=False)]
   else:
      filtered = audit_log

   st.dataframe(filtered, use_container_width=True, hide_index=True)