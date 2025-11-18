# streamlit_app.py
import streamlit as st
from modules.processor import process_combined_sheet
from modules.ui import render_ui

st.set_page_config(page_title="Nard Transfer Assistant", layout="wide")

st.title("Nard Transfer Assistant")
st.markdown("Upload a combined stock sheet (or multiple branch files). The app computes display/backstock/need/surplus per branch and suggests transfers.")

render_ui(process_combined_sheet)
