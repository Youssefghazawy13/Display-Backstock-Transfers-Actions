# modules/ui.py
import streamlit as st
import pandas as pd
import io
from datetime import datetime

def render_ui(process_fn):
    st.header('Upload combined sheet (or multiple branch files)')
    st.markdown('Upload one combined CSV/XLSX that includes columns: `name_en, branch_name, barcodes, available_quantity, sale_price (optional)`.')
    uploaded = st.file_uploader('Upload CSV/XLSX files (multiple allowed)', type=['csv','xls','xlsx'], accept_multiple_files=True)

    st.markdown('---')
    st.subheader('Parameters')
    cols = st.columns([2,2,2])
    with cols[0]:
        DISPLAY_TARGET = st.number_input('Display target per SKU', min_value=0, value=1, step=1)
    with cols[1]:
        BACKSTOCK_SAFETY = st.number_input('Backstock safety', min_value=0, value=2, step=1)
    with cols[2]:
        MIN_TRANSFER_QTY = st.number_input('Min transfer qty', min_value=1, value=1, step=1)

    st.markdown('---')
    if st.button('Compute suggestions'):
        if not uploaded:
            st.error('Please upload at least one file.')
            return
        params = {
            'DISPLAY_TARGET': DISPLAY_TARGET,
            'BACKSTOCK_SAFETY': BACKSTOCK_SAFETY,
            'MIN_TRANSFER_QTY': MIN_TRANSFER_QTY
        }
        try:
            final, transfers = process_fn(uploaded, params)
        except Exception as e:
            st.error(f'Processing failed: {e}')
            return

        st.success('Processing complete')
        st.markdown('---')
        st.subheader('Per-branch suggestions')
        st.dataframe(final.head(500), use_container_width=True)

        st.markdown('---')
        st.subheader('Transfers (aggregated)')
        if transfers.empty:
            st.info('No transfers suggested for uploaded data and parameters')
        else:
            st.dataframe(transfers, use_container_width=True)

        # Downloads
        csv_bytes = final.to_csv(index=False).encode('utf-8')
        with io.BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                final.to_excel(writer, index=False, sheet_name='BRANCH_SUGGESTIONS')
                if not transfers.empty:
                    transfers.to_excel(writer, index=False, sheet_name='TRANSFERS')
            excel_bytes = buffer.getvalue()

        st.download_button('Download report — CSV', data=csv_bytes, file_name=f'branch_transfer_report_{datetime.utcnow().strftime(\"%Y%m%dT%H%M%SZ\")}.csv')
        st.download_button('Download report — Excel', data=excel_bytes, file_name=f'branch_transfer_report_{datetime.utcnow().strftime(\"%Y%m%dT%H%M%SZ\")}.xlsx')

        st.markdown('---')
        st.info('If column names differ in your files, rename or map them before uploading. Expected columns: name_en, branch_name, barcodes, available_quantity, sale_price (optional)')
