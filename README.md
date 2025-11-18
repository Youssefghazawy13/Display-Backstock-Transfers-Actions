# Nard Transfer Assistant

A small Streamlit app to analyze branch stock levels and suggest transfers so each branch keeps a display target and a safety backstock.

## How to use
1. Clone the repo.
2. Install requirements: `pip install -r requirements.txt`.
3. Run locally: `streamlit run streamlit_app.py`.
4. Upload one combined CSV/XLSX that includes columns: `name_en, branch_name, barcodes, available_quantity, sale_price (optional), brand (optional)`.
5. Adjust parameters on the sidebar and click `Compute suggestions`.
6. Download the report (CSV / Excel).

## Flags & Actions
- `Transfer to <branch> xN` — suggested transfer. Action: prepare transfer.
- `Receive from <branch> xN` — expected receiving. Action: prepare receiving.
- `Overstock — keep or transfer` — no immediate transfer destination; consider markdown.
- `Need stock — consider PO` — raise a PO.
- `Balanced` — no action needed.

## Next steps
- Add manual approve button to register transfers in a history table.
- Add mobile barcode scanning POC.
