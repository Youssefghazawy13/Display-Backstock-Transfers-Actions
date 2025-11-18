# modules/processor.py
import pandas as pd
import re

def split_barcodes(val):
    if pd.isna(val):
        return []
    txt = str(val)
    for sep in [';', '|', '/', '\\']:
        txt = txt.replace(sep, ',')
    return [p.strip() for p in txt.split(',') if p.strip()]

def digits_only(s):
    return re.sub(r'[^0-9]', '', str(s)).lstrip('0')

def normalize_key(bc):
    d = digits_only(bc)
    return d if d != '' else str(bc).strip().lower()

def process_combined_sheet(uploaded_files, params):
    """
    uploaded_files: list of uploaded file objects (Streamlit file_uploader items)
    params: dict with DISPLAY_TARGET, BACKSTOCK_SAFETY, MIN_TRANSFER_QTY

    Returns: (final_df, transfers_df)
    """
    # Read files (single combined or multiple) -> DataFrame with required columns
    frames = []
    for f in uploaded_files:
        if f is None:
            continue
        name = getattr(f, "name", "").lower()
        try:
            if name.endswith('.csv'):
                df = pd.read_csv(f, dtype=str, low_memory=False)
            else:
                df = pd.read_excel(f, sheet_name=0, dtype=str)
        except Exception:
            f.seek(0)
            df = pd.read_csv(f, dtype=str, encoding='latin1', low_memory=False)
        frames.append(df)

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Expected columns: name_en, branch_name, barcodes, available_quantity
    required = ['name_en', 'branch_name', 'barcodes', 'available_quantity']
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # Normalize numeric
    df['SystemQty'] = pd.to_numeric(df['available_quantity'], errors='coerce').fillna(0).astype(int)

    # params
    DISPLAY_TARGET = int(params.get('DISPLAY_TARGET', 1))
    BACKSTOCK_SAFETY = int(params.get('BACKSTOCK_SAFETY', 2))
    MIN_TRANSFER_QTY = int(params.get('MIN_TRANSFER_QTY', 1))

    # compute display/backstock/need/surplus
    df['DisplayQty'] = df['SystemQty'].apply(lambda x: min(x, DISPLAY_TARGET))
    df['Backstock'] = df['SystemQty'] - df['DisplayQty']
    df['Need'] = df['DisplayQty'].apply(lambda x: max(DISPLAY_TARGET - x, 0))
    df['Surplus'] = df['Backstock'].apply(lambda x: max(x - BACKSTOCK_SAFETY, 0))

    # expand per barcode
    rows = []
    for _, r in df.iterrows():
        bcs = split_barcodes(r.get('barcodes', ''))
        if not bcs:
            rows.append({
                'Branch': r.get('branch_name', ''),
                'Product name': r.get('name_en', ''),
                'Brand': r.get('brand', '') if 'brand' in r else '',
                'Sale Price': r.get('sale_price', '') if 'sale_price' in r else '',
                'Barcodes': '',
                'Key': '',
                'System Qty': int(r['SystemQty']),
                'Display Qty': int(r['DisplayQty']),
                'Backstock': int(r['Backstock']),
                'Need': int(r['Need']),
                'Surplus': int(r['Surplus'])
            })
        else:
            for bc in bcs:
                rows.append({
                    'Branch': r.get('branch_name', ''),
                    'Product name': r.get('name_en', ''),
                    'Brand': r.get('brand', '') if 'brand' in r else '',
                    'Sale Price': r.get('sale_price', '') if 'sale_price' in r else '',
                    'Barcodes': bc,
                    'Key': normalize_key(bc),
                    'System Qty': int(r['SystemQty']),
                    'Display Qty': int(r['DisplayQty']),
                    'Backstock': int(r['Backstock']),
                    'Need': int(r['Need']),
                    'Surplus': int(r['Surplus'])
                })

    rec = pd.DataFrame(rows)

    if rec.empty:
        return pd.DataFrame(), pd.DataFrame()

    # aggregate per Key x Branch
    agg = rec.groupby(['Key', 'Branch'], as_index=False).agg({
        'Product name': 'first',
        'Brand': 'first',
        'Barcodes': lambda x: ','.join(sorted(set(x.dropna().astype(str)))),
        'Sale Price': 'first',
        'System Qty': 'sum',
        'Display Qty': 'sum',
        'Backstock': 'sum',
        'Need': 'sum',
        'Surplus': 'sum'
    })

    # greedy transfer allocation
    transfers = []
    for key in agg['Key'].unique():
        sub = agg[agg['Key'] == key].copy()
        sources = [{'branch': b, 'surplus': int(s)} for b, s in zip(sub['Branch'], sub['Surplus']) if int(s) >= MIN_TRANSFER_QTY]
        dests = [{'branch': b, 'need': int(n)} for b, n in zip(sub['Branch'], sub['Need']) if int(n) >= 1]
        for d in dests:
            need = d['need']
            for s in sources:
                if s['surplus'] <= 0:
                    continue
                qty = min(s['surplus'], need)
                if qty >= MIN_TRANSFER_QTY:
                    transfers.append({'Key': key, 'From': s['branch'], 'To': d['branch'], 'Qty': int(qty)})
                    s['surplus'] -= qty
                    need -= qty
                if need <= 0:
                    break

    transfers_df = pd.DataFrame(transfers)

    # annotate agg with suggested actions
    agg['Suggested Action'] = ''
    agg['Suggested Transfer Qty'] = 0
    agg['Suggested Partner'] = ''
    if not transfers_df.empty:
        for _, t in transfers_df.iterrows():
            mask_src = (agg['Key'] == t['Key']) & (agg['Branch'] == t['From'])
            agg.loc[mask_src, 'Suggested Action'] = agg.loc[mask_src, 'Suggested Action'].apply(
                lambda x: (x + f"Transfer to {t['To']} x{t['Qty']}; ") if str(x) != '' else (f"Transfer to {t['To']} x{t['Qty']}; "))
            agg.loc[mask_src, 'Suggested Transfer Qty'] += int(t['Qty'])
            agg.loc[mask_src, 'Suggested Partner'] = agg.loc[mask_src, 'Suggested Partner'].apply(
                lambda x: (x + f"{t['To']},") if str(x) != '' else t['To'])
            mask_dst = (agg['Key'] == t['Key']) & (agg['Branch'] == t['To'])
            agg.loc[mask_dst, 'Suggested Action'] = agg.loc[mask_dst, 'Suggested Action'].apply(
                lambda x: (x + f"Receive from {t['From']} x{t['Qty']}; ") if str(x) != '' else (f"Receive from {t['From']} x{t['Qty']}; "))
            agg.loc[mask_dst, 'Suggested Transfer Qty'] += int(t['Qty'])
            agg.loc[mask_dst, 'Suggested Partner'] = agg.loc[mask_dst, 'Suggested Partner'].apply(
                lambda x: (x + f"{t['From']},") if str(x) != '' else t['From'])

    def unified_flag(row):
        sa = str(row.get('Suggested Action', '')).strip()
        if sa != '':
            return sa.rstrip('; ')
        if row['Surplus'] > 0 and row['Need'] == 0:
            return 'Overstock — keep or transfer'
        if row['Need'] > 0 and row['Surplus'] == 0:
            return 'Need stock — consider PO'
        return 'Balanced'

    agg['Sku Flag'] = agg.apply(unified_flag, axis=1)

    # final ordering - ensure columns exist and use 'Barcodes' (not Barcode Raw)
    final_cols = ['Product name', 'Barcodes', 'Sale Price', 'Branch', 'System Qty', 'Display Qty', 'Backstock', 'Need', 'Surplus', 'Sku Flag', 'Suggested Transfer Qty', 'Suggested Partner']
    # rename if necessary (agg already has 'Barcodes')
    final = agg.copy()
    for c in final_cols:
        if c not in final.columns:
            final[c] = '' if final[c].dtype == object else 0
    final = final[final_cols].copy()

    return final, transfers_df
