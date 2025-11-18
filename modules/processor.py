# modules/processor.py
import pandas as pd
import re
from typing import List, Tuple

# --- helpers ---
def split_barcodes(val: str) -> List[str]:
    if pd.isna(val) or val is None:
        return []
    txt = str(val)
    for sep in [';', '|', '/', '\\']:
        txt = txt.replace(sep, ',')
    parts = [p.strip() for p in txt.split(',') if p.strip() != '']
    return parts

def digits_only(s: str) -> str:
    return re.sub(r'[^0-9]', '', str(s)).lstrip('0')

def normalize_key(bc: str) -> str:
    d = digits_only(bc)
    return d if d != '' else str(bc).strip().lower()

# --- core processor ---
def process_combined_sheet(uploaded_files: List, params: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Accepts:
      - uploaded_files: list of file-like objects from Streamlit (CSV/XLSX)
      - params: dict with DISPLAY_TARGET, BACKSTOCK_SAFETY, MIN_TRANSFER_QTY

    Returns:
      - final_df: aggregated per Key x Branch with suggested actions and flags
      - transfers_df: aggregated transfer lines (Key, From, To, Qty)
    """
    # Read and concat uploaded files
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
            # fallback attempt
            try:
                f.seek(0)
                df = pd.read_csv(f, dtype=str, encoding='latin1', low_memory=False)
            except Exception:
                raise RuntimeError(f"Failed to read uploaded file: {getattr(f, 'name', 'unknown')}")
        frames.append(df)

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Validate required columns
    required = ['name_en', 'branch_name', 'barcodes', 'available_quantity']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}. Expected columns include: {required}")

    # Normalize numeric column
    df['SystemQty'] = pd.to_numeric(df['available_quantity'], errors='coerce').fillna(0).astype(int)

    # Parameters
    DISPLAY_TARGET = int(params.get('DISPLAY_TARGET', 1))
    BACKSTOCK_SAFETY = int(params.get('BACKSTOCK_SAFETY', 2))
    MIN_TRANSFER_QTY = int(params.get('MIN_TRANSFER_QTY', 1))

    # Compute display and backstock
    # Display = min(system, DISPLAY_TARGET)
    df['DisplayQty'] = df['SystemQty'].apply(lambda x: min(x, DISPLAY_TARGET))
    df['Backstock'] = df['SystemQty'] - df['DisplayQty']

    # New Need logic:
    # - Need_display: shortfall to reach display target
    df['Need_display'] = df['DisplayQty'].apply(lambda x: max(DISPLAY_TARGET - x, 0))
    # - Need_safety: shortfall to reach (DISPLAY_TARGET + BACKSTOCK_SAFETY) total stock
    df['Need_safety'] = df['SystemQty'].apply(lambda x: max((DISPLAY_TARGET + BACKSTOCK_SAFETY) - x, 0))
    # - Final Need: max of the two (ensures low-total branches request stock to reach safe level)
    df['Need'] = df[['Need_display', 'Need_safety']].max(axis=1).astype(int)

    # Surplus: how much can be given away without breaking safety
    df['Surplus'] = df['Backstock'].apply(lambda x: max(x - BACKSTOCK_SAFETY, 0)).astype(int)

    # Expand by barcode (rows per barcode)
    records = []
    for _, r in df.iterrows():
        bcs = split_barcodes(r.get('barcodes', ''))
        if not bcs:
            # include a row with empty barcode (still aggregated by Key later)
            records.append({
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
                records.append({
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

    rec = pd.DataFrame(records)
    if rec.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Aggregate per Key x Branch
    agg = rec.groupby(['Key', 'Branch'], as_index=False).agg({
        'Product name': 'first',
        'Brand': 'first',
        'Barcodes': lambda x: ','.join(sorted(set(x.dropna().astype(str)))) if len(x.dropna()) > 0 else '',
        'Sale Price': 'first',
        'System Qty': 'sum',
        'Display Qty': 'sum',
        'Backstock': 'sum',
        'Need': 'sum',
        'Surplus': 'sum'
    })

    # Greedy transfer allocation (no branch priority)
    transfers = []
    for key in agg['Key'].unique():
        sub = agg[agg['Key'] == key].copy()
        # prepare mutable sources/dests lists
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

    # Annotate agg with suggested actions and partners
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

    # Unified Sku Flag
    def unified_flag(row):
        sa = str(row.get('Suggested Action', '')).strip()
        if sa != '':
            return sa.rstrip('; ')
        if int(row.get('Surplus', 0)) > 0 and int(row.get('Need', 0)) == 0:
            return 'Overstock — keep or transfer'
        if int(row.get('Need', 0)) > 0 and int(row.get('Surplus', 0)) == 0:
            return 'Need stock — consider PO'
        return 'Balanced'

    agg['Sku Flag'] = agg.apply(unified_flag, axis=1)

    # Action column: human instruction derived from Suggested Action or Sku Flag
    def action_text(row):
        sa = str(row.get('Suggested Action', '')).strip()
        if sa != '':
            # prefer structured instructions
            # e.g. "Transfer to BR xN" -> "Prepare Transfer — Move N units to BR"
            parts = sa.split(';')
            # take first instruction only for single action text
            inst = parts[0].strip() if parts else sa
            if inst.lower().startswith('transfer to '):
                # format: Transfer to <Branch> x<QTY>
                try:
                    left, qty_part = inst.rsplit(' x', 1)
                    branch = left.replace('Transfer to ', '').strip()
                    qty = qty_part.strip()
                    return f"Prepare Transfer — Move {qty} units to {branch}"
                except Exception:
                    return f"Prepare Transfer — {inst}"
            if inst.lower().startswith('receive from '):
                try:
                    left, qty_part = inst.rsplit(' x', 1)
                    branch = left.replace('Receive from ', '').strip()
                    qty = qty_part.strip()
                    return f"Prepare Receiving — Expect {qty} units from {branch}"
                except Exception:
                    return f"Prepare Receiving — {inst}"
            return sa
        # no suggested action -> map flags to actions
        flag = row.get('Sku Flag', '')
        if flag.startswith('Overstock'):
            return 'Review Overstock — Consider markdown or future transfer'
        if flag.startswith('Need stock'):
            return 'Create PO — Replenish stock for this SKU'
        if flag == 'Balanced':
            return 'No action needed'
        return 'Review'

    agg['Action'] = agg.apply(action_text, axis=1)

    # Final ordering and column names
    final_cols = ['Product name', 'Barcodes', 'Sale Price', 'Branch', 'System Qty', 'Display Qty',
                  'Backstock', 'Need', 'Surplus', 'Sku Flag', 'Suggested Transfer Qty', 'Suggested Partner', 'Action']
    # ensure columns exist
    final = agg.copy()
    for c in final_cols:
        if c not in final.columns:
            # default fill depending on type
            final[c] = '' if final[c].dtype == object else 0

    final = final[final_cols].copy()

    return final, transfers_df
