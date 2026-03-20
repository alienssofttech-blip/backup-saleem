#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DUMP_PATH = ROOT / 'backup'
ITEMS_CSV_PATH = ROOT / 'extracted' / 'erpnext_items.csv'
ITEM_PRICES_CSV_PATH = ROOT / 'extracted' / 'erpnext_item_prices.csv'
ITEMS_EXCEL_SAFE_CSV_PATH = ROOT / 'extracted' / 'erpnext_items_excel_safe.csv'
ITEM_GROUPS_CSV_PATH = ROOT / 'extracted' / 'erpnext_item_groups.csv'
UNMATCHED_OUTPUT_PATH = ROOT / 'extracted' / 'unmatched_product_variants.csv'
BRAND_SUPPLIER_PATH = ROOT / 'extracted' / 'brand_supplier_review.csv'
BARCODE_REVIEW_PATH = ROOT / 'extracted' / 'barcode_cleanup_review.csv'
SUMMARY_PATH = ROOT / 'extracted' / 'extraction_summary.txt'
CURRENCY = 'SAR'
DEFAULT_ITEM_GROUP = 'اكسسوارات'
DEFAULT_UOM = 'Nos'
DEFAULT_END_OF_LIFE = '2099-12-31'
BRAND_KEYWORDS = ['شانل', 'بيوتي', 'رومنتك', 'كلاسك', 'شجلام', 'سفن دي', 'فلمنجو', 'فايتي']

MAIN_ITEM_GROUPS = [
    ('اكسسوارات', 'All Item Groups', 1),
    ('التجميل', 'All Item Groups', 1),
    ('حقائب', 'All Item Groups', 1),
    ('احذية', 'All Item Groups', 1),
    ('لانجري', 'All Item Groups', 1),
    ('هدايا', 'All Item Groups', 1),
    ('اكسسوارات نسائية فاخرة', 'All Item Groups', 1),
    ('لصق حناء', 'All Item Groups', 1),
    ('نقش تاتو', 'All Item Groups', 1),
]

CHILD_ITEM_GROUPS = [
    ('اساور استيل', 'اكسسوارات نسائية فاخرة', 0), ('اساور زركون', 'اكسسوارات نسائية فاخرة', 0),
    ('اساور كرستال', 'اكسسوارات نسائية فاخرة', 0), ('اساور مطلي', 'اكسسوارات نسائية فاخرة', 0),
    ('اساور مع خاتم زركون', 'اكسسوارات نسائية فاخرة', 0), ('بروشات شعر كرستال', 'اكسسوارات نسائية فاخرة', 0),
    ('حزامات مطلي', 'اكسسوارات نسائية فاخرة', 0), ('خواتم استيل', 'اكسسوارات نسائية فاخرة', 0),
    ('خواتم زركون', 'اكسسوارات نسائية فاخرة', 0), ('خواتم كرستال', 'اكسسوارات نسائية فاخرة', 0),
    ('خواتم مطلي', 'اكسسوارات نسائية فاخرة', 0), ('دبابيس شعر كرستال', 'اكسسوارات نسائية فاخرة', 0),
    ('ربطات شعر كرستال', 'اكسسوارات نسائية فاخرة', 0), ('ساعات اطفال', 'اكسسوارات نسائية فاخرة', 0),
    ('ساعات رجال ماركة', 'اكسسوارات نسائية فاخرة', 0), ('ساعات رجالي 2', 'اكسسوارات نسائية فاخرة', 0),
    ('ساعات نسائي عرائسي', 'اكسسوارات نسائية فاخرة', 0), ('ساعات نسائي ماركه', 'اكسسوارات نسائية فاخرة', 0),
    ('سلوس استيل', 'اكسسوارات نسائية فاخرة', 0), ('سلوس زركون', 'اكسسوارات نسائية فاخرة', 0),
    ('سلوس كرستال', 'اكسسوارات نسائية فاخرة', 0), ('سلوس مطلي', 'اكسسوارات نسائية فاخرة', 0),
    ('طقم مطلي', 'اكسسوارات نسائية فاخرة', 0), ('طقوم استيل', 'اكسسوارات نسائية فاخرة', 0),
    ('طقوم زركون', 'اكسسوارات نسائية فاخرة', 0), ('طقوم ساعات نسائي رجال', 'اكسسوارات نسائية فاخرة', 0),
    ('طقوم كرستال', 'اكسسوارات نسائية فاخرة', 0), ('قطب استيل', 'اكسسوارات نسائية فاخرة', 0),
    ('قطب زركون', 'اكسسوارات نسائية فاخرة', 0), ('قطب كرستال', 'اكسسوارات نسائية فاخرة', 0),
    ('لثام مطلي', 'اكسسوارات نسائية فاخرة', 0), ('هامات رأس مطلي', 'اكسسوارات نسائية فاخرة', 0),
    ('وزغ مطلي', 'اكسسوارات نسائية فاخرة', 0),
    ('اساور1', 'اكسسوارات', 0), ('اساور2', 'اكسسوارات', 0), ('اطواق بالحبة', 'اكسسوارات', 0),
    ('امصار', 'اكسسوارات', 0), ('خلاخل1', 'اكسسوارات', 0), ('خلاخل2', 'اكسسوارات', 0),
    ('خواتم 1', 'اكسسوارات', 0), ('خواتم 2', 'اكسسوارات', 0), ('ربطات شعر', 'اكسسوارات', 0),
    ('سلوس1', 'اكسسوارات', 0), ('سلوس2', 'اكسسوارات', 0), ('شباصات1', 'اكسسوارات', 0),
    ('شباصات2', 'اكسسوارات', 0), ('صناديق قباضات اطفال وبناتي', 'اكسسوارات', 0), ('طقم اطواق', 'اكسسوارات', 0),
    ('طقم ربطات شعر', 'اكسسوارات', 0), ('طقم شباصات', 'اكسسوارات', 0), ('طقم قباطات شعر اطفال', 'اكسسوارات', 0),
    ('طقم كلبسات ومساكات', 'اكسسوارات', 0), ('فيونكات 1', 'اكسسوارات', 0), ('فيونكات 2', 'اكسسوارات', 0),
    ('قباضات نسائي', 'اكسسوارات', 0), ('قباضات+اطفال2', 'اكسسوارات', 0), ('قباضات1', 'اكسسوارات', 0),
    ('قطب 1', 'اكسسوارات', 0), ('قطب 2', 'اكسسوارات', 0), ('كلبسات1', 'اكسسوارات', 0),
    ('كلبسات2', 'اكسسوارات', 0), ('علب ربالات شعر', 'اكسسوارات', 0),
    ('1 مرايا', 'التجميل', 0), ('2 مرايا', 'التجميل', 0), ('العناية بالأقدام والاظافر', 'التجميل', 0),
    ('امشاط شعر', 'التجميل', 0), ('طقم امشاط', 'التجميل', 0), ('غطاء عيون', 'التجميل', 0),
    ('فصوص شعر', 'التجميل', 0), ('مبارد', 'التجميل', 0),
    ('لصق حناء صغير', 'لصق حناء', 0), ('لصق حناء كبير', 'لصق حناء', 0), ('لصق حناء وسط', 'لصق حناء', 0),
    ('نقش تاتو صغير', 'نقش تاتو', 0), ('نقش تاتو كبير', 'نقش تاتو', 0), ('نقش تاتو وسط', 'نقش تاتو', 0),
    ('شنط اطفال', 'حقائب', 0), ('شنط بناتي', 'حقائب', 0), ('شنط نسائي', 'حقائب', 0), ('شنط يد نسائي', 'حقائب', 0),
    ('بوتي', 'احذية', 0), ('صنادل ربل رجالي', 'احذية', 0), ('صنادل ربل نسائي', 'احذية', 0),
    ('صنادل ريش نسائي', 'احذية', 0), ('صنادل نسائي', 'احذية', 0), ('حبال مائي', 'لانجري', 0), ('علب هدايا', 'هدايا', 0),
]

ITEM_GROUP_ROWS = MAIN_ITEM_GROUPS + CHILD_ITEM_GROUPS
CHILD_GROUP_NAMES = [name for name, _, is_group in ITEM_GROUP_ROWS if is_group == 0]


def normalize_text_static(value: str) -> str:
    value = value or ''
    value = value.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا').replace('ة', 'ه').replace('ى', 'ي')
    value = re.sub(r'\s+', ' ', value.strip())
    return value

NORMALIZED_CHILD_GROUPS = sorted(CHILD_GROUP_NAMES, key=lambda value: len(normalize_text_static(value)), reverse=True)


def parse_copy_sections(text: str) -> Tuple[Dict[str, List[dict]], List[str]]:
    sections: Dict[str, List[dict]] = {}
    warnings: List[str] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.startswith('COPY public.'):
            index += 1
            continue
        header, _ = line.split(' FROM stdin;', 1)
        table_and_cols = header[len('COPY public.'):]
        table, cols_part = table_and_cols.split(' (', 1)
        columns = cols_part[:-1].split(', ')
        rows: List[dict] = []
        index += 1
        while index < len(lines):
            row_line = lines[index]
            if row_line == r'\.':
                index += 1
                break
            if row_line.startswith('COPY public.') or row_line.startswith('--'):
                warnings.append(f'{table}: COPY block ended without \\. terminator; parsed {len(rows)} rows before the next section.')
                break
            if not row_line and index + 1 < len(lines) and (lines[index + 1].startswith('--') or lines[index + 1].startswith('COPY public.')):
                warnings.append(f'{table}: COPY block ended with a blank line instead of \\. terminator; parsed {len(rows)} rows.')
                index += 1
                break
            values = row_line.split('\t')
            rows.append({col: parse_value(val) for col, val in zip(columns, values)})
            index += 1
        sections.setdefault(table, []).extend(rows)
    return sections, warnings


def parse_value(value: str):
    if value == r'\N':
        return None
    if value in {'t', 'f'}:
        return value == 't'
    if value.startswith('{') and value.endswith('}'):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value.strip()


def dedupe_rows(rows: Iterable[dict]) -> List[dict]:
    deduped: Dict[str, dict] = {}
    for row in rows:
        deduped[row['id']] = row
    return list(deduped.values())


def localized_name(value) -> Optional[str]:
    if isinstance(value, dict):
        return value.get('en_US') or value.get('ar_001') or next(iter(value.values()), None)
    return value


def json_number_map(value) -> Optional[float]:
    if isinstance(value, dict) and value:
        first = next(iter(value.values()))
        try:
            return float(first)
        except (TypeError, ValueError):
            return None
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def clean_text(value) -> str:
    return '' if value is None else str(value)


def variant_identity(variant: dict) -> Tuple[str, str]:
    return clean_text(variant.get('default_code')), clean_text(variant.get('barcode'))


def choose_item_group(item_name: str) -> str:
    normalized_name = normalize_text_static(item_name)
    if not normalized_name:
        return DEFAULT_ITEM_GROUP

    for child_group in NORMALIZED_CHILD_GROUPS:
        if child_group in normalized_name:
            # return original script form
            for original in CHILD_GROUP_NAMES:
                if normalize_text_static(original) == child_group:
                    return original

    premium_base = {
        'اساور': 'اساور', 'خواتم': 'خواتم', 'سلوس': 'سلوس', 'سلس': 'سلوس', 'طقم': 'طقم', 'طقوم': 'طقوم', 'قطب': 'قطب'
    }
    premium_finish = {'استيل': 'استيل', 'زركون': 'زركون', 'كرستال': 'كرستال', 'مطلي': 'مطلي'}
    for base_key, base_name in premium_base.items():
        if base_key in normalized_name:
            for finish_key, finish_name in premium_finish.items():
                if finish_key in normalized_name:
                    candidate = f'{base_name} {finish_name}'
                    if candidate in CHILD_GROUP_NAMES:
                        return candidate

    keyword_map = [
        (['شنط يد'], 'شنط يد نسائي'), (['شنط اطفال'], 'شنط اطفال'), (['شنط بناتي'], 'شنط بناتي'), (['شنط'], 'شنط نسائي'),
        (['بوتي'], 'بوتي'), (['صنادل ربل رجالي'], 'صنادل ربل رجالي'), (['صنادل ربل نسائي'], 'صنادل ربل نسائي'),
        (['صنادل ريش نسائي'], 'صنادل ريش نسائي'), (['صندل', 'صنادل'], 'صنادل نسائي'),
        (['حبال مائي'], 'حبال مائي'), (['هدايا'], 'علب هدايا'),
        (['حناء صغير'], 'لصق حناء صغير'), (['حناء كبير'], 'لصق حناء كبير'), (['حناء وسط'], 'لصق حناء وسط'), (['حناء'], 'لصق حناء صغير'),
        (['تاتو صغير'], 'نقش تاتو صغير'), (['تاتو كبير'], 'نقش تاتو كبير'), (['تاتو وسط'], 'نقش تاتو وسط'), (['تاتو'], 'نقش تاتو صغير'),
        (['مرايا 1'], '1 مرايا'), (['مرايا 2'], '2 مرايا'), (['مبارد'], 'مبارد'), (['امشاط'], 'امشاط شعر'),
        (['غطاء عيون'], 'غطاء عيون'), (['فصوص شعر'], 'فصوص شعر'), (['ربطات شعر'], 'ربطات شعر'),
        (['قباضات نسائي'], 'قباضات نسائي'), (['فيونكات'], 'فيونكات 1'), (['كلبسات'], 'كلبسات1'),
        (['اطواق'], 'اطواق بالحبة'), (['خلاخل'], 'خلاخل1'),
    ]
    for keywords, group_name in keyword_map:
        if any(keyword in normalized_name for keyword in keywords):
            return group_name

    main_group_map = [
        (['شنط'], 'حقائب'), (['بوتي', 'صندل', 'صنادل', 'حذاء'], 'احذية'), (['حناء'], 'لصق حناء'),
        (['تاتو'], 'نقش تاتو'), (['مرايا', 'مبارد', 'امشاط', 'اظافر', 'غطاء عيون', 'فصوص شعر'], 'التجميل'),
        (['حبال مائي'], 'لانجري'), (['هدايا'], 'هدايا'),
    ]
    for keywords, group_name in main_group_map:
        if any(keyword in normalized_name for keyword in keywords):
            return group_name

    if any(word in normalized_name for word in ['زركون', 'كرستال', 'مطلي', 'استيل', 'ساعات']):
        return 'اكسسوارات نسائية فاخرة'
    return DEFAULT_ITEM_GROUP




def normalize_barcode(raw_barcode: str) -> str:
    raw_barcode = clean_text(raw_barcode).strip()
    if not raw_barcode:
        return ''
    if raw_barcode.isdigit() or re.fullmatch(r'[A-Za-z0-9]+', raw_barcode):
        return raw_barcode
    if raw_barcode[0].isdigit() and '-' in raw_barcode:
        first_chunk = re.findall(r'\d+', raw_barcode)
        if first_chunk and len(first_chunk[0]) >= 6:
            return first_chunk[0]
    digits_only = ''.join(ch for ch in raw_barcode if ch.isdigit())
    return digits_only or raw_barcode


def infer_brand(item_name: str) -> str:
    normalized_name = normalize_text_static(item_name)
    for brand in BRAND_KEYWORDS:
        if normalize_text_static(brand) in normalized_name:
            return brand
    return ''


def build_item_data(template: Optional[dict], variant: dict) -> dict:
    name_data = (template or {}).get('name')
    item_name = localized_name(name_data) or variant.get('default_code') or variant.get('barcode') or f"Variant {variant.get('id')}"
    item_type = clean_text((template or {}).get('type') or 'consu')
    maintain_stock = 0 if item_type == 'service' else 1
    selling_rate = clean_text((template or {}).get('list_price'))
    buying_rate = json_number_map(variant.get('standard_price'))
    item_group = choose_item_group(clean_text(item_name))
    brand = infer_brand(clean_text(item_name))
    raw_barcode = clean_text(variant.get('barcode'))
    barcode = normalize_barcode(raw_barcode)
    return {
        'variant_id': clean_text(variant.get('id')),
        'template_id': clean_text(variant.get('product_tmpl_id')),
        'item_code': clean_text(variant.get('default_code') or variant.get('id')),
        'item_name': clean_text(item_name),
        'item_group': item_group,
        'barcode': barcode,
        'raw_barcode': raw_barcode,
        'brand': brand,
        'supplier': '',
        'maintain_stock': maintain_stock,
        'selling_rate': selling_rate,
        'buying_rate': '' if buying_rate is None else buying_rate,
        'has_template_data': 1 if template is not None else 0,
    }




def excel_safe_text(value: str) -> str:
    value = clean_text(value)
    if value and all(ch.isdigit() or ch in '.-' for ch in value):
        return f'="{value}"'
    return value


def build_excel_safe_item_rows(items_rows: List[dict]) -> List[dict]:
    safe_rows = []
    for row in items_rows:
        updated = dict(row)
        updated['Item Code'] = excel_safe_text(updated['Item Code'])
        updated['Barcode (Barcodes)'] = excel_safe_text(updated['Barcode (Barcodes)'])
        safe_rows.append(updated)
    return safe_rows

def build_items_template_row(item: dict) -> dict:
    return {
        'Default Unit of Measure': DEFAULT_UOM,
        'Item Code': item['item_code'],
        'Item Group': item['item_group'],
        'Item Name': item['item_name'],
        'Maintain Stock': item['maintain_stock'],
        'Opening Stock': 0,
        'Brand': item['brand'],
        'Shelf Life In Days': 0,
        'End of Life': DEFAULT_END_OF_LIFE,
        'Has Expiry Date': 0,
        'Barcode (Barcodes)': item['barcode'],
        'Re-order Qty (Reorder level based on Warehouse)': '',
        'UOM (UOMs)': DEFAULT_UOM,
        'Default Price List (Item Defaults)': '',
    }


def price_row_id(item_code: str, price_list: str, rate: str) -> str:
    return hashlib.sha1(f'{item_code}|{price_list}|{rate}'.encode('utf-8')).hexdigest()[:10]


def build_price_rows(item: dict) -> List[dict]:
    rows: List[dict] = []
    if item['selling_rate'] != '':
        rows.append({'ID': price_row_id(item['item_code'], 'Standard Selling SAR', item['selling_rate']), 'Item Code': item['item_code'], 'Price List': 'Standard Selling SAR', 'Rate': item['selling_rate'], 'Currency': CURRENCY, 'Selling': 1, 'UOM': '', 'Buying': 0, 'Item Name': item['item_name'], 'Brand': item['brand'], 'Supplier': item['supplier']})
    if item['buying_rate'] != '':
        rows.append({'ID': price_row_id(item['item_code'], 'Standard Buying', clean_text(item['buying_rate'])), 'Item Code': item['item_code'], 'Price List': 'Standard Buying', 'Rate': item['buying_rate'], 'Currency': CURRENCY, 'Selling': 0, 'UOM': DEFAULT_UOM, 'Buying': 1, 'Item Name': item['item_name'], 'Brand': item['brand'], 'Supplier': item['supplier']})
    return rows


def build_item_groups_rows() -> List[dict]:
    return [{'Item Group Name': name, 'Parent Item Group': parent, 'Is Group': is_group} for name, parent, is_group in ITEM_GROUP_ROWS]



def build_barcode_review_rows(items: List[dict]) -> List[dict]:
    rows = []
    for item in items:
        rows.append({
            'Item Code': item['item_code'],
            'Item Name': item['item_name'],
            'Raw Barcode': item['raw_barcode'],
            'Clean Barcode': item['barcode'],
            'Changed': 1 if item['raw_barcode'] != item['barcode'] else 0,
        })
    return rows

def build_brand_supplier_rows(items: List[dict]) -> List[dict]:
    rows = []
    for item in items:
        rows.append({
            'Item Code': item['item_code'],
            'Item Name': item['item_name'],
            'Barcode': item['barcode'],
            'Brand': item['brand'],
            'Supplier': item['supplier'],
            'Brand Source': 'Inferred from item name' if item['brand'] else 'Not found in dump',
            'Supplier Source': 'No supplier rows in product_supplierinfo dump section',
        })
    return rows


def write_csv(path: Path, fieldnames: List[str], rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(total_templates: int, total_unique_variants: int, exported_items: int, exported_price_rows: int, mapped_custom_groups: int, inferred_brands: int, blank_suppliers: int, cleaned_barcodes: int, skipped_missing_identity: int, fallback_rows: int, warnings: List[str]) -> None:
    lines = [
        f'Source file: {DUMP_PATH.name}',
        f'product_template rows found: {total_templates}',
        f'unique product_product rows found: {total_unique_variants}',
        f'ERPNext item rows exported: {exported_items}',
        f'ERPNext item price rows exported: {exported_price_rows}',
        f'Rows mapped into provided item-group hierarchy: {mapped_custom_groups}',
        f'Rows with inferred Brand values: {inferred_brands}',
        f'Rows with blank Supplier values: {blank_suppliers}',
        f'Rows with cleaned Barcode values: {cleaned_barcodes}',
        f'Rows skipped because both Item Code and Barcode were empty: {skipped_missing_identity}',
        f'Exported item rows using template fallbacks: {fallback_rows}',
    ]
    if warnings:
        lines.append('Warnings:')
        lines.extend(f'- {warning}' for warning in warnings)
    SUMMARY_PATH.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> None:
    text = DUMP_PATH.read_text(encoding='utf-8', errors='ignore')
    sections, warnings = parse_copy_sections(text)
    templates = {row['id']: row for row in dedupe_rows(sections.get('product_template', []))}
    variants = dedupe_rows(sections.get('product_product', []))

    unique_items: Dict[Tuple[str, str], dict] = {}
    skipped_missing_identity = 0
    fallback_rows = 0
    unmatched_rows: List[dict] = []
    for variant in sorted(variants, key=lambda row: int(row['id'])):
        identity = variant_identity(variant)
        if identity == ('', ''):
            skipped_missing_identity += 1
            continue
        if identity in unique_items:
            continue
        template = templates.get(variant.get('product_tmpl_id'))
        if template is None:
            fallback_rows += 1
            unmatched_rows.append({'Odoo Variant ID': clean_text(variant.get('id')), 'Odoo Template ID': clean_text(variant.get('product_tmpl_id')), 'Item Code': clean_text(variant.get('default_code')), 'Barcode': clean_text(variant.get('barcode')), 'Buying Rate': '' if json_number_map(variant.get('standard_price')) is None else json_number_map(variant.get('standard_price')), 'Source File': DUMP_PATH.name, 'Issue': 'Template row missing in dump'})
        unique_items[identity] = build_item_data(template, variant)

    item_data_rows = list(unique_items.values())
    item_data_rows.sort(key=lambda row: (row['item_group'], row['item_code'], row['barcode'], row['variant_id']))
    items_rows = [build_items_template_row(item) for item in item_data_rows]
    items_excel_safe_rows = build_excel_safe_item_rows(items_rows)
    price_rows: List[dict] = []
    for item in item_data_rows:
        price_rows.extend(build_price_rows(item))
    groups_rows = build_item_groups_rows()
    brand_supplier_rows = build_brand_supplier_rows(item_data_rows)
    barcode_review_rows = build_barcode_review_rows(item_data_rows)

    items_fieldnames = ['Default Unit of Measure', 'Item Code', 'Item Group', 'Item Name', 'Maintain Stock', 'Opening Stock', 'Brand', 'Shelf Life In Days', 'End of Life', 'Has Expiry Date', 'Barcode (Barcodes)', 'Re-order Qty (Reorder level based on Warehouse)', 'UOM (UOMs)', 'Default Price List (Item Defaults)']
    price_fieldnames = ['ID', 'Item Code', 'Price List', 'Rate', 'Currency', 'Selling', 'UOM', 'Buying', 'Item Name', 'Brand', 'Supplier']
    group_fieldnames = ['Item Group Name', 'Parent Item Group', 'Is Group']

    write_csv(ITEMS_CSV_PATH, items_fieldnames, items_rows)
    write_csv(ITEMS_EXCEL_SAFE_CSV_PATH, items_fieldnames, items_excel_safe_rows)
    write_csv(ITEM_PRICES_CSV_PATH, price_fieldnames, price_rows)
    write_csv(ITEM_GROUPS_CSV_PATH, group_fieldnames, groups_rows)
    write_csv(UNMATCHED_OUTPUT_PATH, ['Odoo Variant ID', 'Odoo Template ID', 'Item Code', 'Barcode', 'Buying Rate', 'Source File', 'Issue'], unmatched_rows)
    write_csv(BRAND_SUPPLIER_PATH, ['Item Code', 'Item Name', 'Barcode', 'Brand', 'Supplier', 'Brand Source', 'Supplier Source'], brand_supplier_rows)
    write_csv(BARCODE_REVIEW_PATH, ['Item Code', 'Item Name', 'Raw Barcode', 'Clean Barcode', 'Changed'], barcode_review_rows)
    mapped_custom_groups = sum(1 for row in items_rows if row['Item Group'] in {name for name, _, _ in ITEM_GROUP_ROWS})
    inferred_brands = sum(1 for row in item_data_rows if row['brand'])
    blank_suppliers = sum(1 for row in item_data_rows if not row['supplier'])
    cleaned_barcodes = sum(1 for row in item_data_rows if row['raw_barcode'] != row['barcode'])
    write_summary(len(templates), len(variants), len(items_rows), len(price_rows), mapped_custom_groups, inferred_brands, blank_suppliers, cleaned_barcodes, skipped_missing_identity, fallback_rows, warnings)

    print(f'Wrote {len(items_rows)} ERPNext item rows to {ITEMS_CSV_PATH.relative_to(ROOT)}')
    print(f'Wrote Excel-safe ERPNext item rows to {ITEMS_EXCEL_SAFE_CSV_PATH.relative_to(ROOT)}')
    print(f'Wrote {len(price_rows)} ERPNext item price rows to {ITEM_PRICES_CSV_PATH.relative_to(ROOT)}')
    print(f'Wrote {len(groups_rows)} item-group rows to {ITEM_GROUPS_CSV_PATH.relative_to(ROOT)}')
    print(f'Wrote brand/supplier review to {BRAND_SUPPLIER_PATH.relative_to(ROOT)}')
    print(f'Wrote barcode cleanup review to {BARCODE_REVIEW_PATH.relative_to(ROOT)}')
    print(f'Wrote {len(unmatched_rows)} unmatched template rows to {UNMATCHED_OUTPUT_PATH.relative_to(ROOT)}')
    print(f'Wrote extraction summary to {SUMMARY_PATH.relative_to(ROOT)}')
    if warnings:
        print('Warnings:')
        for warning in warnings:
            print(f'- {warning}')


if __name__ == '__main__':
    main()
