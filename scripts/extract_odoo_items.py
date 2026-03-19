#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DUMP_PATH = ROOT / 'backup'
OUTPUT_PATH = ROOT / 'extracted' / 'erpnext_items.csv'
UNMATCHED_OUTPUT_PATH = ROOT / 'extracted' / 'unmatched_product_variants.csv'
SUMMARY_PATH = ROOT / 'extracted' / 'extraction_summary.txt'


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
                warnings.append(
                    f'{table}: COPY block ended without \\. terminator; parsed {len(rows)} rows before the next section.'
                )
                break
            if not row_line and index + 1 < len(lines) and (
                lines[index + 1].startswith('--') or lines[index + 1].startswith('COPY public.')
            ):
                warnings.append(
                    f'{table}: COPY block ended with a blank line instead of \\. terminator; parsed {len(rows)} rows.'
                )
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


def build_item_row(template: Optional[dict], variant: dict, categories: Dict[str, object]) -> dict:
    name_data = (template or {}).get('name')
    item_name = localized_name(name_data) or variant.get('default_code') or variant.get('barcode') or f"Variant {variant.get('id')}"
    arabic_name = name_data.get('ar_001', '') if isinstance(name_data, dict) else ''
    item_group = categories.get((template or {}).get('categ_id')) or 'All Item Groups'
    standard_selling_rate = clean_text((template or {}).get('list_price'))
    buying_rate = json_number_map(variant.get('standard_price'))
    active = bool((template or {}).get('active', True)) and bool(variant.get('active', True))

    return {
        'Odoo Template ID': clean_text(variant.get('product_tmpl_id')),
        'Odoo Variant ID': clean_text(variant.get('id')),
        'Item Code': clean_text(variant.get('default_code') or variant.get('id')),
        'Item Name': clean_text(item_name),
        'Arabic Name': clean_text(arabic_name),
        'Item Group': clean_text(item_group),
        'Item Type': clean_text((template or {}).get('type') or 'consu'),
        'Default UOM': 'Nos',
        'Barcode': clean_text(variant.get('barcode')),
        'Standard Selling Rate': standard_selling_rate,
        'Buying Rate': '' if buying_rate is None else buying_rate,
        'Disabled': 0 if active else 1,
        'Can Be Sold': 1 if (template or {}).get('sale_ok', True) else 0,
        'Can Be Purchased': 1 if (template or {}).get('purchase_ok', True) else 0,
        'Odoo Active': 1 if active else 0,
        'Has Template Data': 1 if template is not None else 0,
        'Source File': DUMP_PATH.name,
        'Data Notes': '' if template is not None else 'Template row missing in dump; selling rate/name/group use fallbacks where needed',
    }


def write_csv(path: Path, fieldnames: List[str], rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(
    total_templates: int,
    total_unique_variants: int,
    exportable_unique_items: int,
    missing_identity_rows: int,
    fallback_rows: int,
    warnings: List[str],
) -> None:
    lines = [
        f'Source file: {DUMP_PATH.name}',
        f'product_template rows found: {total_templates}',
        f'unique product_product rows found: {total_unique_variants}',
        f'Unique barcode/item-code item rows exported to ERPNext CSV: {exportable_unique_items}',
        f'Rows skipped because both Item Code and Barcode were empty: {missing_identity_rows}',
        f'Exported rows using template fallbacks: {fallback_rows}',
    ]
    if warnings:
        lines.append('Warnings:')
        lines.extend(f'- {warning}' for warning in warnings)
    SUMMARY_PATH.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> None:
    text = DUMP_PATH.read_text(encoding='utf-8', errors='ignore')
    sections, warnings = parse_copy_sections(text)

    categories = {
        row['id']: row.get('complete_name') or row.get('name')
        for row in dedupe_rows(sections.get('product_category', []))
    }
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
            unmatched_rows.append({
                'Odoo Variant ID': clean_text(variant.get('id')),
                'Odoo Template ID': clean_text(variant.get('product_tmpl_id')),
                'Item Code': clean_text(variant.get('default_code')),
                'Barcode': clean_text(variant.get('barcode')),
                'Buying Rate': '' if json_number_map(variant.get('standard_price')) is None else json_number_map(variant.get('standard_price')),
                'Source File': DUMP_PATH.name,
                'Issue': 'Template row missing in dump',
            })
        unique_items[identity] = build_item_row(template, variant, categories)

    export_rows = list(unique_items.values())
    export_rows.sort(key=lambda row: (row['Item Code'], row['Barcode'], row['Odoo Variant ID']))
    unmatched_rows.sort(key=lambda row: (row['Item Code'], row['Barcode'], row['Odoo Variant ID']))

    write_csv(
        OUTPUT_PATH,
        [
            'Odoo Template ID',
            'Odoo Variant ID',
            'Item Code',
            'Item Name',
            'Arabic Name',
            'Item Group',
            'Item Type',
            'Default UOM',
            'Barcode',
            'Standard Selling Rate',
            'Buying Rate',
            'Disabled',
            'Can Be Sold',
            'Can Be Purchased',
            'Odoo Active',
            'Has Template Data',
            'Source File',
            'Data Notes',
        ],
        export_rows,
    )
    write_csv(
        UNMATCHED_OUTPUT_PATH,
        ['Odoo Variant ID', 'Odoo Template ID', 'Item Code', 'Barcode', 'Buying Rate', 'Source File', 'Issue'],
        unmatched_rows,
    )
    write_summary(
        len(templates),
        len(variants),
        len(export_rows),
        skipped_missing_identity,
        fallback_rows,
        warnings,
    )

    print(f'Wrote {len(export_rows)} unique barcode/item-code rows to {OUTPUT_PATH.relative_to(ROOT)}')
    print(f'Wrote {len(unmatched_rows)} fallback-template rows to {UNMATCHED_OUTPUT_PATH.relative_to(ROOT)}')
    print(f'Wrote extraction summary to {SUMMARY_PATH.relative_to(ROOT)}')
    if warnings:
        print('Warnings:')
        for warning in warnings:
            print(f'- {warning}')


if __name__ == '__main__':
    main()
