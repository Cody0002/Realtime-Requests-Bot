"""Map each row to one of the four report groups: 96G, BLG, WDB or KZO.

Used by /apf and /dpf. The group comes from Kura (kz-kura.int_dw.brand_account.groupName,
selected as `group` in sql/apf_function.sql and sql/dpf_function.sql). Same approach as the
Lark bot (lark_bq_bot_telegram_migrated, app.py extract_sub_group / normalize_brand / fix_group).

The old kz-dp-prod account.group values were handled with a chain of string replacements
that only knew 'PH96G1', 'PHBLG', 'PHK', 'IDK', 'PKK'. Kura's groupName is formatted
differently, so every brand fell through to KZO and /dpf TH showed a single group.

The parser below:
  * strips the row's country prefix         'TH96G1'  -> '96G1'
  * drops separators                         'TH-BLG-2' -> 'BLG2'
  * finds the group token anywhere           96G / BLG / WDB / KZG
  * maps KZ-style codes to KZG               'PKK', 'THK1', 'K' -> 'KZG...'
then the top-level group is the token without its numeric sub-group suffix
('96G1' -> '96G'); anything that is not 96G / BLG / WDB is shown as KZO.
"""
from __future__ import annotations

import re

PARTNER_GROUPS = ("96G", "BLG", "WDB")
DEFAULT_GROUP = "KZO"
_TOKENS = ("96G", "BLG", "WDB", "KZG")


def extract_sub_group(country: str | None, raw_group: str | None) -> str:
    """Return the sub-group code in a raw label, or '' when it is not recognisable.

    'TH96G1' -> '96G1', 'PHBLG' -> 'BLG', 'WDB-2' -> 'WDB2', 'KZG' -> 'KZG',
    'PKK' / 'THK1' / 'K' -> 'KZG' / 'KZG1' / 'KZG', 'UNKNOWN' -> ''.
    """
    s = "" if raw_group is None else str(raw_group).strip().upper()
    if not s:
        return ""
    c = "" if country is None else str(country).strip().upper()
    if c and s.startswith(c):
        s = s[len(c):]
    s = re.sub(r"[^A-Z0-9]", "", s)

    for token in _TOKENS:
        idx = s.find(token)
        if idx >= 0:
            m = re.match(r"^(\d+)", s[idx + len(token):])
            return token + (m.group(1) if m else "")

    if "KZO" in s:
        return "KZG"
    m = re.match(r"^(?:[A-Z]{2})?K(\d*)$", s)
    if m:
        return "KZG" + m.group(1)
    return ""


def top_group(sub_group: str | None) -> str:
    """'96G1' -> '96G', 'BLG' -> 'BLG', 'KZG2' -> 'KZO', '' -> 'KZO'."""
    base = re.sub(r"\d+$", "", str(sub_group or "").strip().upper())
    return base if base in PARTNER_GROUPS else DEFAULT_GROUP


def resolve_group(country: str | None, raw_group: str | None) -> tuple[str, bool]:
    """Return (report_group, recognised). recognised=False means the label was not parsable."""
    sub = extract_sub_group(country, raw_group)
    return top_group(sub), bool(sub)
