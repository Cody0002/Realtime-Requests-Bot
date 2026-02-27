from telegram import Update
from telegram.constants import ParseMode
from textwrap import wrap
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import asyncio
import numpy as np
import time

# ---- unicode "font" converter ----
STYLES = {
    "mono":          {"A":0x1D670, "a":0x1D68A, "0":0x1D7F6},  # Mathematical Monospace
    "sans":          {"A":0x1D5A0, "a":0x1D5BA, "0":0x1D7E2},  # Sans-serif
    "sans_bold":     {"A":0x1D5D4, "a":0x1D5EE, "0":0x1D7EC},  # Sans-serif Bold
    "serif_bold":    {"A":0x1D400, "a":0x1D41A, "0":0x1D7CE},  # Bold
    "serif_italic":  {"A":0x1D434, "a":0x1D44E, "0":None},     # Italic (no special digits)
    "serif_bi":      {"A":0x1D468, "a":0x1D482, "0":None},     # Bold Italic
    "fullwidth":     {"A":0xFF21,  "a":0xFF41,  "0":0xFF10, "space":0x3000},  # Ｆｕｌｌｗｉｄｔｈ
}

def stylize(text: str, style: str = "mono") -> str:
    spec = STYLES.get(style)
    if not spec:
        raise ValueError(f"Unknown style '{style}'. Try: {', '.join(STYLES)}")

    out = []
    for ch in text:
        o = ch
        if "A" <= ch <= "Z" and spec.get("A") is not None:
            o = chr(spec["A"] + (ord(ch) - ord("A")))
        elif "a" <= ch <= "z" and spec.get("a") is not None:
            o = chr(spec["a"] + (ord(ch) - ord("a")))
        elif "0" <= ch <= "9" and spec.get("0") is not None:
            o = chr(spec["0"] + (ord(ch) - ord("0")))
        elif ch == " " and style == "fullwidth" and "space" in spec:
            o = chr(spec["space"])  # optional fullwidth space
        out.append(o)
    return "".join(out)

COUNTRY_TIMEZONES = {
    "TH": "Asia/Bangkok",
    "PH": "Asia/Manila",
    "BD": "Asia/Dhaka",
    "PK": "Asia/Karachi",
    "ID": "Asia/Jakarta",
    "BR": "America/Sao_Paulo",
    "MX": "America/Mexico_City",
    "CO": "America/Bogota",
}

def _country_code(country: str | None) -> str:
    raw = str(country).strip().upper() if country is not None else ""
    if raw in {"", "NONE", "NAN"}:
        return ""
    return raw if len(raw) == 2 else raw[:2]

def _country_flag(country: str | None, fallback: str = "") -> str:
    code = _country_code(country)
    if code == "CO":
        return "\U0001F1E8\U0001F1F4"
    return fallback

def _country_currency(country: str | None, fallback: str = "") -> str:
    code = _country_code(country)
    if code == "CO":
        return "COP"
    return fallback

def _country_timezone(country: str | None) -> ZoneInfo:
    raw = str(country).strip().upper() if country is not None else "TH"
    if raw in {"", "NONE", "NAN"}:
        raw = "TH"
    if raw in COUNTRY_TIMEZONES:
        code = raw
    else:
        two = raw[:2]
        code = two if two in COUNTRY_TIMEZONES else "TH"
    return ZoneInfo(COUNTRY_TIMEZONES.get(code, "Asia/Bangkok"))

def _gmt_label(dt: datetime) -> str:
    offset = dt.utcoffset() or timedelta(0)
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    if minutes == 0:
        return f"GMT{sign}{hours}"
    return f"GMT{sign}{hours}:{minutes:02d}"

def get_country_time_context(country: str | None):
    now_local = datetime.now(_country_timezone(country))
    current_time = now_local.strftime("%H:%M")
    current_date = now_local.strftime("%Y-%m-%d")

    # Expanded to 5 days so the backend caller passes enough data for growth calculation
    dates = [
        current_date,
        (now_local - timedelta(days=1)).strftime("%Y-%m-%d"),
        (now_local - timedelta(days=2)).strftime("%Y-%m-%d"),
        (now_local - timedelta(days=3)).strftime("%Y-%m-%d"),
        (now_local - timedelta(days=4)).strftime("%Y-%m-%d"),
    ]
    return current_time, dates, _gmt_label(now_local)

def get_date_range_header(country: str | None = None):
    """Backward-compatible helper returning only time + dates."""
    current_time, dates, _ = get_country_time_context(country)
    return current_time, dates
    
# ---------- Helpers ----------
def escape_md_v2(text: str) -> str:
    escape_chars = r"_*[]()~>#+-=|{}.!"
    for ch in escape_chars:
        text = text.replace(ch, "\\" + ch)
    return text

def _fmt_number(x, default= 0):
    # Safe, compact formatting for ints/floats/strings
    try:
        if isinstance(x, str) and x.strip() == "":
            return ""
        n = float(x)
        if n.is_integer():
            return f"{int(n):,}"
        if default == 0:
            return f"{n:,.0f}"
        elif default == 2:
            return f"{n:,.2f}"
    except Exception:
        return str(x)
    
def fmt_pct(val, decimals=1) -> str:
    try:
        v = float(val) * 100.0
        return f"{v:.{decimals}f}%"
    except Exception:
        return str(val)
    
def split_table_text(text, max_length=4000):
    """Split table text into chunks that fit Telegram's message limit."""
    lines = text.split("\n")
    chunks, current, length = [], [], 0
    for line in lines:
        if length + len(line) + 1 > max_length and current:
            chunks.append("\n".join(current))
            current, length = [line], len(line) + 1
        else:
            current.append(line)
            length += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks

def split_table_text_customize(text: str, first_len: int = 1400, max_len: int = 4000):
    """
    Split table text into chunks that fit Telegram's message limit.
    - first_len: length limit for the first chunk
    - max_len: length limit for subsequent chunks
    """
    lines = text.split("\n")
    chunks, current, length = [], [], 0
    current_limit = first_len  # first chunk has special limit

    for line in lines:
        if length + len(line) + 1 > current_limit and current:
            chunks.append("\n".join(current))
            current, length = [line], len(line) + 1
            current_limit = max_len  # after the first chunk, switch to full limit
        else:
            current.append(line)
            length += len(line) + 1

    if current:
        chunks.append("\n".join(current))
    return chunks

# ---------- Rendering using TableFormatter ----------

# Inline-code helper: no newlines allowed inside
def inline_code_line(s: str) -> str:
    return f"`s`"

def count_separators(s: str) -> int:
    s = str(s)
    # print("MINUS, PLUS", s.count("+") + s.count("-"))
    return s.count("-") + s.count(",") + s.count("+") + s.count("-")

def render_apf_table_v2(country, rows, max_width=72, brand=False, widths=None, separators=None):
    # --- AFTER ---
    FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩", "BR":"🇧🇷"}
    flag = _country_flag(country, FLAGS.get(country, ""))
    
    # --- group by brand ---
    brand_groups = defaultdict(list)
    for r in rows:
        brand_groups[r.get("brand", "Unknown")].append(r)

  # Use provided widths/separators or calculate them if not provided
    if widths and separators:
        w0, w1, w2, w3, w4 = widths
        x0, x1, x2, x3, x4 = separators
    else:
        # widths computed from what you'll actually print
        dates_all = [str(r.get("date","")) for r in rows]
        nars_all  = [str(_fmt_number(r.get("NAR", 0))) for r in rows]
        ftds_all  = [str(_fmt_number(r.get("FTD", 0))) for r in rows]
        stds_all  = [str(_fmt_number(r.get("STD", 0))) for r in rows]
        ttds_all  = [str(_fmt_number(r.get("TTD", 0))) for r in rows]

        w0 = max(len("Date"), *(len(x) for x in dates_all)) if dates_all else len("Date")
        x0 = max(count_separators("Date"), *(count_separators(x) for x in dates_all)) if dates_all else count_separators("Date")

        w1 = max(len("NAR"), *(len(x) for x in nars_all)) if nars_all else len("NAR")
        x1 = max(count_separators("NAR"), *(count_separators(x) for x in nars_all)) if nars_all else count_separators("NAR")

        w2 = max(len("FTD"), *(len(x) for x in ftds_all)) if ftds_all else len("FTD")
        x2 = max(count_separators("FTD"), *(count_separators(x) for x in ftds_all)) if ftds_all else count_separators("FTD")

        w3 = max(len("STD"), *(len(x) for x in stds_all)) if stds_all else len("STD")
        x3 = max(count_separators("STD"), *(count_separators(x) for x in stds_all)) if stds_all else count_separators("STD")

        w4 = max(len("TTD"), *(len(x) for x in ttds_all)) if ttds_all else len("TTD")
        x4 = max(count_separators("TTD"), *(count_separators(x) for x in ttds_all)) if ttds_all else count_separators("TTD")

    list_seperators = [x0 + x1, x2, x3, x4, 0]
    # print(list_seperators)

    def fmt_row(d, n, f, s, t):
        d = str(d)
        n = str(n)
        f = str(f)
        s = str(s)
        t = str(t)
        _d = count_separators(d)
        _n = count_separators(n)
        _f = count_separators(f)
        _s = count_separators(s)
        _t = count_separators(t)
        print(d, n, f, s, t)
        return "  ".join([
            replace_spacing(d.ljust(w0), x0, _d),
            replace_spacing(n.rjust(w1), x1, _n),
            replace_spacing(f.rjust(w2), x2, _f),
            replace_spacing(s.rjust(w3), x3, _s),
            replace_spacing(t.rjust(w4), x4, _t),
        ])

    header = fmt_row("Date", "NAR", "FTD", "STD", "TTD")
    sep = "-".join(["-" * w0, "-" * w1, "-" * w2, "-" * w3, "-" * w4])

    # --- build output ---
    current_time, _, tz_label = get_country_time_context(country)

    if not brand:

        if list(brand_groups.keys())[0] == "TOTAL":
            subtitle = escape_md_v2(f"{country} Acquisition Summary by Country \n(up to {current_time} {tz_label})")
        else:
            subtitle = escape_md_v2(f"{country} Acquisition Summary by Group \n(up to {current_time} {tz_label})")

        title_line = subtitle

        parts = [title_line]

        parts.append(wrap_separators(inline_code_line(header)))

    else:
        if (country == "BD") or (country == "PK"):
            parts = [wrap_separators(inline_code_line(header))]
        else:
            parts = []

    # Then each brand with only its rows

        # Sort brands by TTD (DESC)

    first = True
    for brand_name, items in sorted(brand_groups.items()):
        if not first:
            parts.append("")  # blank line between brands
        first = False
        print(country, brand)

        parts.append(stylize(f"*{escape_md_v2(str(brand_name))}*", style="sans_bold"))
        for r in sorted(items, key=lambda x: str(x.get("date", "")), reverse=True)[:3]:
            parts.append(
                wrap_separators(
                    inline_code_line(
                        fmt_row(
                            r.get("date", ""),
                            _fmt_number(r.get("NAR", 0)),
                            _fmt_number(r.get("FTD", 0)),
                            _fmt_number(r.get("STD", 0)),
                            _fmt_number(r.get("TTD", 0)),
                        )
                    )
                )
            )

    print(parts)
    return "\n".join(parts)

# ------- TESTING APF TABLE with GROUP and BRAND -------
from collections import defaultdict
from telegram.constants import ParseMode

def _aggregate_dpf_by_date(rows: list[dict], pseudo_brand: str):
    """Per-date totals across given rows; averages averaged; % is now Growth (Today-Yest)/Yest."""
    by_date = {}
    for r in rows:
        d = str(r.get("date", ""))
        by_date.setdefault(d, {"date": d, "brand": pseudo_brand, "TotalDeposit": 0.0, "AvgList": []})
        by_date[d]["TotalDeposit"] += _num_to_float(r.get("TotalDeposit", 0))
        if r.get("AverageDeposit") is not None:
            by_date[d]["AvgList"].append(_num_to_float(r["AverageDeposit"]))
    
    collapsed = []
    for d, obj in by_date.items():
        avg_val = (sum(obj["AvgList"])/len(obj["AvgList"])) if obj["AvgList"] else None
        collapsed.append({"date": d, "brand": pseudo_brand, "AverageDeposit": avg_val, "TotalDeposit": obj["TotalDeposit"]})
    
    # Sort descending (Newest date at index 0)
    collapsed.sort(key=lambda x: x["date"], reverse=True)

    # --- NEW LOGIC: Growth vs Previous Day ---
    for i in range(len(collapsed)):
        # If there is a "next" item in the list, that is the "previous day"
        if i + 1 < len(collapsed):
            curr_total = collapsed[i]["TotalDeposit"]
            prev_total = collapsed[i+1]["TotalDeposit"]
            # (Today - Yesterday) / Yesterday
            collapsed[i]["Weightage"] = (curr_total - prev_total) / prev_total if prev_total != 0 else None
        else:
            # Oldest date has no comparison
            collapsed[i]["Weightage"] = None

    return collapsed

def _aggregate_by_date_for_group(rows: list[dict], brand_label: str):
    """
    Collapse rows for one group into per-date totals for APF rendering.
    """
    def _to_int(v):
        try:
            return int(v)
        except Exception:
            try:
                return int(float(str(v).replace(",", "")))
            except Exception:
                return 0

    by_date = {}
    for r in rows:
        d = r.get("date")
        if d not in by_date:
            by_date[d] = {
                "date": d,
                "brand": brand_label,
                "country": r.get("country"),
                "NAR": 0,
                "FTD": 0,
                "STD": 0,
                "TTD": 0,
            }
        by_date[d]["NAR"] += _to_int(r.get("NAR", 0))
        by_date[d]["FTD"] += _to_int(r.get("FTD", 0))
        by_date[d]["STD"] += _to_int(r.get("STD", 0))
        by_date[d]["TTD"] += _to_int(r.get("TTD", 0))

    return sorted(by_date.values(), key=lambda x: str(x["date"]), reverse=True)

def render_group_then_brands(country: str, group_name: str, group_rows: list[dict], max_width=72) -> str:
    """
    Layout only:
      1) Group summary (as one compact 'pseudo-brand' block)
      2) Separator line
      3) All brands of this group (your existing brand-grouped table)
    """
    group_label = f"{group_name.upper()}"
    group_summary_rows = _aggregate_by_date_for_group(group_rows, group_label)

   # 1. Combine all rows to find the maximum possible width
    all_rows = group_summary_rows + group_rows
    
    dates_all = [str(r.get("date","")) for r in all_rows]
    nars_all  = [str(_fmt_number(r.get("NAR", 0))) for r in all_rows]
    ftds_all  = [str(_fmt_number(r.get("FTD", 0))) for r in all_rows]
    stds_all  = [str(_fmt_number(r.get("STD", 0))) for r in all_rows]
    ttds_all  = [str(_fmt_number(r.get("TTD", 0))) for r in all_rows]

    w0 = max(len("Date"), *(len(x) for x in dates_all)) if dates_all else len("Date")
    x0 = max(count_separators("Date"), *(count_separators(x) for x in dates_all)) if dates_all else count_separators("Date")

    w1 = max(len("NAR"), *(len(x) for x in nars_all)) if nars_all else len("NAR")
    x1 = max(count_separators("NAR"), *(count_separators(x) for x in nars_all)) if nars_all else count_separators("NAR")

    w2 = max(len("FTD"), *(len(x) for x in ftds_all)) if ftds_all else len("FTD")
    x2 = max(count_separators("FTD"), *(count_separators(x) for x in ftds_all)) if ftds_all else count_separators("FTD")

    w3 = max(len("STD"), *(len(x) for x in stds_all)) if stds_all else len("STD")
    x3 = max(count_separators("STD"), *(count_separators(x) for x in stds_all)) if stds_all else count_separators("STD")

    w4 = max(len("TTD"), *(len(x) for x in ttds_all)) if ttds_all else len("TTD")
    x4 = max(count_separators("TTD"), *(count_separators(x) for x in ttds_all)) if ttds_all else count_separators("TTD")
    
    common_widths = (w0, w1, w2, w3, w4)
    common_separators = (x0, x1, x2, x3, x4)

   # 2. Render both blocks using the common widths/separators
    if (country != "BD") and (country != "PK"):
        group_block = render_apf_table_v2(country, group_summary_rows, max_width=max_width, widths=common_widths, separators=common_separators)
        brands_block = render_apf_table_v2(country, group_rows, max_width=max_width, brand=True, widths=common_widths, separators=common_separators)
        sep_line = "—" * 10
        return "\n".join([group_block, sep_line, brands_block])
    
    current_time, _, tz_label = get_country_time_context(country)
    subtitle = escape_md_v2(f"{country} Acquisition Summary by Group \n(up to {current_time} {tz_label})")
    brands_block = render_apf_table_v2(country, group_rows, max_width=max_width, brand=True, widths=common_widths, separators=common_separators)
    
    parts = [subtitle, brands_block]
    return "\n".join(parts)

def _aggregate_by_date_all(rows, brand_label="ALL GROUPS TOTAL"):
    """
    Collapse ALL rows (every group/brand) into per-date totals so it prints
    as a single pseudo-brand block.
    """
    by_date = {}
    for r in rows:
        d = r.get("date")
        if d not in by_date:
            by_date[d] = {
                "date": d, "brand": brand_label, "country": r.get("country"),
                "NAR": 0, "FTD": 0, "STD": 0, "TTD": 0
            }
        by_date[d]["NAR"] += int(r.get("NAR", 0) or 0)
        by_date[d]["FTD"] += int(r.get("FTD", 0) or 0)
        by_date[d]["STD"] += int(r.get("STD", 0) or 0)
        by_date[d]["TTD"] += int(r.get("TTD", 0) or 0)
    # newest first
    return sorted(by_date.values(), key=lambda x: str(x["date"]), reverse=True)

def render_country_total(country: str, rows: list[dict], max_width=72) -> str:
    """
    One compact summary message per country: totals by date across ALL groups/brands.
    """
    total_rows = _aggregate_by_date_all(rows, brand_label="TOTAL")
    # header ON (brand=False default) so it has the country title + table header
    return render_apf_table_v2(country, total_rows, max_width=max_width)

# ---------- Summation helpers ----------
def _sum_field(rows, field="NAR"):
    s = 0
    for r in rows:
        v = r.get(field, 0)
        try:
            s += int(v)
        except Exception:
            s += int(float(v or 0))
    return s

# ---------- Telegram send ----------
async def send_apf_tables(update: Update, country_groups, max_width=72, max_length=4000):
    # country_groups: { "TH": rows_th, "PH": rows_ph, ... } where each row has keys: date, country, group, brand, NAR/FTD/STD/TTD
    for country, rows in sorted(
        ((c, r) for c, r in country_groups.items() if c is not None),
        key=lambda x: x[0]
    ):
        
        # split rows by GROUP within this country
        groups = defaultdict(list)
        for r in rows:
            g = r.get("group") or r.get("`group`") or "Unknown"
            groups[g].append(r)

        # order groups by total NAR DESC
        groups_sorted = sorted(groups.items(), key=lambda kv: _sum_field(kv[1], "NAR"), reverse=True)

        # print(f"Country {country} has {len(rows)} rows in {len(groups_sorted)} groups.")

        # one message per GROUP
        for gname, g_rows in groups_sorted:
            msg = render_group_then_brands(country, gname, g_rows, max_width=max_width)
            
            for chunk in split_table_text_customize(msg, first_len=2000):
                # safe_chunk = escape_md_v2(chunk)
                await update.effective_chat.send_message(
                    chunk,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    disable_web_page_preview=False
                )
                await asyncio.sleep(1)

        # --- country GRAND TOTAL by date (all groups/brands) ---
        total_msg = render_country_total(country, rows, max_width=max_width)
        for chunk in split_table_text_customize(total_msg, first_len=2000):
            # safe_chunk = escape_md_v2(chunk)
            await update.effective_chat.send_message(
                chunk,
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
            )
            await asyncio.sleep(1)

# ---------- Channel distribution rendering ----------
def _to_percent_number(val) -> float:
    """
    Convert val to a percent *number* (e.g., 171.8 for 171.8%).
    - Accepts "171.8%", "171.8", "0.718", 0.718, etc.
    - If input has '%', treat as already percent.
    - Else: if <= 1.5 (heuristic), treat as fraction and x100; otherwise already percent.
    """
    if val is None:
        return 0.0
    s = str(val).strip()
    had_pct = ("%" in s) or ("％" in s)
    s = s.replace("%", "").replace("％", "").replace(",", "")  # remove symbols
    try:
        x = float(s)
    except Exception:
        return 0.0
    if had_pct:
        return x
    # Heuristic: values like 0.12 → 12%; values like 171.8 → already percent
    return x * 100.0 if x <= 1.5 else x

def _to_percent_number(val) -> float:
    if val is None:
        return 0.0
    s = str(val).strip()
    had_pct = ("%" in s) or ("％" in s)
    s = s.replace("%", "").replace("％", "").replace(",", "")
    try:
        x = float(s)
    except Exception:
        return 0.0
    return x if had_pct else (x * 100.0 if x <= 1.5 else x)

# --- AFTER ---
FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩", "BR":"🇧🇷"}
CURRENCIES = {"PH":"PHP","TH":"THB","BD":"BDT","PK":"PKR","ID":"IDR", "BR":"BRL"}

def render_channel_distribution(country: str, rows: list[dict], topn: int = 5) -> str:
    FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩"}
    CURRENCIES = {"PH":"PHP","TH":"THB","BD":"BDT","PK":"PKR","ID":"IDR"}
    flag = _country_flag(country, FLAGS.get(country, ""))
    currency = _country_currency(country, CURRENCIES.get(country, ""))

    def escape_md_v2(text: str) -> str:
        for ch in r"_*[]()~`>#+-=|{}.!":
            text = text.replace(ch, "\\"+ch)
        return text

    raw_title = f"COUNTRY: {country} {flag} - ({currency})"
    # title = stylize(f"*{escape_md_v2(raw_title)}*", style="sans_bold")
    title = f"{country} Total Summary"

    # --- Prepare strings ---
    channels = [str(r.get("method",""))
                .replace("-bd","").replace("-id","").replace("-pk","").replace("bank-transfer","bank")
                .replace("-ph","").replace("qr-code","qr").replace("vcpay-native", "vcpay")
                for r in rows]
    counts  = [str(_fmt_number(r.get("deposit_tnx_count"))) for r in rows]
    vols    = [str(_fmt_number(r.get("total_deposit_amount_native"))) for r in rows]
    avgs    = [str(_fmt_number(r.get("average_deposit_amount_native"))) for r in rows]
    ratios  = [f"{_to_percent_number(r.get('pct_of_country_total_native',0)):.0f}" for r in rows]

    # --- Wrap channel names (for mapping table) ---
    MAX_CHANNEL = 25
    chan_wrapped = [wrap(ch, width=MAX_CHANNEL, break_long_words=True, break_on_hyphens=True) or [""] for ch in channels]

    # --- Column widths for Table A (metrics) ---
    w_idx = max(len("#"), len(str(len(rows))))
    w_cnt = max(len("Cnt"), *(len(x) for x in counts or ["0"]))
    x1 = max(count_separators("Cnt"), *(count_separators(x) for x in counts or ["0"]))

    w_vol = max(len("Vol"), *(len(x) for x in vols   or ["0"]))
    x2 = max(count_separators("Vol"), *(count_separators(x) for x in vols   or ["0"]))

    w_avg = max(len("Avg"), *(len(x) for x in avgs   or ["0"]))
    x3 = max(count_separators("Avg"), *(count_separators(x) for x in avgs   or ["0"]))

    w_pct = max(len("%"),   *(len(x) for x in ratios or ["0"]))
    # replace_spacing
    headerA = "  ".join([
        "#".rjust(w_idx),
        replace_spacing("Cnt".rjust(w_cnt), x1, 0),
        replace_spacing("Vol".rjust(w_vol), x2, 0),
        replace_spacing("Avg".rjust(w_avg), x3, 0),
        "%".rjust(w_pct),
    ])
    sepA = (r"-" * len(headerA))

    # --- Inline-code each line so Telegram preserves spacing ---
    def inline_code_line(s: str) -> str:
        return f"`{s.replace('`','ˋ')}`"
    
    # --- Build Table A: one line per row ---
    def inline_code(s: str) -> str:
        return f"`{s}`"
    # headerA = [wrap_separators(inline_code(headerA))]

    linesA = []
    for i in range(len(rows)):
        _count = count_separators(counts[i])
        _vols = count_separators(vols[i])
        _avgs = count_separators(avgs[i])
        
        linesA.append("  ".join([
            str(i+1).rjust(w_idx),
            replace_spacing(counts[i].rjust(w_cnt), x1, _count),
            replace_spacing(vols[i].rjust(w_vol), x2, _vols),
            replace_spacing(avgs[i].rjust(w_avg), x3, _avgs),
            ratios[i].rjust(w_pct),
        ]))

    # --- Build Table B: index → channel mapping ---
    headerB = f"{'#'.rjust(w_idx)}  Channel"

    # sublinesB = [headerB]
    linesB = []
    for i, frags in enumerate(chan_wrapped, start=1):
        linesB.append(f"{str(i).rjust(w_idx)}  {frags[0]}")
        for frag in frags[1:]:
            linesB.append(f"{'   ' * w_idx}  {frag}")

    # White comma
    # codeA = [wrap_separators(inline_code_line(l)) for l in linesA]

    codeA = [(inline_code_line(l)) for l in linesA]

    codeB = [escape_md_v2(l) for l in linesB]
    halfB = f"`{'\n'.join([*codeB])}`"
    # print(halfB)
    # print("\n".join([title, *codeA]))
    return "\n".join([title,*codeA, "", inline_code(escape_md_v2(sepA)), halfB])
    # return "\n".join([title, *headerA, *codeA, "", *sublinesB, *codeB])

async def send_channel_distribution(update: Update, country_groups: dict[str, list[dict]], max_width: int = 35):
    for country, rows in sorted(country_groups.items()):
        text = render_channel_distribution(country, rows)
        # fancy = stylize(text, style = "mono")
        await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=False)
        
# Constants
FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩"}
CURRENCIES = {"PH":"PHP","TH":"THB","BD":"BDT","PK":"PKR","ID":"IDR"}

FIGURE_SPACE = "\u2007"  # digit-width space

def fmt_num_commas(x) -> str:
    # Use your own _fmt_number (adds commas), but no thin spaces
    return _fmt_number(x)

def fmt_pct(val, decimals=1) -> str:
    try:
        v = float(val) * 100.0
        return f"{v:.{decimals}f}%"
    except Exception:
        return str(val)

def right_pad_figspace(s: str, width: int) -> str:
    # Left-pad with FIGURE_SPACE so numbers right-align visually
    n = len(s)
    return (FIGURE_SPACE * max(0, width - n)) + s

# ------- small helpers -------
def _num_to_float(x):
    if x is None: return 0.0
    s = str(x).replace(",", "").replace(" ", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def _shrink_date(d):
    # match APF visual: "2025-09-12" -> "25 0912" style; you used replace("202","2")
    # keep exactly your rule:
    return str(d)

def _fmt_commas0(x):
    try:
        return f"{_num_to_float(x):,.0f}"
    except Exception:
        return str(x)
    
def _fmt_pct_int(x, j=0):
    if x is None:
        return "+0"
    try:
        return f"{_num_to_float(x)*100:+.0f}"
    except Exception:
        return "+0"

def _fmt_pct_int_2(x, j=0):
    if x is None:
        return "-"
    try:
        return f"{_num_to_float(x)*100:+.2f}"
    except Exception:
        return "-"

def inline_code_line(s: str) -> str:
    return f"`{str(s).replace('`','ˋ')}`"

FIG = "` `"  # figure space: same width as digits in proportional fonts

def pad_with_figspace(s: str, width: int, num_seps: int, align: str = "left") -> str:
    s_count = "" if s is None else str(s).strip().replace("`","")
    # how many figure spaces needed? => Max width trừ cho width of s
    pad = max(0, width - len(s_count))

    # num of figure spaces = pad - num_seps (if any)
    num_inlines = max(0, pad - num_seps)
    num_seps = 0 if pad == 0 else num_seps

    string_inlines = "" if num_inlines == 0 else f"{num_inlines*' '}"
    
    convert_pads = string_inlines + f"`{num_seps*" "}`"
    
    print(f"Len: {len(s)}, Num inlines: {num_inlines}, Num seps: {num_seps}, String: '{s}' -> '{convert_pads + s if align=='right' else s + convert_pads}'")

    return (convert_pads + s) if align == "right" else (s + convert_pads)



def _sum_field(rows, field="TotalDeposit"):
    s = 0.0
    for r in rows:
        v = r.get(field, 0)  # raw rows (group/brand split) contain TotalDeposit
        try:
            s += float(v or 0)
        except Exception:
            pass
    return s
import re

def escape_md_v2(text: str) -> str:
    if not text:
        return text
    # escape backslash first
    text = text.replace("\\", "\\\\")
    # escape the full Telegram set
    return re.sub(r"([_*[\]()~`>#+\-=\|{}\.!])", r"\\\1", text)

def render_dpf_table_v2(country, rows, max_width=72, brand=False, widths=None, separators=None):
    # --- helpers used here are assumed to exist in your env:
    # escape_md_v2, stylize, inline_code_line, wrap_separators, backtick_with_trailing_spaces
    # _num_to_float, _fmt_commas0, _fmt_pct_int, _shrink_date, _sum_field,
    # count_separators, get_date_range_header

    FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩", "BR":"🇧🇷"}
    CURRENCIES = {"TH":"THB","PH":"PHP","BD":"BDT","PK":"PKR","ID":"IDR", "BR":"BRL"}
    flag     = _country_flag(country, FLAGS.get(country, ""))
    currency = _country_currency(country, CURRENCIES.get(country, ""))

    import pandas as pd
    from collections import defaultdict

    # group by brand
    brand_groups = defaultdict(list)
    for r in rows:
        brand_groups[r.get("brand", "Unknown")].append(r)
        
    # collapse rows by date within each brand; compute Weightage vs latest day per brand
    prepped = []
    for b, items in brand_groups.items():
        by_date = {}
        for r in items:
            d = str(r.get("date", ""))
            obj = by_date.setdefault(d, {"date": d, "brand": b, "AverageDeposit": [], "TotalDeposit": 0.0})
            obj["TotalDeposit"] += _num_to_float(r.get("TotalDeposit", 0))
            if r.get("AverageDeposit") is not None:
                obj["AverageDeposit"].append(_num_to_float(r["AverageDeposit"]))

        collapsed = []
        for d, obj in by_date.items():
            avg_val = sum(obj["AverageDeposit"])/len(obj["AverageDeposit"]) if obj["AverageDeposit"] else None
            collapsed.append({
                "date": d,
                "brand": b,
                "AverageDeposit": avg_val,
                "TotalDeposit": obj["TotalDeposit"]
            })
        
        # Sort descending (Newest first)
        collapsed.sort(key=lambda x: x["date"], reverse=True)
        
        # --- NEW CALCULATION LOGIC: (Today - Yesterday) / Yesterday ---
        for i in range(len(collapsed)):
            if i + 1 < len(collapsed):
                curr_total = collapsed[i]["TotalDeposit"]
                prev_total = collapsed[i+1]["TotalDeposit"]
                collapsed[i]["Weightage"] = (curr_total - prev_total) / prev_total if prev_total != 0 else None
            else:
                collapsed[i]["Weightage"] = None

        # 👇 FILTERING LOGIC: Keep only top 3 days for this brand
        if len(collapsed) > 3:
            collapsed = collapsed[:3]

        prepped.extend(collapsed)

    # string render per row (already formatted)
    def _row_strs(r):
        return (
            str(r["date"]).replace("/",""),
            _fmt_commas0(r["AverageDeposit"]) if r["AverageDeposit"] is not None else "-",
            _fmt_commas0(r["TotalDeposit"]),
            _fmt_pct_int_2(r["Weightage"])
        )
    
    # Use provided widths/separators or calculate them if not provided
    if widths and separators:
        w0, w1, w2, w3 = widths
        x0, x1, x2, x3 = separators
    else:
        # collect for width + separator counts
        dates_all, avgs_all, totals_all, w_all = [], [], [], []
        for r in prepped:
            d, a, t, w = _row_strs(r)
            dates_all.append(d); avgs_all.append(a); totals_all.append(t); w_all.append(w)

        w0 = max(len("Date"),  *(len(s) for s in dates_all)) if dates_all else len("Date")
        w1 = max(len("Avg"),   *(len(s) for s in avgs_all))  if avgs_all else len("Avg")
        w2 = max(len("Total"), *(len(s) for s in totals_all))if totals_all else len("Total")
        w3 = max(len("%"),     *(len(s) for s in w_all))     if w_all     else len("%")

        x0 = max(count_separators("Date"),  *(count_separators(x) for x in dates_all))  if dates_all  else count_separators("Date")
        x1 = max(count_separators("Avg"),   *(count_separators(x) for x in avgs_all))   if avgs_all   else count_separators("Avg")
        x2 = max(count_separators("Total"), *(count_separators(x) for x in totals_all)) if totals_all else count_separators("Total")
        x3 = max(count_separators("%"),     *(count_separators(x) for x in w_all))      if w_all      else count_separators("%")

    # plain-space aligned row (no figure spaces)
    def fmt_row(d, a, t, w):
        d = str(d); a = str(a); t = str(t); w = str(w)
        _a = count_separators(a)
        _t = count_separators(t)
        _x = count_separators(w)
        print('MAX SEP', x3, 'CURRENT SEP', _x)
        return "  ".join([
            d.ljust(w0),
            replace_spacing(a.rjust(w1), max_sep=x1, cur_sep=_a),
            replace_spacing(t.rjust(w2), max_sep=x2, cur_sep=_t),
            # replace_spacing(w.rjust(w3), max_sep=x3, cur_sep=_x)
            w.ljust(w3),
        ])
        
    def fmt_header_row(d, a, t, w):
        d = str(d); a = str(a); t = str(t); w = str(w)
        _d = count_separators(d)
        _a = count_separators(a)
        _t = count_separators(t)
        _x = count_separators(w)
        return "  ".join([
            replace_spacing(d.ljust(w0), max_sep=x0, cur_sep=_d),
            replace_spacing(a.rjust(w1), max_sep=x1, cur_sep=_a),
            replace_spacing(t.rjust(w2), max_sep=x2, cur_sep=_t),
            # replace_spacing(w.rjust(w3), max_sep=x3, cur_sep=_x),
            w.ljust(w3),
        ])
        
    # header + horizontal sep built from real widths
    header = fmt_header_row("Date", "Avg", "Total", "%")
    
    # --- build output (APF style) ---
    current_time, _, tz_label = get_country_time_context(country)
    title    = stylize(f"*{escape_md_v2(f'COUNTRY: {country} {flag} ({currency})')}*", style="sans_bold")
    
    if list(brand_groups.keys())[0] == "TOTAL":
        subtitle = "\n" + escape_md_v2(f"{country} Deposit Performance by Country \n(up to {current_time} {tz_label})")
    else:
        subtitle = "\n" + escape_md_v2(f"{country} Deposit Performance by Group \n(up to {current_time} {tz_label})")

    parts = []
    
    if not brand:
        parts.append(subtitle)
        parts.append(wrap_separators(inline_code_line(header)))

    # sort brands by TotalDeposit DESC
    brands_sorted = sorted(
        brand_groups.items(),
        key=lambda kv: _sum_field(kv[1], "TotalDeposit"),
        reverse=True
        )
    
    if brand:
        if (country == "BD") | (country == "PK"):
            parts = [wrap_separators(inline_code_line(header))]
        else:
            parts = []

    first = True
    for brand_name, _ in brands_sorted:
        if not first:
            parts.append("")  # blank line between brands
        first = False

        parts.append(stylize(f"*{escape_md_v2(str(brand_name))}*", style="sans_bold"))
        
        # rows for this brand (already collapsed in prepped)
        for r in filter(lambda x: x["brand"] == brand_name, prepped):
            d, a, t, w = _row_strs(r)
            line = fmt_row(d, escape_md_v2(a), escape_md_v2(t), escape_md_v2(w))
            parts.append(wrap_separators(inline_code_line(line)))

    return "\n".join(parts)

def replace_spacing(s: str, max_sep: int, cur_sep: int):
    print(cur_sep, max_sep, s)
    if cur_sep < max_sep:
        dif = max_sep - cur_sep
        print(s.replace(" "*dif, f"`{" "*dif}`"), 1)
        return s.replace(" "*dif, f"{" "*dif}", 1)
    else:
        return s

def _generate_dpf_summary_message(rows: list[dict]) -> str:
    """
    Generates the text summary comparing the latest two dates.
    """
    # 1. Aggregate totals by date (returns sorted: [Today, Yesterday, ...])
    # We use a temp label just to utilize the aggregator
    total_rows = _aggregate_dpf_by_date(rows, pseudo_brand="TOTAL")
    
    # We need at least 2 days to compare
    if len(total_rows) < 2:
        return ""

    latest = total_rows[0]
    prev = total_rows[1]

    # 2. Parse Dates (Input YYYY-MM-DD -> Output "11 Feb")
    def _fmt_date_short(d_str):
        try:
            dt = datetime.strptime(str(d_str), "%Y-%m-%d")
            return dt.strftime("%d %b")
        except:
            return str(d_str)

    d_latest_str = _fmt_date_short(latest["date"])
    d_prev_str = _fmt_date_short(prev["date"])

    # 3. Math
    val_latest = float(latest["TotalDeposit"] or 0)
    val_prev = float(prev["TotalDeposit"] or 0)

    diff_abs = val_latest - val_prev
    print(diff_abs, val_latest, val_prev)
    
    # Growth % = (Current - Prev) / Prev
    if val_prev != 0:
        growth_pct = (diff_abs / val_prev) * 100.0
    else:
        growth_pct = 100.0 if val_latest > 0 else 0.0

    # 4. Formatting
    sign = "+" if diff_abs >= 0 else "" 
    abs_str = _fmt_number(diff_abs) 
    
    # 5. Construct Message with Markdown V2 Escaping
    safe_prev_date = escape_md_v2(d_prev_str)
    safe_latest_date = escape_md_v2(d_latest_str)
    safe_sign = escape_md_v2(sign)
    safe_abs = escape_md_v2(abs_str)
    safe_pct = escape_md_v2(f"{growth_pct:.0f}")

    line1 = (f"Between {safe_prev_date} and {safe_latest_date}, the total changed by "
             f"{safe_sign}{safe_pct}% \\({safe_sign} {safe_abs}\\)\\.")
    
    # Italicize the Note
    # note_content = "Note: As of 12 February, the % calculation was updated to (Today - Yesterday) / Yesterday."
    # line2 = f"_{escape_md_v2(note_content)}_"

    # return f"{line1}\n{line2}"
    return f"{line1}"
# ------- aggregate by date (used for group summary / country total) -------
def _aggregate_dpf_by_date(rows: list[dict], pseudo_brand: str):
    """Per-date totals across given rows; averages averaged; % vs Previous Day."""
    by_date = {}
    for r in rows:
        d = str(r.get("date", ""))
        by_date.setdefault(d, {"date": d, "brand": pseudo_brand, "TotalDeposit": 0.0, "AvgList": []})
        by_date[d]["TotalDeposit"] += _num_to_float(r.get("TotalDeposit", 0))
        if r.get("AverageDeposit") is not None:
            by_date[d]["AvgList"].append(_num_to_float(r["AverageDeposit"]))
    
    collapsed = []
    for d, obj in by_date.items():
        avg_val = (sum(obj["AvgList"])/len(obj["AvgList"])) if obj["AvgList"] else None
        collapsed.append({"date": d, "brand": pseudo_brand, "AverageDeposit": avg_val, "TotalDeposit": obj["TotalDeposit"]})
    
    # Sort descending (Newest first)
    collapsed.sort(key=lambda x: x["date"], reverse=True)
    
    # --- NEW CALCULATION LOGIC: (Today - Yesterday) / Yesterday ---
    for i in range(len(collapsed)):
        if i + 1 < len(collapsed):
            curr_total = collapsed[i]["TotalDeposit"]
            prev_total = collapsed[i+1]["TotalDeposit"]
            collapsed[i]["Weightage"] = (curr_total - prev_total) / prev_total if prev_total != 0 else None
        else:
            collapsed[i]["Weightage"] = None

    return collapsed

# ------- DPF group -> brands (no inline tick; figure spaces) -------

def render_dpf_group_then_brands(country: str, group_name: str, group_rows: list[dict], max_width=72) -> str:
    # 1. Aggregate group summary data
    group_summary_rows = _aggregate_dpf_by_date(group_rows, pseudo_brand=group_name.upper())
    
    # 2. Prepare ALL rows (Summary + Brands) to calculate the maximum required width
    # We must simulate the breakdown logic to ensure widths cover the brands too.
    all_width_rows = []
    
    # Add Summary Rows
    all_width_rows.extend(group_summary_rows)
    
    # Add Brand Rows (Aggregate per brand exactly as render_dpf_table_v2 does)
    from collections import defaultdict
    brand_groups = defaultdict(list)
    for r in group_rows:
        brand_groups[r.get("brand", "Unknown")].append(r)
    
    for b, items in brand_groups.items():
        # Reuse the aggregator to get the exact rows that will be printed
        brand_collapsed = _aggregate_dpf_by_date(items, pseudo_brand=b)
        all_width_rows.extend(brand_collapsed)

    # 3. Calculate widths based on the combined list
    temp_rows = []
    for r in all_width_rows:
        d = str(r.get("date", "")).replace("/", "")
        a = _fmt_commas0(r["AverageDeposit"]) if r["AverageDeposit"] is not None else "-"
        t = _fmt_commas0(r["TotalDeposit"])
        w = _fmt_pct_int_2(r["Weightage"])
        temp_rows.append((d, a, t, w))

    if not temp_rows:
        return ""

    dates_all, avgs_all, totals_all, w_all = zip(*temp_rows)

    w0 = max(len("Date"), *(len(s) for s in dates_all))
    w1 = max(len("Avg"), *(len(s) for s in avgs_all))
    w2 = max(len("Total"), *(len(s) for s in totals_all))
    w3 = max(len("%"), *(len(s) for s in w_all))

    x0 = max(count_separators("Date"), *(count_separators(x) for x in dates_all))
    x1 = max(count_separators("Avg"), *(count_separators(x) for x in avgs_all))
    x2 = max(count_separators("Total"), *(count_separators(x) for x in totals_all))
    x3 = max(count_separators("%"), *(count_separators(x) for x in w_all))

    common_widths = (w0, w1, w2, w3)
    common_separators = (x0, x1, x2, x3)

    # 4. Render both blocks using the calculated common widths
    group_block = render_dpf_table_v2(
        country, 
        group_summary_rows, 
        max_width=max_width, 
        widths=common_widths, 
        separators=common_separators
    )
    
    brands_block = render_dpf_table_v2(
        country, 
        group_rows, 
        max_width=max_width, 
        brand=True, 
        widths=common_widths, 
        separators=common_separators
    )
    
    if (country != "BD") and (country != "PK"):
        sep_line = "—" * 10
        return "\n".join([group_block, sep_line, brands_block])
    
    current_time, _, tz_label = get_country_time_context(country)
    subtitle = escape_md_v2(f"{country} Deposit Summary by Group \n(up to {current_time} {tz_label})")
    parts = [subtitle, brands_block]

    return "\n".join(parts)

# ------- DPF country GRAND TOTAL (no inline tick; figure spaces) -------
def render_dpf_country_total(country: str, rows: list[dict], max_width=72) -> str:
    total_rows = _aggregate_dpf_by_date(rows, pseudo_brand="TOTAL")
    return render_dpf_table_v2(country, total_rows, max_width=max_width, brand=False)

# ------- sender (sort groups by TotalDeposit desc) -------
async def send_dpf_tables(update: Update, country_groups: dict[str, list[dict]], max_width: int = 72):
    # def escape_md_v2(text: str) -> str:
    #     for ch in r"=-,+":
    #         text = text.replace(ch, "\\"+ch)
    #     return text

    for country, rows in sorted(country_groups.items()):
        # --- END NEW ---
        
        # split by group
        groups = defaultdict(list)
        for r in rows:
            g = r.get("group") or "Unknown"
            groups[g].append(r)

        # order groups by summed TotalDeposit desc
        groups_sorted = sorted(groups.items(), key=lambda kv: _sum_field(kv[1], "TotalDeposit"), reverse=True)

        # one message per group (group summary + brands)
        for gname, g_rows in groups_sorted:
            msg = render_dpf_group_then_brands(country, gname, g_rows, max_width=max_width)
            for chunk in split_table_text_customize(msg, first_len=2000):
                await update.effective_chat.send_message(
                chunk, 
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
                )
                await asyncio.sleep(1)

        # final country GRAND TOTAL by date
        total_msg = render_dpf_country_total(country, rows, max_width=max_width)
        for chunk in split_table_text_customize(total_msg, first_len=2000):
            await update.effective_chat.send_message(
            chunk, 
            parse_mode=ParseMode.MARKDOWN_V2, 
            disable_web_page_preview=True
            )
            await asyncio.sleep(1)
            
        # --- NEW: Send Summary Message First ---
        try:
            summary_msg = _generate_dpf_summary_message(rows)
            if summary_msg:
                await update.effective_chat.send_message(
                    summary_msg,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    disable_web_page_preview=True
                )
                await asyncio.sleep(0.5) # Short pause to ensure order
        except Exception as e:
            print(f"Error sending summary for {country}: {e}")
# -----------------------------------------------------------
import pandas as pd
# /pmh provider TH 20251006
# # --- Rendering Functions (No changes needed) ---
# def render_deposit_provider_summary(title: str, final_report: pd.DataFrame) -> str:
#     """Formats a DataFrame into a Telegram message for deposit summaries."""
#     providers = [escape_md_v2(str(r["Provider"])) for _, r in final_report.iterrows()]
#     counts = [str(r["#Depo"]) for _, r in final_report.iterrows()]
#     pct_3m = [str(r["%3m"]) for _, r in final_report.iterrows()]
#     timeouts = [str(r["Timeo"]) for _, r in final_report.iterrows()]
#     errors = [str(r["Error"]) for _, r in final_report.iterrows()]

#     w_provider = max(len("Provider"), *(len(p) for p in providers))
#     w_depo = max(len("#Depo"), *(len(c) for c in counts))
#     w_3m = max(len("%<3m"), *(len(p) for p in pct_3m))
#     w_timeout = max(len("Timeo"), *(len(t) for t in timeouts))
#     w_error = max(len("Error"), *(len(e) for e in errors))

#     header = " ".join([ "Provider".ljust(w_provider), "#Depo".rjust(w_depo), "%<3m".rjust(w_3m), "Timeo".rjust(w_timeout), "Error".rjust(w_error) ])
#     lines = [header]
#     for i in range(len(final_report)):
#         lines.append(" ".join([ providers[i].ljust(w_provider), counts[i].rjust(w_depo), pct_3m[i].rjust(w_3m), timeouts[i].rjust(w_timeout), errors[i].rjust(w_error) ]))
    
#     message_body = "\n".join(lines)
#     return f"*{escape_md_v2(title)}*\n`{message_body}`"

# def render_withdrawal_provider_summary(title: str, final_report: pd.DataFrame) -> str:
#     """Formats a DataFrame into a Telegram message for withdrawal summaries."""
#     providers = [escape_md_v2(str(r["Provider"])) for _, r in final_report.iterrows()]
#     counts = [str(r["Withdraw"]) for _, r in final_report.iterrows()]
#     pct_5m = [str(r["%<5m"]) for _, r in final_report.iterrows()]
#     pct_15m = [str(r["%<15m"]) for _, r in final_report.iterrows()]

#     w_provider = max(len("Provider"), *(len(p) for p in providers))
#     w_withdraw = max(len("Withdraw"), *(len(c) for c in counts))
#     w_5m = max(len("%<5m"), *(len(p) for p in pct_5m))
#     w_15m = max(len("%<15m"), *(len(p) for p in pct_15m))

#     header = " ".join([ "Provider".ljust(w_provider), "Withdraw".rjust(w_withdraw), "%<5m".rjust(w_5m), "%<15m".rjust(w_15m) ])
#     lines = [header]
#     for i in range(len(final_report)):
#         lines.append(" ".join([ providers[i].ljust(w_provider), counts[i].rjust(w_withdraw), pct_5m[i].rjust(w_5m), pct_15m[i].rjust(w_15m) ]))
        
#     message_body = "\n".join(lines)
#     return f"*{escape_md_v2(title)}*\n`{message_body}`"

def process_deposits_by_method(df: pd.DataFrame) -> pd.DataFrame:
    """Processes deposit data by method, robust against list values in cells."""
    if df.empty: return pd.DataFrame()

    if df['total_count'].apply(type).eq(list).any():
        df = df.explode('total_count').copy()
        df['total_count'] = pd.to_numeric(df['total_count'], errors='coerce').fillna(0)

    deposits_df = df[df['tnx_type'] == 'DEPOSIT'].copy()
    if deposits_df.empty: return pd.DataFrame()

    provider_summary = deposits_df.pivot_table(
        index='method', # <-- CHANGED from methodKey
        columns='status', 
        values='total_count', 
        aggfunc='sum', 
        fill_value=0
    )
    fast_transactions = deposits_df[deposits_df['status'] == 'completed'].groupby('method')['transaction_within_180s'].sum() # <-- CHANGED
    provider_summary = provider_summary.join(fast_transactions, how='left').fillna(0)
    provider_summary['Num'] = provider_summary.get('completed', 0) + provider_summary.get('error', 0) + provider_summary.get('timeout', 0)
    completed_counts = provider_summary.get('completed', 0)
    timeout_counts = provider_summary.get('timeout', 0)
    error_counts = provider_summary.get('error', 0)
    def safe_percent(numerator, denominator):
        with np.errstate(divide='ignore', invalid='ignore'):
            result = np.divide(numerator, denominator) * 100
        return np.nan_to_num(result)
    provider_summary['%3m'] = safe_percent(provider_summary['transaction_within_180s'], completed_counts)
    provider_summary['Timeo'] = safe_percent(timeout_counts, provider_summary['Num'])
    provider_summary['Error'] = safe_percent(error_counts, provider_summary['Num'])
    
    final_report = provider_summary[['Num', '%3m', 'Timeo', 'Error']].reset_index().rename(columns={'method': 'Method'}) # <-- CHANGED
    final_report = final_report.sort_values(by='Num', ascending=False)

    total_num = final_report['Num'].sum()
    final_report['%'] = (final_report['Num'] / total_num * 100).round(0)

    final_report['Num'] = final_report['Num'].map('{:,.0f}'.format)
    final_report['%3m'] = final_report['%3m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%TO'] = final_report['Timeo'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%ER'] = final_report['Error'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%'] = final_report['%'].map('{:.0f}%'.format).str.replace("%", "")
    final_report["Method"] = final_report["Method"].str.replace("-", "").str.replace("normal", "norm")

    return final_report[["Method", "Num", "%", "%3m", "%TO", "%ER"]]


def process_withdrawals_by_method(df: pd.DataFrame) -> pd.DataFrame:
    """Processes withdrawal data by method, robust against list values in cells."""
    if df.empty: return pd.DataFrame()

    if df['total_count'].apply(type).eq(list).any():
        df = df.explode('total_count').copy()
        df['total_count'] = pd.to_numeric(df['total_count'], errors='coerce').fillna(0)
        
    withdrawals_df = df[df['tnx_type'] == 'WITHDRAWAL'].copy()
    if withdrawals_df.empty: return pd.DataFrame()
    
    provider_summary = withdrawals_df.groupby('method').agg(Num=('total_count', 'sum')) # <-- CHANGED
    completed_summary = withdrawals_df[withdrawals_df['status'] == 'completed'].groupby('method').agg( # <-- CHANGED
        CompletedWdraw=('total_count', 'sum'), 
        FastWdraw_5m=('transaction_within_300s', 'sum'), 
        FastWdraw_15m=('transaction_within_900s', 'sum')
    )

    provider_summary = provider_summary.join(completed_summary, how='left').fillna(0)
    provider_summary['%<5m'] = (provider_summary['FastWdraw_5m'] / provider_summary['CompletedWdraw'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    provider_summary['%<15m'] = (provider_summary['FastWdraw_15m'] / provider_summary['CompletedWdraw'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    final_report = provider_summary[['Num', '%<5m', '%<15m']].reset_index().rename(columns={'method': 'Method'}) # <-- CHANGED
    final_report = final_report.sort_values(by='Num', ascending=False)

    total_num = final_report['Num'].sum()
    final_report['%'] = (final_report['Num'] / total_num * 100).round(0)

    final_report['Num'] = final_report['Num'].map('{:,.0f}'.format)
    final_report['%5m'] = final_report['%<5m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%15m'] = final_report['%<15m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%'] = final_report['%'].map('{:.0f}%'.format).str.replace("%", "")
    final_report["Method"] = final_report["Method"].str.replace("normal", "norm")

    return final_report[["Method", "Num", "%", "%5m", "%15m"]]

def format_split_summary_table(title: str, subtitle: str, report_df: pd.DataFrame) -> str:
    """
    Formats a DataFrame into a two-part message:
    1. A table with numerical data, indexed by '#'.
    2. A mapping table from '#' to the full Method name.
    """
    if report_df.empty:
        return ""

    # Convert all data to strings for width calculation
    df_str = report_df.astype(str)
    
    # Add an index column '#' to link the two tables
    df_str.insert(0, '#', range(1, 1 + len(df_str)))
    df_str['#'] = df_str['#'].astype(str)

    # Separate the method names from the numerical data
    method_map_df = df_str[['#', 'Method']]
    numerical_df = df_str.drop(columns=['Method'])

    # --- Part 1: Build the Numerical Table ---
    col_widths_A = {col: max(len(col), numerical_df[col].str.len().max()) for col in numerical_df.columns}
    header_A = "  ".join(col.rjust(col_widths_A[col]) for col in numerical_df.columns)
    
    table_A_lines = [header_A]
    for _, row in numerical_df.iterrows():
        parts = [row[col].rjust(col_widths_A[col]) for col in numerical_df.columns]
        table_A_lines.append("  ".join(parts))

    # --- Part 2: Build the Method Name Mapping Table ---
    # First, calculate the full width of the numerical table to use for the separator
    table_A_width = len(header_A)

    # The header for the second table is now a separator line
    header_B = "-" * table_A_width
    
    # Calculate the correct width for the index column for proper alignment
    w_idx_B = method_map_df['#'].str.len().max()
    
    # Start the table lines with the new separator header
    table_B_lines = [header_B]

    for _, row in method_map_df.iterrows():
        table_B_lines.append(f"{row['#'].rjust(w_idx_B)}  {row['Method']}")

    # --- Combine all parts into the final message string ---
    message_parts = [
        f"*{escape_md_v2(title)}*",
        f"{escape_md_v2(subtitle)}",
        "`" + "\n".join(table_A_lines) + "`",
        "", # Spacer line
        "`" + "\n".join(table_B_lines) + "`"
    ]
    
    return "\n".join(message_parts)


async def send_method_summaries(update: Update, df: pd.DataFrame, target_date: str):
    """
    Processes and sends method summary reports, with Deposit and Withdrawal
    in separate messages. Each message splits the table for readability.
    """
    if df.empty:
        await update.effective_chat.send_message(
            "`No method data to process.`", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    for country, country_df in df.groupby("country"):
        try:
            title = f"{country} Payment Health by Method ({target_date})"

            # --- 1. Process and Send Deposit Message ---
            deposit_df = process_deposits_by_method(country_df)
            if not deposit_df.empty:
                deposit_text = format_split_summary_table(
                    title=title,
                    subtitle="Deposit",
                    report_df=deposit_df
                )
                await update.effective_chat.send_message(
                    deposit_text, parse_mode=ParseMode.MARKDOWN_V2
                )
                await asyncio.sleep(1)
            else:
                 await update.effective_chat.send_message(
                    f"*{escape_md_v2(title)}*\n_No deposit data to display for this period._",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                 await asyncio.sleep(1)


            # --- 2. Process and Send Withdrawal Message ---
            withdrawal_df = process_withdrawals_by_method(country_df)
            if not withdrawal_df.empty:
                withdrawal_text = format_split_summary_table(
                    title=title,
                    subtitle="Withdrawal",
                    report_df=withdrawal_df
                )
                await update.effective_chat.send_message(
                    withdrawal_text, parse_mode=ParseMode.MARKDOWN_V2
                )
                await asyncio.sleep(1)
            else:
                await update.effective_chat.send_message(
                    f"*{escape_md_v2(title)}*\n_No withdrawal data to display for this period._",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                await asyncio.sleep(1)


        except Exception as e:
            error_msg = f"Failed to generate method report for {country}: {e}"
            print(error_msg)
            await update.effective_chat.send_message(
                escape_md_v2(error_msg), parse_mode=ParseMode.MARKDOWN_V2
            )

# --- Pandas Processing Functions (No changes needed) ---
def process_deposits(df: pd.DataFrame) -> pd.DataFrame:
    """Processes deposit data, now robust against list values in cells."""
    if df.empty:
        return pd.DataFrame()

    # --- FIX: Add this block to handle lists in the data ---
    if df['total_count'].apply(type).eq(list).any():
        df = df.explode('total_count').copy()
        df['total_count'] = pd.to_numeric(df['total_count'], errors='coerce').fillna(0)
    # --- END FIX ---

    deposits_df = df[df['tnx_type'] == 'DEPOSIT'].copy()
    if deposits_df.empty: 
        return pd.DataFrame()

    # (The rest of the function remains exactly the same)
    provider_summary = deposits_df.pivot_table(
        index='providerKey', 
        columns='status', 
        values='total_count', 
        aggfunc='sum', 
        fill_value=0
    )
    fast_transactions = deposits_df[deposits_df['status'] == 'completed'].groupby('providerKey')['transaction_within_180s'].sum()
    provider_summary = provider_summary.join(fast_transactions, how='left').fillna(0)
    provider_summary['Num'] = provider_summary.get('completed', 0) + provider_summary.get('error', 0) + provider_summary.get('timeout', 0)
    completed_counts = provider_summary.get('completed', 0)
    timeout_counts = provider_summary.get('timeout', 0)
    error_counts = provider_summary.get('error', 0)
    def safe_percent(numerator, denominator):
        with np.errstate(divide='ignore', invalid='ignore'):
            result = np.divide(numerator, denominator) * 100
        return np.nan_to_num(result)
    provider_summary['%3m'] = safe_percent(provider_summary['transaction_within_180s'], completed_counts)
    provider_summary['Timeo'] = safe_percent(timeout_counts, provider_summary['Num'])
    provider_summary['Error'] = safe_percent(error_counts, provider_summary['Num'])
    
    # 1️⃣ Compute before formatting
    final_report = provider_summary[['Num', '%3m', 'Timeo', 'Error']].reset_index().rename(columns={'providerKey': 'Provider'})
    final_report = final_report.sort_values(by='Num', ascending=False)

    # 2️⃣ Compute percentage before formatting (use raw numeric 'Num')
    total_num = final_report['Num'].sum()
    final_report['%'] = (final_report['Num'] / total_num * 100).round(0)

    # 3️⃣ Apply formatting AFTER calculation
    final_report['Num'] = final_report['Num'].map('{:,.0f}'.format)
    final_report['%3m'] = final_report['%3m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%TO'] = final_report['Timeo'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%ER'] = final_report['Error'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%'] = final_report['%'].map('{:.0f}%'.format).str.replace("%", "")
    final_report["Provider"] = final_report["Provider"].str.replace("-", "").str.replace("normal", "norm")

    return final_report[["Provider", "Num", "%", "%3m", "%TO", "%ER"]]


def process_withdrawals(df: pd.DataFrame) -> pd.DataFrame:
    """Processes withdrawal data, now robust against list values in cells."""
    if df.empty:
        return pd.DataFrame()

    # --- FIX: Add this block to handle lists in the data ---
    if df['total_count'].apply(type).eq(list).any():
        df = df.explode('total_count').copy()
        df['total_count'] = pd.to_numeric(df['total_count'], errors='coerce').fillna(0)
    # --- END FIX ---
        
    withdrawals_df = df[df['tnx_type'] == 'WITHDRAWAL'].copy()
    if withdrawals_df.empty: 
        return pd.DataFrame()
    
    provider_summary = withdrawals_df.groupby('providerKey').agg(Num=('total_count', 'sum'))
    completed_summary = withdrawals_df[withdrawals_df['status'] == 'completed'].groupby('providerKey').agg(
        CompletedWdraw=('total_count', 'sum'), 
        FastWdraw_5m=('transaction_within_300s', 'sum'), 
        FastWdraw_15m=('transaction_within_900s', 'sum')
    )

    provider_summary = provider_summary.join(completed_summary, how='left').fillna(0)
    provider_summary['%<5m'] = (provider_summary['FastWdraw_5m'] / provider_summary['CompletedWdraw'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    provider_summary['%<15m'] = (provider_summary['FastWdraw_15m'] / provider_summary['CompletedWdraw'] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    final_report = provider_summary[['Num', '%<5m', '%<15m']].reset_index().rename(columns={'providerKey': 'Provider'})
    final_report = final_report.sort_values(by='Num', ascending=False)

    # Compute percentage before formatting (use raw numeric 'Num')
    total_num = final_report['Num'].sum()
    final_report['%'] = (final_report['Num'] / total_num * 100).round(0)

    final_report['Num'] = final_report['Num'].map('{:,.0f}'.format)

    final_report['%5m'] = final_report['%<5m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%15m'] = final_report['%<15m'].map('{:.0f}%'.format).str.replace("%", "")
    final_report['%'] = final_report['%'].map('{:.0f}%'.format).str.replace("%", "")

    final_report["Provider"] = final_report["Provider"].str.replace("-", "").str.replace("normal", "norm").str.replace("native", "nat").str.replace("direct", "dir")



    return final_report[["Provider", "Num", "%", "%5m", "%15m"]]


def format_table(report_df: pd.DataFrame) -> str:
    """
    Formats a DataFrame into a fixed-width, monospaced table.
    Returns just the table without code block wrapper.
    """
    if report_df.empty:
        return "No data to display."

    # Convert all data to strings for width calculation
    df_str = report_df.astype(str)
    
    # Calculate the max width for each column
    column_widths = {
        col: max(len(col), df_str[col].str.len().max())
        for col in df_str.columns
    }

    # Build the table header and rows
    header = "  ".join(
        col.ljust(column_widths[col]) if i == 0 else col.rjust(column_widths[col])
        for i, col in enumerate(df_str.columns)
    )
    
    table_lines = [header]
    for _, row in df_str.iterrows():
        parts = [
            row[col].ljust(column_widths[col]) if i == 0 else row[col].rjust(column_widths[col])
            for i, col in enumerate(df_str.columns)
        ]
        table_lines.append("  ".join(parts))

    return "\n".join(table_lines)


async def send_provider_summaries(update: Update, df: pd.DataFrame, target_date: str):
    """
    Processes and sends combined provider summary reports (Deposit + Withdrawal)
    in a single message per country.
    Only the table sections are wrapped in `code blocks`, not the entire message.
    """
    if df.empty:
        await update.effective_chat.send_message(
            "`No provider data to process.`", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    for country, country_df in df.groupby("country"):
        try:
            # --- Process both deposit and withdrawal ---
            deposit_df = process_deposits(country_df)
            withdrawal_df = process_withdrawals(country_df)

            parts = []
            title = f"{country} Payment Health by Provider ({target_date})"
            parts.append(f"*{escape_md_v2(title)}*")
            parts.append("")

            # --- Deposit Section ---
            parts.append("Deposit")
            if not deposit_df.empty:
                parts.append("`" + format_table(deposit_df) + "`")
            else:
                parts.append("_No deposit data to display._")
            parts.append("")

            # --- Withdrawal Section ---
            parts.append("Withdrawal")
            if not withdrawal_df.empty:
                parts.append("`" + format_table(withdrawal_df) + "`")
            else:
                parts.append("_No withdrawal data to display._")

            # --- Combine ---
            message_text = "\n".join(parts)

            # --- Send ---
            await update.effective_chat.send_message(
                message_text, parse_mode=ParseMode.MARKDOWN_V2
            )
            await asyncio.sleep(1)

        except Exception as e:
            error_msg = f"Failed to generate report for {country}: {e}"
            print(error_msg)
            await update.effective_chat.send_message(
                escape_md_v2(error_msg), parse_mode=ParseMode.MARKDOWN_V2
            )

# --- NEW: Asynchronous Sending Functions ---
FLAGS = {"TH":"🇹🇭","PH":"🇵🇭","BD":"🇧🇩","PK":"🇵🇰","ID":"🇮🇩", "BR":"🇧🇷"}

# async def send_deposit_summary(update: Update, df: pd.DataFrame):
#     """Groups data by country, processes it, and sends a deposit summary for each."""
#     for country, country_df in df.groupby('country'):
#         flag = FLAGS.get(country, "")
#         title = f"Deposit by Provider ({country} {flag})"
        
#         report_df = process_deposits(country_df)
#         if not report_df.empty:
#             text = render_deposit_provider_summary(title, report_df)
#             await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN_V2)

# async def send_withdrawal_summary(update: Update, df: pd.DataFrame):
#     """Groups data by country, processes it, and sends a withdrawal summary for each."""
#     for country, country_df in df.groupby('country'):
#         flag = FLAGS.get(country, "")
#         title = f"Withdrawal by Provider ({country} {flag})"
        
#         report_df = process_withdrawals(country_df)
#         if not report_df.empty:
#             text = render_withdrawal_provider_summary(title, report_df)
#             await update.effective_chat.send_message(text, parse_mode=ParseMode.MARKDOWN_V2)
#-------------------------------------
import pandas as pd
from typing import List, Tuple

def process_pmh_total(dataframe: pd.DataFrame) -> dict:
    """
    Calculates all metrics for the total PMH summary report.
    Returns a dictionary with the results.
    """
    if dataframe.empty:
        return {}
    
    # IMPORTANT: Define which providers or methods count as "Slipscan".
    SLIPSCAN_PROVIDERS = ['slipscan_provider'] # <-- CUSTOMIZE THIS LIST
    
    report = {}

    # --- Pre-calculations ---
    df_completed = dataframe[dataframe['status'] == 'completed'].copy()
    df_completed['total_duration_seconds'] = df_completed['avg_diff_seconds_transaction'] * df_completed['total_count']
        
    # df_without_slipscan = dataframe[~dataframe['providerKey'].isin(SLIPSCAN_PROVIDERS)]
    # df_completed_without_slipscan = df_completed[~df_completed['providerKey'].isin(SLIPSCAN_PROVIDERS)]
    
    deposits_df = dataframe[dataframe['tnx_type'] == 'DEPOSIT']
    withdrawals_df = dataframe[dataframe['tnx_type'] == 'WITHDRAWAL']
    
    completed_deposits_df = df_completed[df_completed['tnx_type'] == 'DEPOSIT']
    completed_withdrawals_df = df_completed[df_completed['tnx_type'] == 'WITHDRAWAL']

    # -- DEPOSITS --
    report['deposit_total'] = deposits_df['total_count'].sum()
    report['deposit_percent'] = (deposits_df['total_count'].sum()/dataframe['total_count'].sum()) * 100 if dataframe['total_count'].sum() > 0 else 0
    report['deposit_complete'] = completed_deposits_df['total_count'].sum()
    report['deposit_under_3m_count'] = completed_deposits_df['transaction_within_180s'].sum()
    report['deposit_pct_under_3m'] = (report['deposit_under_3m_count'] / report['deposit_complete']) * 100 if report['deposit_complete'] > 0 else 0
    report['deposit_avg_time'] = completed_deposits_df['total_duration_seconds'].sum() / report['deposit_complete'] if report['deposit_complete'] > 0 else 0

    # -- DEPOSITS (Without Slipscan) --
    # report['deposit_total_no_slip'] = df_without_slipscan[df_without_slipscan['tnx_type'] == 'DEPOSIT']['total_count'].sum()
    # report['deposit_complete_no_slip'] = df_completed_without_slipscan[df_completed_without_slipscan['tnx_type'] == 'DEPOSIT']['total_count'].sum()
    # report['deposit_under_3m_no_slip_count'] = df_completed_without_slipscan[df_completed_without_slipscan['tnx_type'] == 'DEPOSIT']['transaction_within_180s'].sum()
    # report['deposit_pct_under_3m_no_slip'] = (report['deposit_under_3m_no_slip_count'] / report['deposit_complete_no_slip']) * 100 if report['deposit_complete_no_slip'] > 0 else 0

    # -- WITHDRAWALS --
    # NOTE: The SQL provides transaction_within_900s for the 15min mark. You may need to add a 300s column for a true 5min mark.
    report['withdrawal_total'] = withdrawals_df['total_count'].sum()
    report['withdrawal_complete'] = completed_withdrawals_df['total_count'].sum()
    report['withdrawal_under_5m_count'] = completed_withdrawals_df['transaction_within_300s'].sum()
    report['withdrawal_under_15m_count'] = completed_withdrawals_df['transaction_within_900s'].sum()
    report['withdrawal_pct_under_5m'] = (report['withdrawal_under_5m_count'] / report['withdrawal_complete']) * 100 if report['withdrawal_complete'] > 0 else 0
    report['withdrawal_pct_under_15m'] = (report['withdrawal_under_15m_count'] / report['withdrawal_complete']) * 100 if report['withdrawal_complete'] > 0 else 0
        
    # -- PAYMENT SUCCESS RATE --
    report['total_transactions'] = dataframe['total_count'].sum()
    report['total_complete'] = dataframe[dataframe['status'] == 'completed']['total_count'].sum()
        
    # report['total_transactions_no_slip'] = df_without_slipscan['total_count'].sum()
    # report['total_complete_no_slip'] = df_without_slipscan[df_without_slipscan['status'] == 'completed']['total_count'].sum()
    # report['success_rate_no_slip'] = (report['total_complete_no_slip'] / report['total_transactions_no_slip']) * 100 if report['total_transactions_no_slip'] > 0 else 0

    # -- TIMEOUT --
    report['total_timeout'] = dataframe[dataframe['status'] == 'timeout']['total_count'].sum()
    report['total_error'] = dataframe[dataframe['status'] == 'error']['total_count'].sum()
    
    # Note: Denominator changed to 'deposit_total' to match requested table logic (%TO and %ER as a fraction of deposits)
    report['timeout_rate'] = (report['total_timeout'] / report['deposit_total']) * 100 if report['deposit_total'] > 0 else 0
    report['error_rate'] = (report['total_error'] / report['deposit_total']) * 100 if report['deposit_total'] > 0 else 0
    report['overall_success_rate'] = (report['deposit_complete'] / report['deposit_total']) * 100 if report['deposit_total'] > 0 else 0

    return report

def _format_markdown_table(headers: List[str], data: List[List[str]]) -> str:
    """Helper to format a list of headers and data into a monospaced table."""
    if not data:
        return ""

    # Calculate column widths for alignment
    col_widths = [0] * len(headers)
    for i, header in enumerate(headers):
        col_widths[i] = len(str(header))
    for row in data:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Format header
    header_line = "  ".join(
        str(headers[i]).ljust(col_widths[i]) if i == 0 else str(headers[i]).rjust(col_widths[i])
        for i in range(len(headers))
    )

    # Format body
    body_lines = []
    for row in data:
        line = "  ".join(
            str(row[j]).ljust(col_widths[j]) if j == 0 else str(row[j]).rjust(col_widths[j])
            for j in range(len(row))
        )
        body_lines.append(line)

    return "\n".join([header_line] + body_lines)

# --- render_pmh_comparison_table (Unchanged) ---
def render_pmh_comparison_table(total_report: dict, group_reports: list[tuple[str, dict]], title: str) -> str:
    """
    Renders two comparison tables (Deposits and Withdrawals) of key PMH
    metrics across different groups, as requested.

    Args:
        total_report: The report dictionary for the country's total.
        group_reports: A list of tuples, where each is (group_name, group_report_dict).
        title: The main title for the report.

    Returns:
        A formatted string containing the Markdown V2 tables.
    """
    if not total_report:
        return ""

    # --- MODIFIED LOGIC ---
    # Start with just the groups
    all_reports = group_reports

    # Only add the "TOTAL" line if there are 0 or 2+ groups.
    # If len == 1, the single group *is* the total, so we omit the "TOTAL" line.
    if len(group_reports) != 1:
        all_reports = all_reports + [("TOTAL", total_report)]
    # --- END MODIFICATION ---


    # --- Table 1: Deposits ---
    deposit_headers = ["Group", "#", "Avg", "%SC", "%TO", "%ER"]
    deposit_data = []

    for name, report in all_reports:
        row = [
            name,
            f"{report.get('deposit_total', 0):,}",  # # Deposits
            f"{report.get('deposit_avg_time', 0):.1f}s", # Avg Time
            f"{report.get('overall_success_rate', 0):.0f}", # %Success
            f"{report.get('timeout_rate', 0):.0f}",  # %TO
            f"{report.get('error_rate', 0):.1f}"  # %ER
        ]
        deposit_data.append(row)

    table1_str = _format_markdown_table(deposit_headers, deposit_data)

    # --- Table 2: Withdrawals ---
    withdrawal_headers = ["Group", "#", "%<5min", "%<15min"]
    withdrawal_data = []

    for name, report in all_reports:
        row = [
            name,
            f"{report.get('withdrawal_total', 0):,}", # # Withdrawals
            f"{report.get('withdrawal_pct_under_5m', 0):.1f}",  # %<5min
            f"{report.get('withdrawal_pct_under_15m', 0):.1f}" # %<15min
        ]
        withdrawal_data.append(row)

    table2_str = _format_markdown_table(withdrawal_headers, withdrawal_data)

    # --- Combine and Return ---
    output = [
        f"{title}",
        "*DEPOSITS*",
        f"`{table1_str}`\n",
        "*WITHDRAWALS*",
        f"`{table2_str}`"
    ]

    return "\n".join(output)


# --- Helper function (Unchanged) ---
def _compute_pmh_report(df_slice: pd.DataFrame) -> dict:
    """Computes one PMH report dictionary from a DataFrame slice."""
    report = process_pmh_total(df_slice) # Assumes process_pmh_total is defined elsewhere
    return report if report else {}


# --- Main Function (MODIFIED) ---  
async def send_pmh_total(update: Update, df: pd.DataFrame, target_date):
    """
    For each country in df:
      - Computes TOTAL and individual group stats.
      - Sends a SINGLE message with a comparison table.
    """
    if df.empty:
        await update.effective_chat.send_message("`No data.`", parse_mode=ParseMode.MARKDOWN_V2)
        return

    for country, cdf in df.groupby("country"):
        now_local = datetime.now(_country_timezone(country))
        today_local_str = now_local.date().isoformat()
        current_time_str = now_local.strftime('%H:%M')
        tz_label = _gmt_label(now_local)
        is_today = str(target_date) == today_local_str

        # 1) --- Data Processing ---
        country_title_2 = escape_md_v2(f"{country} Group Comparison ({target_date})")

        # --- NEW: Conditionally build header ---
        subtitle = ""
        if is_today:
            subtitle = escape_md_v2(f"(up to {current_time_str} {tz_label})")
        
        header = f"{country_title_2}\n"
        if subtitle:
            header = f"{country_title_2}\n{subtitle}\n"
        # --- END NEW ---

        # Compute the total report
        total_report = _compute_pmh_report(cdf)

        # Group data for comparison
        groups = defaultdict(pd.DataFrame)
        for g, gdf in cdf.groupby(cdf.get("group_name", pd.Series(["Unknown"]*len(cdf)))):
            groups[g] = gdf

        # Helper to sort groups by transaction count
        def _grp_key(gdf_):
            rep = _compute_pmh_report(gdf_) # Use new helper
            return rep.get("total_transactions", 0) if rep else 0

        ordered = sorted(groups.items(), key=lambda kv: _grp_key(kv[1]), reverse=True)

        # Compute reports for each group
        group_reports_for_comparison = []
        for gname, gdf in ordered:
            g_report = _compute_pmh_report(gdf)
            if g_report:
                group_reports_for_comparison.append((str(gname).upper(), g_report))

        # 2) --- Assemble and Send the SEPARATE Comparison Table Message ---
        # The 'header' variable is now already built
        if total_report and group_reports_for_comparison:
            comparison_message = render_pmh_comparison_table(
                total_report,
                group_reports_for_comparison,
                header  # <-- Use the new, conditional header
            )
            if comparison_message:
                for chunk in split_table_text_customize(comparison_message, first_len=3500):
                    await update.effective_chat.send_message(
                        chunk,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        disable_web_page_preview=False
                    )
                    await asyncio.sleep(1)

def _safe_div(n, d):
    try:
        n = float(n); d = float(d)
        return (n / d) if d else 0.0
    except Exception:
        return 0.0

def _pct(n, d):
    return _safe_div(n, d) * 100.0

def _fmt_num(x):   return f"{x:,.0f}"
def _fmt_sec(x):   return f"{x:.0f}s"
def _fmt_pcti(x, j = 0):  
    if j == 0:
        return f"{x:.0f}"  # keep without % sign to match your style
    else:
        return f"{x:.1f}"

def _weekly_deposits_metrics(df: pd.DataFrame) -> dict:
    """
    df: slice for one (country x group x period) with mix of statuses, tnx_type, etc.
    Returns the dict needed to print a row in the Deposits table.
    """
    d = df[df["tnx_type"] == "DEPOSIT"].copy()
    if d.empty:
        return dict(num=0, avg_s=0.0, sc=0.0, to=0.0, er=0.0)

    # counts by status
    total = d["total_count"].sum()
    comp  = d.loc[d["status"] == "completed", "total_count"].sum()
    tout  = d.loc[d["status"] == "timeout",  "total_count"].sum()
    err   = d.loc[d["status"] == "error",    "total_count"].sum()

    # avg time (use completed rows for avg)
    comp_rows = d.loc[d["status"] == "completed"]
    # weight the avg seconds by counts
    w_avg = 0.0
    if not comp_rows.empty:
        w_avg = _safe_div(
            (comp_rows["avg_diff_seconds_transaction"] * comp_rows["total_count"]).sum(),
            comp
        )

    return dict(
        num=float(total),
        avg_s=float(w_avg),
        sc=_pct(comp, total),
        to=_pct(tout, total),
        er=_pct(err,  total),
    )

def _weekly_withdrawals_metrics(df: pd.DataFrame) -> dict:
    """
    Returns dict for Withdrawals weekly row with:
      - num (# of withdrawals any status)
      - p5m (% completed within 5m)
      - p15m (% completed within 15m)
    """
    w = df[df["tnx_type"] == "WITHDRAWAL"].copy()
    if w.empty:
        return dict(num=0, p5m=0.0, p15m=0.0)

    total = w["total_count"].sum()
    comp  = w.loc[w["status"] == "completed", "total_count"].sum()

    # Sum of completed-within thresholds
    fast5  = w.loc[w["status"] == "completed", "transaction_within_300s"].sum()
    fast15 = w.loc[w["status"] == "completed", "transaction_within_900s"].sum()

    return dict(
        num=float(total),
        p5m=_pct(fast5,  comp),
        p15m=_pct(fast15, comp),
    )

def _growth_pct(cur_val, prev_val):
    if prev_val == 0 or prev_val is None:
        # define growth from 0 baseline as 100% if cur>0, else 0
        return 100.0 if (cur_val or 0) > 0 else 0.0
    return ((float(cur_val) - float(prev_val)) / float(prev_val)) * 100.0

# -- Table Build Functions (MODIFIED) ---

def _build_growth_table(base_headers, cur_rows, prev_rows, keys_order, group_order):
    """
    MODIFIED: Added 'group_order' to ensure same sorting as main table.
    MODIFIED: Growth formatted to 0 decimal places.
    MODIFIED: Group name is now UPPERCASE.
    """
    headers = base_headers[:]  # copy
    data = []
    
    # CHANGED: Iterate over the pre-sorted group_order list
    for g in group_order:
        cur = cur_rows.get(g, {})
        prv = prev_rows.get(g, {})
        
        # CHANGED: Convert group name to UPPERCASE
        row = [str(g).upper()] 
        
        for k in keys_order:
            if k != "er":
                growth = _growth_pct(cur.get(k, 0.0), prv.get(k, 0.0))
                # CHANGED: Format to 0 decimal places (e.g., "+10" instead of "+10.1")
                row.append(("+" if growth >= 0 else "") + f"{growth:.0f}")
            else:
                growth = _growth_pct(cur.get(k, 0.0), prv.get(k, 0.0))
                # CHANGED: Format to 0 decimal places (e.g., "+10" instead of "+10.1")
                row.append(("+" if growth >= 0 else "") + f"{growth:.0f}")
        data.append(row)
    df = pd.DataFrame(data, columns=headers)
    return format_table(df) # Assuming format_table() exists

def _build_deposits_table(rows):
    headers = ["Group", "#", "Avg", "%SC", "%TO", "%ER"]
    data = []
    for g, m in rows:
        # Group name is already made uppercase here, "TOTAL" will also be uppercased.
        g_disp = str(g).upper()
        data.append([
            g_disp,
            _fmt_num(m["num"]),
            _fmt_sec(m["avg_s"]),
            _fmt_pcti(m["sc"]),
            _fmt_pcti(m["to"]),
            _fmt_pcti(m["er"], 1),
        ])
    df = pd.DataFrame(data, columns=headers)
    return format_table(df) # Assuming format_table() exists

def _build_withdrawals_table(rows):
    headers = ["Group", "#", "%<5min", "%<15min"]
    data = []
    for g, m in rows:
        # Group name is already made uppercase here
        g_disp = str(g).upper()
        data.append([
            g_disp,
            _fmt_num(m["num"]),
            _fmt_pcti(m["p5m"]),
            _fmt_pcti(m["p15m"]),
        ])
    df = pd.DataFrame(data, columns=headers)
    return format_table(df) # Assuming format_table() exists

# --- Main Function (MODIFIED) ---
def week_of_month(dt: datetime, week_start: int = 0) -> int:
    """
    Week number within the month (1..5/6).
    week_start: 0=Mon, 6=Sun. Uses calendar-style counting:
      - Find the first day of the month
      - Offset by the weekday of that first day
      - Integer-divide the (offset + day-1) by 7
    """
    first = dt.replace(day=1)
    offset = (first.weekday() - week_start) % 7
    return 1 + (offset + dt.day - 1) // 7

def month_week_label(dt: datetime, week_start: int = 0) -> str:
    return f"Week {week_of_month(dt, week_start)} - {dt.strftime('%b %y')}"

async def send_pmh_week(update: Update, df: pd.DataFrame, as_of_date: str):
    if df.empty:
        await update.effective_chat.send_message("`No weekly data.`", parse_mode=ParseMode.MARKDOWN_V2)
        return

    as_of_dt = datetime.strptime(str(as_of_date), "%Y-%m-%d").date()
    week_start = (as_of_dt - timedelta(days=(as_of_dt.weekday())))

    for country, cdf in df.groupby("country"):
        now_local = datetime.now(_country_timezone(country))
        now_time = now_local.strftime("%H:%M")
        is_today = (as_of_dt == now_local.date())
        tz_label = _gmt_label(now_local)

        cur_df = cdf[cdf["period"] == "CUR"].copy()
        prv_df = cdf[cdf["period"] == "PREV"].copy()

        gkey = "group_name" if "group_name" in cdf.columns else "brand"

        cur_dep, prv_dep, cur_wdr, prv_wdr = {}, {}, {}, {}
        groups = sorted(set(cur_df[gkey].astype(str)).union(set(prv_df[gkey].astype(str))))

        for g in groups:
            g_cur = cur_df[cur_df[gkey].astype(str) == g]
            g_prv = prv_df[prv_df[gkey].astype(str) == g]
            cur_dep[g] = _weekly_deposits_metrics(g_cur)
            prv_dep[g] = _weekly_deposits_metrics(g_prv)
            cur_wdr[g] = _weekly_withdrawals_metrics(g_cur)
            prv_wdr[g] = _weekly_withdrawals_metrics(g_prv)

        # These sorted lists control the order for BOTH main and growth tables
        dep_order = sorted(groups, key=lambda x: cur_dep.get(x, {}).get("num", 0.0), reverse=True)
        wdr_order = sorted(groups, key=lambda x: cur_wdr.get(x, {}).get("num", 0.0), reverse=True)

        # === CHANGED: Add TOTAL row logic ===
        # Only add a TOTAL row if there is more than one group
        if len(groups) > 1:
            # Calculate metrics for the entire period (all groups)
            # We can re-use the metrics functions on the period dataframes
            total_cur_dep = _weekly_deposits_metrics(cur_df)
            total_prv_dep = _weekly_deposits_metrics(prv_df)
            total_cur_wdr = _weekly_withdrawals_metrics(cur_df)
            total_prv_wdr = _weekly_withdrawals_metrics(prv_df)
         
            # Add to the metric dictionaries
            cur_dep["TOTAL"] = total_cur_dep
            prv_dep["TOTAL"] = total_prv_dep
            cur_wdr["TOTAL"] = total_cur_wdr
            prv_wdr["TOTAL"] = total_prv_wdr
            
            # Add "TOTAL" to the end of the sorted order lists
            dep_order.append("TOTAL")
            wdr_order.append("TOTAL")
        # === End of TOTAL row logic ===

        # These row builders will now include "TOTAL" if it was added to dep_order/wdr_order
        dep_rows = [(g, cur_dep[g]) for g in dep_order]
        wdr_rows = [(g, cur_wdr[g]) for g in wdr_order]

        deposits_table     = _build_deposits_table(dep_rows)
        withdrawals_table  = _build_withdrawals_table(wdr_rows)

        dep_growth_table = _build_growth_table(
            base_headers=["Group", "#", "Avg", "%SC", "%TO", "%ER"],
            cur_rows=cur_dep, prev_rows=prv_dep,
            keys_order=["num","avg_s","sc","to","er"],
            group_order=dep_order  # This list now includes "TOTAL" if needed
        )
        wdr_growth_table = _build_growth_table(
            base_headers=["Group", "#", "%<5min", "%<15min"],
            cur_rows=cur_wdr, prev_rows=prv_wdr,
            keys_order=["num","p5m","p15m"],
            group_order=wdr_order  # This list now includes "TOTAL" if needed
        )

        # Build "Week N Mon YY" label (weeks start Monday = 0)
        week_label = month_week_label(datetime.combine(as_of_dt, datetime.min.time()), week_start=0)

        hdr_title = escape_md_v2(f"{country} Report - {week_label}")
        if is_today:
            hdr_range = escape_md_v2(f"(from {week_start.isoformat()} to today {now_time} {tz_label})")
        else:
            hdr_range = escape_md_v2(f"(from {week_start.isoformat()} to {as_of_dt.isoformat()})")

        # --- ONE message for Deposits: main + growth ---
        msg_deposits = "\n".join([
            f"*{hdr_title}*",
            hdr_range,
            "",
            # escape_md_v2("DEPOSITS WEEKLY REPORT"),
           stylize(f"*{escape_md_v2("DEPOSITS REPORT")}*", style="sans_bold"),
           f"`{deposits_table}`",
            "",
            stylize(f"*{escape_md_v2("DEPOSITS +/- %")}*", style="sans_bold") + escape_md_v2(" (vs. same days last week)"),
            # escape_md_v2("DEPOSITS +/- % (vs. same days last week)"),
            f"`{dep_growth_table}`",
        ])
        await update.effective_chat.send_message(msg_deposits, parse_mode=ParseMode.MARKDOWN_V2)

       # --- ONE message for Withdrawals: main + growth ---
        msg_withdrawals = "\n".join([
            f"*{hdr_title}*",
            hdr_range,
            "",
            # escape_md_v2("WITHDRAWALS"),
            stylize(f"*{escape_md_v2("WITHDRAWALS REPORT")}*", style="sans_bold"),
            f"`{withdrawals_table}`",
            "",
            stylize(f"*{escape_md_v2("WITHDRAWALS +/- %")}*", style="sans_bold") + escape_md_v2(" (vs. same days last week)"),
            # escape_md_v2("WITHDRAWALS +/- % (vs. same days last week)"),
            f"`{wdr_growth_table}`",
        ])
        await update.effective_chat.send_message(msg_withdrawals, parse_mode=ParseMode.MARKDOWN_V2)# %%%
def wrap_separators(s: str) -> str:
    """
    Replace '-' and ',' in the string with MarkdownV2-safe backtick-wrapped versions.
    
    Example:
      '2025-09-12  2,832,000  6 1 0'
    →  '2025`\\-`09`\\-`12 2`\\,`832`\\,`000 6 1 0'
    """

    ## NOT USING THIS NOW
    # if not s:
    #     return s
    # i = 0
    # result = []

    # for ch in s:
    #     if ch == "-":
    #         result.append("`\\-`")
    #         i += 1
    #     elif ch == ",":
    #         result.append("`,`")
    #         i += 1
    #     elif ch == ":":
    #         result.append("` `")
    #         # i += 1
    #         # if i == count_:
    #             # result.append(f"`{" "*i}`")
    #     else:
    #         result.append(ch)

    # final_result= "".join(result)
    # # print("RESULT:", final_result)
    # final_result = final_result.replace("` `` `", " ")
    # print("RESULT:", final_result)
    # return final_result
    return s

import re

def backtick_with_trailing_spaces(line: str, spaces: list[int]) -> str:
    """
    Example:
      line   = "`Date           NAR   FTD   STD   TTD`"  # 12 spaces after Date
      spaces = [2, 1, 0, 0, 0]
      Output: `Date␠␠`␠␠␠␠␠␠␠␠␠␠`NAR␠` `FTD   STD   TTD`
              ^^^^^ inside      ^^^^^^^^^^^ remaining original spaces (12-2 = 10)
    """
    # strip global wrapping backticks if present
    src = line.strip()
    if src.startswith("`") and src.endswith("`"):
        src = src[1:-1]

    # Split into alternating [word][ws][word][ws]... preserving whitespace
    tokens = re.findall(r'\S+|\s+', src)
    # Build (word, following_ws) pairs
    pairs = []
    i = 0
    while i < len(tokens):
        word = tokens[i]
        ws = tokens[i+1] if i+1 < len(tokens) and tokens[i+1].isspace() else ""
        pairs.append((word, ws))
        i += 2 if ws else 1

    words = [w for w, _ in pairs]
    if len(words) != len(spaces):
        raise ValueError(f"Expected {len(words)} space counts, got {len(spaces)}")

    # last index where we split explicitly
    last_nonzero = max((idx for idx, n in enumerate(spaces) if n > 0), default=-1)

    out = []
    for idx, ((word, ws), n) in enumerate(zip(pairs, spaces)):
        if idx <= last_nonzero:
            # take n spaces inside backticks, keep the rest outside
            take = min(int(n), len(ws))
            outside = ws[:take]
            inside = ws[take:]
            out.append(f"`{word}{inside}`{outside}")
        else:
            # collapse the remainder (this word + its original trailing ws + the rest) into one backticked block
            # reconstruct remainder exactly from the original tokens
            # find the token position of this word
            # rebuild the exact tail string
            # (simpler: join this word+ws, plus all following pairs)
            tail_parts = [word + ws]
            for w2, ws2 in pairs[idx+1:]:
                tail_parts.append(w2 + ws2)
            tail = "".join(tail_parts)
            out.append(f"`{tail}`")
            break
    total = "".join(out)
    time.sleep(0.5) # Pause for half a second
    # Join WITHOUT adding extra spaces (we already preserved originals)
    return total
# %%
