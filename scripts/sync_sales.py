#!/usr/bin/env python3
"""
식물행성 대시보드 매출 동기화 스크립트 (v3)
- 01_송장비서/A_order list/YYMMDD/* 4채널 일일 주문 파일을 파싱해 sales.json 생성
- "수집일(collection date)" 기준 집계: 전일 06:00 ~ 당일 05:59 윈도우 (+18h shift)
- 주문 ID 기준 dedup (품목 행 단위)
- 배송건수는 주문/묶음 단위 ID로 count (품목이 아닌 실제 배송 단위)
- 스마트스토어/오늘의집 가격은 cafe24/coupang + all_orders_database*.csv 학습 맵으로 추정
"""
import os, sys, csv, json, glob, re, unicodedata
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import openpyxl

KST = timezone(timedelta(hours=9))
CHANNELS = ["스마트스토어", "쿠팡", "카페24", "오늘의집"]

# 송장비서 수집 윈도우: 전일 06:00 ~ 당일 05:59 → +18h shift로 경계를 자정으로 정렬
COLLECTION_SHIFT = timedelta(hours=18)

# --- 경로 찾기 (한글 NFD 대응) ---
def find_work_dir():
    for c in glob.glob("/sessions/*/mnt/*/01*"):
        if "송장비서" in unicodedata.normalize("NFC", c):
            return c
    raise RuntimeError("송장비서 폴더를 찾지 못함")

WORK = find_work_dir()
ORDER_DIR = os.path.join(WORK, "A_order list")

def yymmdd_to_iso(yymmdd):
    return f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"

def list_date_folders():
    if not os.path.isdir(ORDER_DIR):
        return []
    return sorted([n for n in os.listdir(ORDER_DIR) if re.fullmatch(r"\d{6}", n)])

def open_xlsx(path):
    return openpyxl.load_workbook(path, read_only=True, data_only=True)

def rows_dict(ws):
    rows = []
    headers = None
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = [h if h is not None else f"col{k}" for k, h in enumerate(row)]
        else:
            rows.append({headers[k]: v for k, v in enumerate(row) if k < len(headers)})
    return rows

# --- 날짜 파서 & 수집일 계산 ---
def _parse_dt(value):
    """다양한 포맷의 주문일시 → KST datetime"""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=KST)
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(KST)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=KST)
        except Exception:
            continue
    # "2026. 04. 08 23:29:57" (오늘의집)
    m = re.search(r"(\d{4})\.?\s*(\d{1,2})\.?\s*(\d{1,2})(?:[ T](\d{1,2}):(\d{1,2}):?(\d{1,2})?)?", s)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                        int(m.group(4) or 0), int(m.group(5) or 0),
                        int(m.group(6) or 0), tzinfo=KST)
    return None

def collection_date(value, fallback_iso):
    """주문일시 → 송장비서 수집일(YYYY-MM-DD). 전일 06:00 ~ 당일 05:59 윈도우."""
    dt = _parse_dt(value)
    if dt is None:
        return fallback_iso
    return (dt + COLLECTION_SHIFT).date().isoformat()

# --- dedup 글로벌 ---
SEEN_ORDERS = set()
def _once(channel, oid):
    if not oid:
        return True
    key = (channel, str(oid))
    if key in SEEN_ORDERS:
        return False
    SEEN_ORDERS.add(key)
    return True

# --- 상품명 정규화 & 가격 맵 ---
def normalize_pname(name):
    if not name:
        return ""
    s = unicodedata.normalize("NFC", str(name))
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"^식물행성\s+", "", s)
    s = re.sub(r"[-–—].*$", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokens(s):
    return set(t for t in re.split(r"[ \+/,]+", s) if len(t) >= 2)

def lookup_price(price_map, pname):
    key = normalize_pname(pname)
    if not key:
        return None
    if key in price_map:
        return price_map[key]
    for k, v in price_map.items():
        if k and (k in key or key in k):
            return v
    ktok = tokens(key)
    if len(ktok) >= 2:
        best_v, best_s = None, 0
        for k, v in price_map.items():
            inter = ktok & tokens(k)
            if len(inter) >= 2 and len(inter) > best_s:
                best_s = len(inter); best_v = v
        if best_v is not None:
            return best_v
    return None

# --- 채널별 파서 ---
def parse_coupang(folder_path, fallback_iso):
    result = []
    files = glob.glob(os.path.join(folder_path, "coupang_DeliveryList*.xlsx"))
    if not files:
        return result
    wb = open_xlsx(files[0])
    try:
        rows = rows_dict(wb.active)
        for r in rows:
            oid = r.get("주문번호") or r.get("묶음배송번호")
            if not _once("쿠팡", oid):
                continue
            amt = r.get("결제액") or 0
            try:
                amt = int(float(amt))
            except:
                continue
            if not amt:
                continue
            d = collection_date(r.get("주문일"), fallback_iso)
            pn = r.get("등록상품명") or r.get("노출상품명(옵션명)") or ""
            # shipment_key: 묶음배송번호 groups items shipped together
            ship_key = str(r.get("묶음배송번호") or r.get("주문번호") or oid)
            result.append(("쿠팡", d, amt, pn, ship_key))
    finally:
        wb.close()
    return result

def parse_cafe24(folder_path, fallback_iso):
    result = []
    files = glob.glob(os.path.join(folder_path, "cafe24_orders_*.csv"))
    if not files:
        return result
    with open(files[0], encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            oid = row.get("품목별 주문번호") or row.get("주문번호")
            if not _once("카페24", oid):
                continue
            amt_str = (row.get("결제금액") or "0").replace(",", "").strip()
            try:
                amt = int(float(amt_str))
            except:
                amt = 0
            if not amt:
                continue
            d = collection_date(row.get("주문일"), fallback_iso)
            pn = row.get("주문상품명") or ""
            # shipment_key: 주문번호 (not 품목별) = one shipment per order
            ship_key = str(row.get("주문번호") or oid)
            result.append(("카페24", d, amt, pn, ship_key))
    return result

def parse_smartstore_file(folder_path, fallback_iso, price_map):
    result = []
    missing = 0
    total = 0
    files = [f for f in glob.glob(os.path.join(folder_path, "*.xlsx"))
             if "스마트스토어" in unicodedata.normalize("NFC", os.path.basename(f))]
    if not files:
        return result, missing, total
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    wb = open_xlsx(files[0])
    try:
        rows = rows_dict(wb.active)
        for r in rows:
            oid = r.get("상품주문번호") or r.get("주문번호")
            if not _once("스마트스토어", oid):
                continue
            total += 1
            pn = r.get("상품명") or ""
            try:
                qty = int(r.get("수량") or 1)
            except:
                qty = 1
            unit = lookup_price(price_map, pn)
            if unit is None:
                missing += 1
                continue
            d = collection_date(r.get("주문일시"), fallback_iso)
            # shipment_key: 주문번호 (not 상품주문번호) = one shipment per order
            ship_key = str(r.get("주문번호") or oid)
            result.append(("스마트스토어", d, unit * qty, pn, ship_key))
    finally:
        wb.close()
    return result, missing, total

def parse_ohou(folder_path, fallback_iso, price_map):
    result = []
    missing = 0
    total = 0
    files = [f for f in glob.glob(os.path.join(folder_path, "*.xlsx"))
             if "오늘의집" in unicodedata.normalize("NFC", os.path.basename(f))]
    if not files:
        return result, missing, total
    wb = open_xlsx(files[0])
    try:
        rows = rows_dict(wb.active)
        for r in rows:
            oid = r.get("주문옵션번호") or r.get("주문번호")
            if not _once("오늘의집", oid):
                continue
            total += 1
            pn = r.get("상품명") or ""
            try:
                qty = int(r.get("수량") or 1)
            except:
                qty = 1
            unit = lookup_price(price_map, pn)
            if unit is None:
                missing += 1
                continue
            d = collection_date(r.get("주문결제완료일"), fallback_iso)
            # shipment_key: 주문번호 (not 주문옵션번호) = one shipment per order
            ship_key = str(r.get("주문번호") or oid)
            result.append(("오늘의집", d, unit * qty, pn, ship_key))
    finally:
        wb.close()
    return result, missing, total

# --- 메인 ---
def main():
    now = datetime.now(KST)
    # "오늘" 라벨 = 송장비서 수집일 = (now - 6h).date()
    #   06:00 이전이면 전일 윈도우, 이후면 당일 윈도우
    today = (now - timedelta(hours=6)).date()
    today_str = today.isoformat()
    month_str = today.strftime("%Y-%m")
    warnings = []
    folders = list_date_folders()
    if not folders:
        print("no date folders found"); sys.exit(1)

    # 0) 가격 맵 시드 (과거 DB)
    price_map = {}
    for dbname in ("all_orders_database.csv", "all_orders_database_v2.csv"):
        p = os.path.join(WORK, dbname)
        if not os.path.exists(p):
            continue
        try:
            with open(p, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    pn = (row.get("std_product_name") or row.get("상품명")
                          or row.get("주문상품명") or row.get("등록상품명") or "")
                    amt_str = (row.get("최종 상품별 총 주문금액")
                               or row.get("결제액") or row.get("결제금액")
                               or row.get("판매가") or "0")
                    try:
                        amt = int(float(str(amt_str).replace(",", "").strip() or 0))
                    except:
                        amt = 0
                    key = normalize_pname(pn)
                    if key and amt > 0:
                        price_map[key] = amt
        except Exception as e:
            print(f"[seed] {dbname}: {e}")
    print(f"[seed-price-map] {len(price_map)}")

    # 1) coupang + cafe24: 직접 집계 & 가격 맵 보강
    raw_records = []
    for f in folders:
        folder = os.path.join(ORDER_DIR, f)
        fallback = yymmdd_to_iso(f)
        for rec in parse_coupang(folder, fallback):
            key = normalize_pname(rec[3])
            if key:
                price_map[key] = rec[2]
            raw_records.append(rec)
        for rec in parse_cafe24(folder, fallback):
            key = normalize_pname(rec[3])
            if key:
                price_map[key] = rec[2]
            raw_records.append(rec)
    print(f"[price-map] {len(price_map)}")

    # 2) smartstore + ohou: 가격 맵 사용
    ss_miss = ss_tot = oh_miss = oh_tot = 0
    for f in folders:
        folder = os.path.join(ORDER_DIR, f)
        fallback = yymmdd_to_iso(f)
        recs, miss, tot = parse_smartstore_file(folder, fallback, price_map)
        raw_records.extend(recs); ss_miss += miss; ss_tot += tot
        recs, miss, tot = parse_ohou(folder, fallback, price_map)
        raw_records.extend(recs); oh_miss += miss; oh_tot += tot

    if ss_tot:
        warnings.append(f"스마트스토어: 가격 매핑 {ss_tot - ss_miss}/{ss_tot}건 (실패 {ss_miss})")
    if oh_tot:
        warnings.append(f"오늘의집: 가격 매핑 {oh_tot - oh_miss}/{oh_tot}건 (실패 {oh_miss})")

    # --- 집계 ---
    # records are now 5-tuples: (channel, date, amount, product_name, shipment_key)
    # Revenue: sum all line items (correct per-item)
    # Orders/배송건수: count unique (channel, shipment_key) per period (= actual shipments)
    today_rev = sum(r[2] for r in raw_records if r[1] == today_str)
    today_ships = {(r[0], r[4]) for r in raw_records if r[1] == today_str}
    today_cnt = len(today_ships)
    month_rev = sum(r[2] for r in raw_records if r[1].startswith(month_str))
    month_ships = {(r[0], r[4]) for r in raw_records if r[1].startswith(month_str)}
    month_cnt = len(month_ships)

    ch_rev = defaultdict(int); ch_ships = defaultdict(set)
    for ch, d, amt, _, sk in raw_records:
        if d.startswith(month_str):
            ch_rev[ch] += amt
            ch_ships[ch].add(sk)
    ch_cnt = {ch: len(ch_ships.get(ch, set())) for ch in CHANNELS}
    ch_total = sum(ch_rev.values()) or 1
    by_channel = {ch: {"revenue": ch_rev.get(ch, 0),
                       "orders": ch_cnt.get(ch, 0),
                       "share": round(ch_rev.get(ch, 0) / ch_total, 4)}
                  for ch in CHANNELS}

    daily_map = defaultdict(lambda: {"revenue": 0, "shipment_keys": set(),
                                     "by_channel_keys": defaultdict(set)})
    for ch, d, amt, _, sk in raw_records:
        if d.startswith(month_str):
            daily_map[d]["revenue"] += amt
            daily_map[d]["shipment_keys"].add((ch, sk))
            daily_map[d]["by_channel_keys"][ch].add(sk)
    daily = []
    for d in sorted(daily_map.keys()):
        e = daily_map[d]
        daily.append({"date": d, "revenue": e["revenue"],
                      "orders": len(e["shipment_keys"]),
                      "by_channel": {ch: len(keys) for ch, keys in e["by_channel_keys"].items()}})

    channel_deliveries_month = {ch: ch_cnt.get(ch, 0) for ch in CHANNELS}

    out = {
        "generated_at": now.isoformat(),
        "today": {"date": today_str, "revenue": today_rev, "orders": today_cnt},
        "month": {"month": month_str, "revenue": month_rev, "orders": month_cnt},
        "by_channel": by_channel,
        "daily": daily,
        "channel_deliveries_month": channel_deliveries_month,
        "warnings": warnings,
    }

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sales.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[ok] {out_path}")
    print(f"  오늘({today_str}) ₩{today_rev:,} / {today_cnt}건")
    print(f"  이번달({month_str}) ₩{month_rev:,} / {month_cnt}건")
    for ch, v in by_channel.items():
        print(f"  {ch}: ₩{v['revenue']:,} ({v['orders']}건 {v['share']*100:.1f}%)")
    # 오늘 채널별 배송건수 출력
    today_entry = next((x for x in daily if x["date"] == today_str), None)
    if today_entry:
        print(f"  오늘 채널별 배송: {today_entry['by_channel']}")
    for w in warnings:
        print("  ⚠", w)

if __name__ == "__main__":
    main()
