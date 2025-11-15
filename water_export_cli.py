import re
import requests
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from dateutil import tz
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# =========================
# PATH & TIMEZONE CONFIG
# =========================

TZ_LOCAL = tz.gettz("Asia/Ho_Chi_Minh")  # GMT+7

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)
OUT_CSV = OUT_DIR / "water_data_full_combined.csv"

# ---- Determine fetch window based on existing CSV ----
DEFAULT_END_DATE = datetime.now(TZ_LOCAL).date()

if OUT_CSV.exists():
    try:
        df_old_meta = pd.read_csv(OUT_CSV, encoding="utf-8-sig", usecols=["Thời gian (UTC)"])
        last_dt = pd.to_datetime(df_old_meta["Thời gian (UTC)"]).max()
        if pd.isna(last_dt):
            raise ValueError("No valid timestamp in existing CSV")
        last_date = last_dt.date()
        # tránh trường hợp clock lệch
        if last_date > DEFAULT_END_DATE:
            DEFAULT_START_DATE = DEFAULT_END_DATE - timedelta(days=6)
        else:
            DEFAULT_START_DATE = last_date
        print(f"🔁 Existing CSV found. Fetching data from {DEFAULT_START_DATE} to {DEFAULT_END_DATE} (GMT+7).")
    except Exception as e:
        print(f"⚠️ Could not infer start date from existing CSV, fallback to 7 days. Reason: {e}")
        DEFAULT_START_DATE = DEFAULT_END_DATE - timedelta(days=6)
else:
    # first run: 7 days history
    DEFAULT_START_DATE = DEFAULT_END_DATE - timedelta(days=6)
    print(f"🆕 No existing CSV. Fetching 7 days from {DEFAULT_START_DATE} to {DEFAULT_END_DATE} (GMT+7).")

TIMEOUT_SECONDS = 60

RIVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://vndms.dmptc.gov.vn/",
    "Origin": "https://vndms.dmptc.gov.vn",
}

# ---- Lakes: ID + recoded basin ----
LAKE_CONFIG = {
    "467D6521-FEAE-40F3-BC73-8E4B0B1F598F": {"name": "Pleikrông",      "basin_recode": "Sê San"},
    "53e42d94-1faa-4029-93f0-739f8f5da487": {"name": "SeSan4",         "basin_recode": "Sê San"},
    "A11984FB-8CAD-44D7-BF9E-A0E881483E47": {"name": "Hương Điền",     "basin_recode": "Hương - Bồ"},
    "EE42CA6E-E5FC-4A9F-B90C-040211672E1B": {"name": "Sông Tranh 2",   "basin_recode": "Vu Gia - Thu Bồn"},
    "545B2C88-D719-42F1-8663-A1E796F44C14": {"name": "Ialy",           "basin_recode": "Sê San"},
    "A72755CC-49FE-44AF-827D-7010EB7EBCB4": {"name": "Sông Bung 4",    "basin_recode": "Vu Gia - Thu Bồn"},
    "1D320527-2DC9-4C79-A00B-EBB16D44F735": {"name": "Bình Điền",      "basin_recode": "Hương - Bồ"},
    "fd622826-9f2e-4130-8995-1654bac81895": {"name": "Tả Trạch",       "basin_recode": "Hương - Bồ"},
    "D0C28BB9-FE47-4BC2-B0DB-445038C1D1C5": {"name": "Sông Hinh",      "basin_recode": "Ba"},
    "7D5B7DB0-D64A-4A36-BD4E-54A95CA62E9D": {"name": "Sông Ba Hạ",     "basin_recode": "Ba"},
    "72659CC3-2BB5-4E34-810E-96722BCE0F54": {"name": "A Vương",        "basin_recode": "Vu Gia - Thu Bồn"},
    "4AB3F3C8-D7F4-44AA-897C-E93BDCFA1DCC": {"name": "Kanak",          "basin_recode": "Ba"},
    "929f34bb-4d88-4364-8882-4099e75bcfd5": {"name": "Nước Trong",     "basin_recode": "Trà Khúc"},
    "9CBE33CD-5CFB-4CB9-BAEB-59147A825DF0": {"name": "Ayun Hạ",        "basin_recode": "Ba"},
    "c9a8c4ca-f1bb-467f-82c4-0999294af8fc": {"name": "Định Bình",      "basin_recode": "Kôn - Hà Thanh"},
    "4006E5A9-4E5A-4A46-AC19-35F1233E6B4A": {"name": "Thượng Kon Tum", "basin_recode": "Sê San"},
    "73bb8be6-bbd6-4042-8360-30abdced336a": {"name": "Ia MLá",         "basin_recode": "Ba"},
    "9BFF6E76-94E2-4233-B659-258D74A1295F": {"name": "Trà Xom",        "basin_recode": "Kôn - Hà Thanh"},
    "062A7CF0-46F3-4E99-8BCD-040CEF304344": {"name": "Thuận Ninh",     "basin_recode": "Kôn - Hà Thanh"},
}
LAKE_IDS = set(LAKE_CONFIG.keys())

# ---- Stations: ID + recoded basin ONLY (no river/province recode) ----
STATION_CONFIG = {
    "69702": {"name": "Kon Tum",   "basin_recode": "Sê San"},
    "69704": {"name": "Kon Plông", "basin_recode": "Sê San"},
    "71518": {"name": "Phú Ốc",    "basin_recode": "Hương - Bồ"},
    "71520": {"name": "Kim Long",  "basin_recode": "Hương - Bồ"},
    "71527": {"name": "Ái Nghĩa",  "basin_recode": "Vu Gia - Thu Bồn"},
    "71533": {"name": "Hội Khách", "basin_recode": "Vu Gia - Thu Bồn"},
    "71540": {"name": "Trà Khúc",  "basin_recode": "Trà Khúc"},
    "71549": {"name": "Bình Nghi", "basin_recode": "Kôn - Hà Thanh"},
    "71558": {"name": "Củng Sơn",  "basin_recode": "Ba"},
    "71559": {"name": "Phú Lâm",   "basin_recode": "Ba"},
    "71708": {"name": "An Khê",    "basin_recode": "Ba"},
    "71709": {"name": "AyunPa",    "basin_recode": "Ba"},
}
STATION_IDS = set(STATION_CONFIG.keys())

# =========================
# HELPERS
# =========================

def get_robust_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_first_num(d, key):
    v = d.get(key)
    if not v:
        return None
    parts = str(v).split(",")
    try:
        val = float(parts[0])
        return val if val > 0 else None
    except:
        return None

def classify_exceed(level, bd1, bd2, bd3, hist):
    if level is None:
        return 0
    if hist and level >= hist:
        return 4
    if bd3 and level >= bd3:
        return 3
    if bd2 and level >= bd2:
        return 2
    if bd1 and level >= bd1:
        return 1
    return 0

def alert_name_from_value(v):
    return {
        0: "Dưới BĐ1",
        1: "Trên BĐ1",
        2: "Trên BĐ2",
        3: "Trên BĐ3",
        4: "Trên lũ lịch sử",
    }.get(v, "Không xác định")

def calculate_alert_diff(level, val, bd1, bd2, bd3, hist):
    if val is None:
        return None
    try:
        if level == 4:
            diff = val - hist
        elif level == 3:
            diff = val - bd3
        elif level == 2:
            diff = val - bd2
        elif level == 1:
            diff = val - bd1
        elif bd1:
            diff = val - bd1
        else:
            return None
        return round(diff, 2)
    except:
        return None

def parse_river_dt(lbl, year):
    """
    VNDMS labels are already GMT+7.
    Example: '7h00/12' => 07:00, day=12, current month/year.
    """
    try:
        clean = lbl.strip().replace("\n", "")
        m = re.match(r"(\d{1,2})h(\d{1,2})/(\d{1,2})", clean)
        if m:
            hour = int(m.group(1))
            minute = int(m.group(2))
            day = int(m.group(3))
            now_local = datetime.now(TZ_LOCAL)
            month = now_local.month
            dt = datetime(year, month, day, hour, minute)
            if TZ_LOCAL:
                return dt.replace(tzinfo=TZ_LOCAL)
            return dt
    except:
        pass
    return None

def ms_to_dt_local(ms_str):
    """
    Thuyloivietnam returns epoch milliseconds (UTC).
    Convert UTC -> GMT+7.
    """
    try:
        ms = int(re.search(r"\d+", str(ms_str)).group())
        ts = ms / 1000.0
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        if TZ_LOCAL:
            return dt_utc.astimezone(TZ_LOCAL)
        return dt_utc
    except:
        return None

# =========================
# SCRAPERS
# =========================

def scrape_river_stations_list():
    """
    Fetch all stations from vndms, keep only IDs in STATION_IDS.
    """
    url = "https://vndms.dmptc.gov.vn/water_level"
    session = get_robust_session()
    session.headers.update(RIVER_HEADERS)
    stations = {}

    for lv in ["0", "1", "2", "3"]:
        try:
            r = session.get(url, params={"lv": lv}, timeout=TIMEOUT_SECONDS)
            data = r.json()
            for f in data.get("features", []):
                props = f.get("properties", {})
                popup = props.get("popupInfo", "")

                m_id = re.search(r"Mã trạm:\s*<b>(\d+)</b>", popup, re.IGNORECASE)
                sid = m_id.group(1) if m_id else None
                if not sid:
                    continue
                if sid not in STATION_IDS:
                    continue

                river = "Unknown"
                m_riv = re.search(r"Sông:\s*<b>(.*?)</b>", popup, re.IGNORECASE)
                if m_riv:
                    river = m_riv.group(1).strip()

                x, y = None, None
                geom = f.get("geometry", {})
                if geom and geom.get("type") == "Point":
                    coords = geom.get("coordinates")
                    if coords and len(coords) == 2:
                        x, y = coords[0], coords[1]

                stations[sid] = {
                    "river": river,
                    "name": props.get("label", f"Station {sid}"),
                    "x": x,
                    "y": y,
                }
        except:
            pass

    return stations

def scan_lakes_via_api():
    """
    Scan thuyloivietnam API from DEFAULT_START_DATE to DEFAULT_END_DATE,
    keep only lakes with IDs in LAKE_IDS.
    """
    url = "http://e15.thuyloivietnam.vn/CanhBaoSoLieu/ATCBDTHo"
    session = get_robust_session()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://e15.thuyloivietnam.vn/",
    }

    results = []
    curr = DEFAULT_START_DATE
    while curr <= DEFAULT_END_DATE:
        try:
            time_param = f"{curr.strftime('%Y-%m-%d')} 00:00:00,000"
            r = session.post(
                url,
                data={"time": time_param, "ishothuydien": "0"},
                headers=headers,
                timeout=TIMEOUT_SECONDS,
            )
            if r.status_code == 200:
                for rec in r.json():
                    lc = rec.get("LakeCode")
                    if lc in LAKE_IDS:
                        results.append(rec)
        except:
            pass
        curr += timedelta(days=1)

    return results

# =========================
# MAIN LOGIC
# =========================

def main():
    print("📡 Scanning selected river stations & lakes...")

    cached_river_stations = scrape_river_stations_list()
    lake_data = scan_lakes_via_api()

    final_rows = []

    # ----------- RIVERS -----------
    session = get_robust_session()
    session.headers.update(RIVER_HEADERS)
    curr_year = datetime.now(TZ_LOCAL).year

    print(f"🌊 Fetching {len(cached_river_stations)} river stations...")
    for sid, meta in tqdm(cached_river_stations.items(), desc="Rivers"):
        try:
            payload = {
                "id": sid,
                "timeSelect": "7",  # API trả 7 ngày; mình lọc lại bằng DEFAULT_START_DATE
                "source": "Water",
                "fromDate": "",
                "toDate": "",
            }
            r = session.post(
                "https://vndms.dmc.gov.vn/home/detailRain",
                data=payload,
                headers=RIVER_HEADERS,
                timeout=15,
            )
            if r.status_code != 200:
                continue
            d = r.json()

            bd1 = get_first_num(d, "bao_dong1")
            bd2 = get_first_num(d, "bao_dong2")
            bd3 = get_first_num(d, "bao_dong3")
            hist_m = get_first_num(d, "gia_tri_lu_lich_su")
            hist_y = d.get("nam_lu_lich_su")

            labels = (d.get("labels") or "").split(",")
            raw_vals = str(d.get("value", "")).split(",")
            values = [
                float(x.strip())
                if x.strip() not in ["-", "null", "", "NULL"]
                else None
                for x in raw_vals
            ]

            config = STATION_CONFIG.get(sid, {})
            recoded_basin = config.get(
                "basin_recode", d.get("river_name", meta["river"])
            )
            recoded_name = config.get("name", d.get("name_vn", meta["name"]))
            province = d.get("province_name", "")  # không recode, lấy từ API

            for i in range(min(len(labels), len(values))):
                dt_local = parse_river_dt(labels[i], curr_year)
                if not dt_local:
                    continue
                if dt_local.date() < DEFAULT_START_DATE:
                    continue

                val = values[i]
                alert_val = classify_exceed(val, bd1, bd2, bd3, hist_m)
                alert_name = alert_name_from_value(alert_val)
                alert_diff = calculate_alert_diff(
                    alert_val, val, bd1, bd2, bd3, hist_m
                )

                final_rows.append(
                    {
                        "type": "River",
                        "id": sid,
                        "name": recoded_name,
                        "basin": recoded_basin,
                        "province": province,
                        "timestamp_utc": dt_local.strftime("%Y-%m-%d %H:%M"),  # GMT+7
                        "water_level_m": val,
                        "alert_status": alert_name,
                        "alert_value": alert_val,
                        "alert_diff_m": alert_diff,
                        "bd1_m": bd1,
                        "bd2_m": bd2,
                        "bd3_m": bd3,
                        "hist_flood_level_m": hist_m,
                        "hist_flood_year": hist_y,
                        "x": meta.get("x"),
                        "y": meta.get("y"),
                    }
                )
        except Exception as e:
            print(f"  > Skipping station {sid}. Error: {e}")

    # ----------- LAKES -----------
    print("💧 Processing lake data...")
    for rec in lake_data:
        lc = rec.get("LakeCode")
        if lc not in LAKE_CONFIG:
            continue

        conf = LAKE_CONFIG[lc]
        recoded_basin = conf["basin_recode"]
        recoded_name = conf["name"]

        dt_local = ms_to_dt_local(rec.get("ThoiGianCapNhat"))

        # lọc theo DEFAULT_START_DATE giống river
        if dt_local and dt_local.date() < DEFAULT_START_DATE:
            continue

        final_rows.append(
            {
                "type": "Lake",
                "id": lc,
                "name": recoded_name,
                "basin": recoded_basin,
                "province": rec.get("ProvinceName"),
                "timestamp_utc": dt_local.strftime("%Y-%m-%d %H:%M") if dt_local else None,
                "water_level_m": rec.get("TdMucNuoc"),
                "alert_status": rec.get("XuThe", "N/A"),
                "water_volume_m3": rec.get("TdDungTich"),
                "design_volume_m3": rec.get("TkDungTich"),
                "volume_percent": rec.get("TiLeDungTichTdSoTk"),
                "inflow_m3s": rec.get("QDen"),
                "outflow_m3s": rec.get("QXa"),
                "normal_level_m": rec.get("MucNuocDangBinhThuong"),
                "reinforced_level_m": rec.get("MucNuocDangGiaCuong"),
                "alert_code": rec.get("MucCanhBao"),
                "province_code": rec.get("ProvinceCode"),
                "basin_code": rec.get("BasinCode"),
                "x": rec.get("X"),
                "y": rec.get("Y"),
            }
        )

    # ----------- EXPORT + INCREMENTAL MERGE -----------
    if not final_rows:
        print("❌ No data collected for current window.")
        return

    df = pd.DataFrame(final_rows)
    df = df.sort_values(["type", "basin", "name", "timestamp_utc"])

    df = df.rename(
        columns={
            "id": "Mã trạm/LakeCode",
            "name": "Trạm/Hồ",
            "basin": "Tên sông/Lưu vực",
            "province": "Tên tỉnh",
            # Stored as GMT+7, keep name for compatibility
            "timestamp_utc": "Thời gian (UTC)",
            "water_level_m": "Mực nước (m)",
            "alert_status": "Cảnh báo/Xu thế",
            "alert_value": "Cảnh báo value (0-4)",
            "alert_diff_m": "Chênh lệch cảnh báo (m)",
            "bd1_m": "BĐ1 (m)",
            "bd2_m": "BĐ2 (m)",
            "bd3_m": "BĐ3 (m)",
            "hist_flood_level_m": "Mực nước lịch sử (m)",
            "hist_flood_year": "Năm lũ lịch sử",
            "water_volume_m3": "Dung tích (m3)",
            "design_volume_m3": "Dung tích TK (m3)",
            "volume_percent": "Tỷ lệ dung tích (%)",
            "inflow_m3s": "Q đến (m3/s)",
            "outflow_m3s": "Q xả (m3/s)",
            "normal_level_m": "Mực nước BT (m)",
            "reinforced_level_m": "Mực nước GC (m)",
            "alert_code": "Mã Cảnh báo",
        }
    )

    if OUT_CSV.exists():
        try:
            df_old = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
            combined = pd.concat([df_old, df], ignore_index=True)

            combined = combined.drop_duplicates(
                subset=["type", "Mã trạm/LakeCode", "Thời gian (UTC)"],
                keep="last",
            )

            combined = combined.sort_values(
                ["type", "Tên sông/Lưu vực", "Trạm/Hồ", "Thời gian (UTC)"]
            )

            combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            print(f"✅ Appended & deduplicated. Now {len(combined)} rows in {OUT_CSV}")
        except Exception as e:
            print(f"⚠️ Error merging with existing CSV, overwriting file. Reason: {e}")
            df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
            print(f"✅ Saved {len(df)} rows to {OUT_CSV}")
    else:
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ Saved {len(df)} rows to {OUT_CSV}")

    print("⏱ Note: 'Thời gian (UTC)' currently stores timestamps in GMT+7 (Asia/Ho_Chi_Minh).")

if __name__ == "__main__":
    main()