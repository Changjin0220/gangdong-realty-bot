import requests
import os
import json
from datetime import datetime, timedelta
from collections import defaultdict

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
MOLIT_API_KEY = os.environ.get("MOLIT_API_KEY")

DONG_NAMES = {
    "길동": "11740", "둔촌동": "11740", "암사동": "11740",
    "성내동": "11740", "천호동": "11740", "강일동": "11740",
    "상일동": "11740", "명일동": "11740", "고덕동": "11740",
}

def get_recent_months(n=3):
    months = []
    now = datetime.now()
    for i in range(n):
        d = now - timedelta(days=30 * i)
        months.append(d.strftime("%Y%m"))
    return months

def fetch_trade_data(lawdcd, yearmonth):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {"serviceKey": MOLIT_API_KEY, "pageNo": 1, "numOfRows": 1000,
              "LAWD_CD": lawdcd, "DEAL_YMD": yearmonth, "_type": "json"}
    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        items = data["response"]["body"]["items"]
        if not items:
            return []
        item = items["item"]
        return item if isinstance(item, list) else [item]
    except Exception as e:
        print(f"매매 API 오류: {e}")
        return []

def fetch_jeonse_data(lawdcd, yearmonth):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    params = {"serviceKey": MOLIT_API_KEY, "pageNo": 1, "numOfRows": 1000,
              "LAWD_CD": lawdcd, "DEAL_YMD": yearmonth, "_type": "json"}
    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        items = data["response"]["body"]["items"]
        if not items:
            return []
        item = items["item"]
        return item if isinstance(item, list) else [item]
    except Exception as e:
        print(f"전세 API 오류: {e}")
        return []

def make_trade_key(t):
    return f"{t.get('aptNm','')}_{t.get('dealYear','')}_{t.get('dealMonth','')}_{t.get('dealDay','')}_{t.get('dealAmount','')}_{t.get('excluUseAr','')}"

def make_jeonse_key(t):
    return f"{t.get('aptNm','')}_{t.get('year','')}_{t.get('month','')}_{t.get('day','')}_{t.get('deposit','')}_{t.get('excluUseAr','')}_{t.get('monthlyRent','0')}"

def get_pyeong(area_str):
    try:
        area = float(area_str)
        pyeong = int(area / 3.3)
        return f"{(pyeong // 10) * 10}평대"
    except:
        return "기타"

def get_price(t):
    try:
        return int(str(t.get("dealAmount", "0")).replace(",", ""))
    except:
        return 0

def get_deposit(t):
    try:
        return int(str(t.get("deposit", "0")).replace(",", ""))
    except:
        return 0

def format_won(amount):
    if amount >= 10000:
        uk = amount // 10000
        man = amount % 10000
        if man == 0:
            return f"{uk}억"
        return f"{uk}억 {man:,}만"
    return f"{amount:,}만"

def find_prev_trade(apt_name, pyeong, current_deal_date, all_trades):
    candidates = []
    for t in all_trades:
        if t.get("aptNm", "").strip() != apt_name:
            continue
        if get_pyeong(t.get("excluUseAr", "0")) != pyeong:
            continue
        try:
            t_date = f"{t.get('dealYear','')}{str(t.get('dealMonth','')).zfill(2)}{str(t.get('dealDay','')).zfill(2)}"
            if t_date < current_deal_date:
                candidates.append((t_date, t))
        except:
            pass
    if not candidates:
        return None
    return sorted(candidates, reverse=True)[0][1]

def find_prev_jeonse(apt_name, pyeong, contract_type, current_date, all_jeonse):
    candidates = []
    for t in all_jeonse:
        if t.get("aptNm", "").strip() != apt_name:
            continue
        if get_pyeong(t.get("excluUseAr", "0")) != pyeong:
            continue
        t_type = "월세" if t.get("monthlyRent") and str(t.get("monthlyRent","0")).strip() not in ["","0"] else "전세"
        if t_type != contract_type:
            continue
        try:
            t_date = f"{t.get('year','')}{str(t.get('month','')).zfill(2)}{str(t.get('day','')).zfill(2)}"
            if t_date < current_date:
                candidates.append((t_date, t))
        except:
            pass
    if not candidates:
        return None
    return sorted(candidates, reverse=True)[0][1]

def calc_change(current, prev):
    diff = current - prev
    rate = round((diff / prev) * 100, 1) if prev else 0
    arrow = "📈" if diff > 0 else ("📉" if diff < 0 else "➡️")
    sign = "+" if diff >= 0 else ""
    return diff, rate, arrow, sign

def get_apt_stats(trades):
    size_groups = defaultdict(list)
    for t in trades:
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        price = get_price(t)
        if price > 0:
            size_groups[pyeong].append(price)
    return {g: {"min": min(p), "max": max(p), "count": len(p)} for g, p in size_groups.items()}

def get_jeonse_stats(trades):
    size_groups = defaultdict(list)
    for t in trades:
        t_type = "월세" if t.get("monthlyRent") and str(t.get("monthlyRent","0")).strip() not in ["","0"] else "전세"
        if t_type != "전세":
            continue
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        dep = get_deposit(t)
        if dep > 0:
            size_groups[pyeong].append(dep)
    return {g: {"min": min(p), "max": max(p), "count": len(p)} for g, p in size_groups.items()}

def calc_jeonse_rate(매매stats, 전세stats):
    rates = {}
    for group in 매매stats:
        if group in 전세stats:
            avg_매매 = (매매stats[group]["min"] + 매매stats[group]["max"]) / 2
            avg_전세 = (전세stats[group]["min"] + 전세stats[group]["max"]) / 2
            if avg_매매 > 0:
                rates[group] = round((avg_전세 / avg_매매) * 100, 1)
    return rates

def load_history():
    try:
        with open("history.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def get_yesterday_keys(history, dong_name, data_type):
    if not history:
        return set()
    last = history[-1]
    for dong in last.get("data", []):
        if dong.get("dong") == dong_name:
            return set(dong.get(data_type + "_keys", []))
    return set()

def analyze_data(all_trades):
    apt_trades = defaultdict(list)
    for t in all_trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_trades[name].append(t)
    if not apt_trades:
        return None
    top3_count = sorted(apt_trades.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    top3_price = sorted(apt_trades.items(), key=lambda x: max([get_price(t) for t in x[1]], default=0), reverse=True)[:3]
    return {"top3_count": top3_count, "top3_price": top3_price, "apt_trades": apt_trades}

def build_dong_data(dong_name, trades, jeonse_trades, analysis, trade_keys, jeonse_keys):
    if not analysis:
        return None
    apt_jeonse = defaultdict(list)
    for t in jeonse_trades:
        apt_jeonse[t.get("aptNm","").strip()].append(t)

    def build_apt(name, apt_list):
        매매stats = get_apt_stats(apt_list)
        전세stats = get_jeonse_stats(apt_jeonse.get(name, []))
        prices = [get_price(t) for t in apt_list]
        return {
            "name": name, "count": len(apt_list),
            "max_price": max(prices) if prices else 0,
            "stats": 매매stats, "jeonse_stats": 전세stats,
            "jeonse_rate": calc_jeonse_rate(매매stats, 전세stats)
        }

    return {
        "dong": dong_name,
        "total_trades": len(trades),
        "total_jeonse": len(jeonse_trades),
        "top3_count": [build_apt(n, l) for n, l in analysis["top3_count"]],
        "top3_price": [build_apt(n, l) for n, l in analysis["top3_price"]],
        "trade_keys": list(trade_keys),
        "jeonse_keys": list(jeonse_keys)
    }

def build_trade_message(today_str, top3_dongs, all_dong_trades, all_dong_jeonse, all_trade_keys, prev_keys_map):
    """메시지 1: 매매 신규 신고 TOP3"""
    # 전체 신규 매매 수집
    all_new_trades = []
    new_count_by_dong = {}
    for dong_name, _ in top3_dongs:
        prev = prev_keys_map.get(dong_name, {}).get("trade", set())
        new = [t for t in all_dong_trades[dong_name] if make_trade_key(t) not in prev]
        new_count_by_dong[dong_name] = len(new)
        for t in new:
            t["_dong"] = dong_name
            all_new_trades.append(t)

    # 가격 TOP3 추출
    top3 = sorted(all_new_trades, key=lambda t: get_price(t), reverse=True)[:3]

    lines = [f"🏙️ 강동구 부동산 일일 리포트 - 매매"]
    lines.append(f"📅 {today_str} 오전 8시\n")

    # 요약
    lines.append("📊 어제 신규 신고 현황 (매매)")
    for dong_name, _ in top3_dongs:
        lines.append(f"  {dong_name}: +{new_count_by_dong[dong_name]}건")

    if not all_new_trades:
        lines.append("\n어제 신규 신고된 매매 거래가 없습니다.")
        return "\n".join(lines)

    lines.append(f"\n💰 신규 신고 중 최고가 TOP3")
    lines.append("═" * 30)

    for i, t in enumerate(top3, 1):
        apt_name = t.get("aptNm", "").strip()
        dong = t.get("_dong", "")
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        price = get_price(t)
        floor = t.get("floor", "")
        deal_year = t.get("dealYear", "")
        deal_month = str(t.get("dealMonth", "")).zfill(2)
        deal_day = str(t.get("dealDay", "")).zfill(2)
        deal_date = f"{deal_year}{deal_month}{deal_day}"

        lines.append(f"\n{i}위 🏢 {apt_name} ({dong})")
        lines.append(f"   {pyeong} {floor}층 | 계약일: {deal_year}.{deal_month}.{deal_day}")
        lines.append(f"   💰 매매가: {format_won(price)}원")

        # 직전 거래 비교
        dong_trades = all_dong_trades[dong]
        prev = find_prev_trade(apt_name, pyeong, deal_date, dong_trades)
        if prev:
            prev_price = get_price(prev)
            prev_m = str(prev.get("dealMonth","")).zfill(2)
            diff, rate, arrow, sign = calc_change(price, prev_price)
            lines.append(f"   {arrow} 직전거래({prev.get('dealYear','')}.{prev_m}) 대비: {sign}{format_won(abs(diff))}원 ({sign}{rate}%)")
        else:
            lines.append(f"   ℹ️ 직전 동일평형 거래 없음")

        # 전세가율
        apt_jeonse = [j for j in all_dong_jeonse[dong] if j.get("aptNm","").strip() == apt_name]
        t_stat = get_apt_stats([t])
        j_stat = get_jeonse_stats(apt_jeonse)
        rates = calc_jeonse_rate(t_stat, j_stat)
        if rates:
            lines.append(f"   📊 전세가율: {list(rates.values())[0]}%")

    return "\n".join(lines)

def build_jeonse_message(today_str, top3_dongs, all_dong_trades, all_dong_jeonse, prev_keys_map):
    """메시지 2: 전월세 신규 신고 TOP3"""
    all_new_jeonse = []
    new_count_by_dong = {}
    for dong_name, _ in top3_dongs:
        prev = prev_keys_map.get(dong_name, {}).get("jeonse", set())
        new = [t for t in all_dong_jeonse[dong_name] if make_jeonse_key(t) not in prev]
        new_count_by_dong[dong_name] = len(new)
        for t in new:
            t["_dong"] = dong_name
            all_new_jeonse.append(t)

    top3 = sorted(all_new_jeonse, key=lambda t: get_deposit(t), reverse=True)[:3]

    lines = [f"🏙️ 강동구 부동산 일일 리포트 - 전월세"]
    lines.append(f"📅 {today_str} 오전 8시\n")

    lines.append("📊 어제 신규 신고 현황 (전월세)")
    for dong_name, _ in top3_dongs:
        lines.append(f"  {dong_name}: +{new_count_by_dong[dong_name]}건")

    if not all_new_jeonse:
        lines.append("\n어제 신규 신고된 전월세 거래가 없습니다.")
        return "\n".join(lines)

    lines.append(f"\n🔑 신규 신고 중 최고 보증금 TOP3")
    lines.append("═" * 30)

    for i, t in enumerate(top3, 1):
        apt_name = t.get("aptNm", "").strip()
        dong = t.get("_dong", "")
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        deposit = get_deposit(t)
        monthly = t.get("monthlyRent", "0")
        contract_type = "월세" if monthly and str(monthly).strip() not in ["","0"] else "전세"
        year = t.get("year", "")
        month = str(t.get("month", "")).zfill(2)
        day = str(t.get("day", "")).zfill(2)
        current_date = f"{year}{month}{day}"

        lines.append(f"\n{i}위 🏢 {apt_name} ({dong}) [{contract_type}]")
        lines.append(f"   {pyeong} | 계약일: {year}.{month}.{day}")

        if contract_type == "전세":
            lines.append(f"   💰 전세보증금: {format_won(deposit)}원")
            prev = find_prev_jeonse(apt_name, pyeong, "전세", current_date, all_dong_jeonse[dong])
            if prev:
                prev_dep = get_deposit(prev)
                prev_m = str(prev.get("month","")).zfill(2)
                diff, rate, arrow, sign = calc_change(deposit, prev_dep)
                lines.append(f"   {arrow} 직전전세({prev.get('year','')}.{prev_m}) 대비: {sign}{format_won(abs(diff))}원 ({sign}{rate}%)")
            else:
                lines.append(f"   ℹ️ 직전 동일평형 전세 없음")

            # 전세가율
            apt_trades = [tr for tr in all_dong_trades[dong] if tr.get("aptNm","").strip() == apt_name]
            t_stat = get_apt_stats(apt_trades)
            j_stat = {pyeong: {"min": deposit, "max": deposit, "count": 1}}
            rates = calc_jeonse_rate(t_stat, j_stat)
            if rates:
                lines.append(f"   📊 전세가율: {list(rates.values())[0]}%")
        else:
            try:
                monthly_int = int(str(monthly).replace(",",""))
            except:
                monthly_int = 0
            lines.append(f"   💰 보증금: {format_won(deposit)}원 / 월세: {format_won(monthly_int)}원")
            prev = find_prev_jeonse(apt_name, pyeong, "월세", current_date, all_dong_jeonse[dong])
            if prev:
                prev_dep = get_deposit(prev)
                try:
                    prev_mon = int(str(prev.get("monthlyRent","0")).replace(",",""))
                except:
                    prev_mon = 0
                prev_m = str(prev.get("month","")).zfill(2)
                diff_d, rate_d, arrow_d, sign_d = calc_change(deposit, prev_dep)
                diff_m, rate_m, arrow_m, sign_m = calc_change(monthly_int, prev_mon)
                lines.append(f"   {arrow_d} 직전월세({prev.get('year','')}.{prev_m}) 대비:")
                lines.append(f"      보증금 {sign_d}{format_won(abs(diff_d))}원 ({sign_d}{rate_d}%)")
                lines.append(f"      월세 {sign_m}{format_won(abs(diff_m))}원 ({sign_m}{rate_m}%)")
            else:
                lines.append(f"   ℹ️ 직전 동일평형 월세 없음")

    return "\n".join(lines)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        res = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk})
        print(f"텔레그램 전송: {res.status_code}")

def update_history(dong_data_list):
    today = datetime.now().strftime("%Y-%m-%d")
    history = load_history()
    history.append({"date": today, "data": dong_data_list})
    history = history[-90:]
    with open("history.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print("history.json 저장 완료")

def main():
    print("강동구 부동산 알리미 시작...")
    months = get_recent_months(3)
    history = load_history()
    today_str = datetime.now().strftime("%Y.%m.%d")

    dong_totals = {}
    all_dong_trades = {}
    all_dong_jeonse = {}
    all_trade_keys = {}
    all_jeonse_keys = {}

    for dong_name, lawd_cd in DONG_NAMES.items():
        trades, jeonse = [], []
        for month in months:
            trades += fetch_trade_data(lawd_cd, month)
            jeonse += fetch_jeonse_data(lawd_cd, month)

        dong_trades = [t for t in trades if dong_name in t.get("umdNm", "")]
        dong_jeonse = [t for t in jeonse if dong_name in t.get("umdNm", "")]

        dong_totals[dong_name] = len(dong_trades)
        all_dong_trades[dong_name] = dong_trades
        all_dong_jeonse[dong_name] = dong_jeonse
        all_trade_keys[dong_name] = set(make_trade_key(t) for t in dong_trades)
        all_jeonse_keys[dong_name] = set(make_jeonse_key(t) for t in dong_jeonse)
        print(f"{dong_name}: 매매 {len(dong_trades)}건 / 전세 {len(dong_jeonse)}건")

    top3_dongs = sorted(dong_totals.items(), key=lambda x: x[1], reverse=True)[:3]

    # 어제 키 맵
    prev_keys_map = {}
    for dong_name, _ in top3_dongs:
        prev_keys_map[dong_name] = {
            "trade": get_yesterday_keys(history, dong_name, "trade"),
            "jeonse": get_yesterday_keys(history, dong_name, "jeonse")
        }

    # 메시지 1: 매매
    msg1 = build_trade_message(today_str, top3_dongs, all_dong_trades, all_dong_jeonse, all_trade_keys, prev_keys_map)
    print(msg1)
    send_telegram(msg1)

    # 메시지 2: 전월세
    msg2 = build_jeonse_message(today_str, top3_dongs, all_dong_trades, all_dong_jeonse, prev_keys_map)
    print(msg2)
    send_telegram(msg2)

    # 대시보드용 data.json 저장
    dong_data_list = []
    for dong_name, _ in top3_dongs:
        analysis = analyze_data(all_dong_trades[dong_name])
        data = build_dong_data(
            dong_name, all_dong_trades[dong_name], all_dong_jeonse[dong_name],
            analysis, all_trade_keys[dong_name], all_jeonse_keys[dong_name]
        )
        if data:
            dong_data_list.append(data)

    output = {
        "updated_at": datetime.now().strftime("%Y년 %m월 %d일 %H:%M"),
        "top3_dongs": [d["dong"] for d in dong_data_list],
        "data": dong_data_list
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("data.json 저장 완료")

    update_history(dong_data_list)
    print("완료!")

if __name__ == "__main__":
    main()
