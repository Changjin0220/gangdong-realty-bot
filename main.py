import requests
import os
import json
from datetime import datetime, timedelta
from collections import defaultdict

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
MOLIT_API_KEY = os.environ.get("MOLIT_API_KEY")

DONG_NAMES = {
    "길동": "11740",
    "둔촌동": "11740",
    "암사동": "11740",
    "성내동": "11740",
    "천호동": "11740",
    "강일동": "11740",
    "상일동": "11740",
    "명일동": "11740",
    "고덕동": "11740",
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
    params = {
        "serviceKey": MOLIT_API_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "LAWD_CD": lawdcd,
        "DEAL_YMD": yearmonth,
        "_type": "json"
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        items = data["response"]["body"]["items"]
        if not items:
            return []
        item = items["item"]
        return item if isinstance(item, list) else [item]
    except Exception as e:
        print(f"매매 API 오류 ({lawdcd}, {yearmonth}): {e}")
        return []

def fetch_jeonse_data(lawdcd, yearmonth):
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
    params = {
        "serviceKey": MOLIT_API_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "LAWD_CD": lawdcd,
        "DEAL_YMD": yearmonth,
        "_type": "json"
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        items = data["response"]["body"]["items"]
        if not items:
            return []
        item = items["item"]
        return item if isinstance(item, list) else [item]
    except Exception as e:
        print(f"전세 API 오류 ({lawdcd}, {yearmonth}): {e}")
        return []

def make_trade_key(t):
    """매매 거래 고유키: 아파트명+계약년+계약월+계약일+금액+면적"""
    return f"{t.get('aptNm','')}_{t.get('dealYear','')}_{t.get('dealMonth','')}_{t.get('dealDay','')}_{t.get('dealAmount','')}_{t.get('excluUseAr','')}"

def make_jeonse_key(t):
    """전월세 거래 고유키"""
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

def find_prev_trade(apt_name, pyeong, area, current_deal_date, all_trades):
    """같은 아파트, 같은 평수의 직전 거래 찾기"""
    candidates = []
    for t in all_trades:
        if t.get("aptNm", "").strip() != apt_name:
            continue
        t_pyeong = get_pyeong(t.get("excluUseAr", "0"))
        if t_pyeong != pyeong:
            continue
        try:
            t_date = f"{t.get('dealYear','')}{str(t.get('dealMonth','')).zfill(2)}{str(t.get('dealDay','')).zfill(2)}"
            if t_date < current_deal_date:
                candidates.append((t_date, t))
        except:
            pass
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def find_prev_jeonse(apt_name, pyeong, contract_type, current_date, all_jeonse):
    """같은 아파트, 같은 평수, 같은 유형(전세/월세)의 직전 거래 찾기"""
    candidates = []
    for t in all_jeonse:
        if t.get("aptNm", "").strip() != apt_name:
            continue
        t_pyeong = get_pyeong(t.get("excluUseAr", "0"))
        if t_pyeong != pyeong:
            continue
        t_type = "월세" if t.get("monthlyRent") and str(t.get("monthlyRent", "0")).strip() not in ["", "0"] else "전세"
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
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def calc_change(current, prev):
    diff = current - prev
    rate = round((diff / prev) * 100, 1) if prev else 0
    arrow = "📈" if diff > 0 else ("📉" if diff < 0 else "➡️")
    sign = "+" if diff >= 0 else ""
    return diff, rate, arrow, sign

def format_won(amount):
    """만원 단위를 억/만원으로 표시"""
    if amount >= 10000:
        uk = amount // 10000
        man = amount % 10000
        if man == 0:
            return f"{uk}억"
        return f"{uk}억 {man:,}만"
    return f"{amount:,}만"

def load_history():
    try:
        with open("history.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def get_yesterday_keys(history, dong_name, data_type):
    """어제 저장된 거래 키 목록 반환"""
    if not history:
        return set()
    last = history[-1]
    for dong in last.get("data", []):
        if dong.get("dong") == dong_name:
            return set(dong.get(data_type + "_keys", []))
    return set()

def get_apt_stats_for_dashboard(trades):
    size_groups = defaultdict(list)
    for t in trades:
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        price = get_price(t)
        if price > 0:
            size_groups[pyeong].append(price)
    result = {}
    for group, prices in size_groups.items():
        result[group] = {"min": min(prices), "max": max(prices), "count": len(prices)}
    return result

def get_jeonse_stats_for_dashboard(jeonse_trades):
    size_groups = defaultdict(list)
    for t in jeonse_trades:
        t_type = "월세" if t.get("monthlyRent") and str(t.get("monthlyRent", "0")).strip() not in ["", "0"] else "전세"
        if t_type != "전세":
            continue
        pyeong = get_pyeong(t.get("excluUseAr", "0"))
        deposit = get_deposit(t)
        if deposit > 0:
            size_groups[pyeong].append(deposit)
    result = {}
    for group, prices in size_groups.items():
        result[group] = {"min": min(prices), "max": max(prices), "count": len(prices)}
    return result

def calc_jeonse_rate(매매stats, 전세stats):
    rates = {}
    for group in 매매stats:
        if group in 전세stats:
            avg_매매 = (매매stats[group]["min"] + 매매stats[group]["max"]) / 2
            avg_전세 = (전세stats[group]["min"] + 전세stats[group]["max"]) / 2
            if avg_매매 > 0:
                rates[group] = round((avg_전세 / avg_매매) * 100, 1)
    return rates

def analyze_data(all_trades):
    apt_trades = defaultdict(list)
    for t in all_trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_trades[name].append(t)
    if not apt_trades:
        return None

    def max_price(trades):
        prices = [get_price(t) for t in trades]
        return max(prices) if prices else 0

    top3_count = sorted(apt_trades.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    top3_price = sorted(apt_trades.items(), key=lambda x: max_price(x[1]), reverse=True)[:3]
    return {"top3_count": top3_count, "top3_price": top3_price, "apt_trades": apt_trades}

def build_dong_data(dong_name, trades, jeonse_trades, analysis, trade_keys, jeonse_keys):
    if not analysis:
        return None

    apt_jeonse = defaultdict(list)
    for t in jeonse_trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_jeonse[name].append(t)

    def build_apt(name, apt_list):
        매매stats = get_apt_stats_for_dashboard(apt_list)
        전세stats = get_jeonse_stats_for_dashboard(apt_jeonse.get(name, []))
        jeonse_rate = calc_jeonse_rate(매매stats, 전세stats)
        prices = [get_price(t) for t in apt_list]
        return {
            "name": name,
            "count": len(apt_list),
            "max_price": max(prices) if prices else 0,
            "stats": 매매stats,
            "jeonse_stats": 전세stats,
            "jeonse_rate": jeonse_rate
        }

    top3_count = [build_apt(n, l) for n, l in analysis["top3_count"]]
    top3_price = [build_apt(n, l) for n, l in analysis["top3_price"]]

    return {
        "dong": dong_name,
        "total_trades": len(trades),
        "total_jeonse": len(jeonse_trades),
        "top3_count": top3_count,
        "top3_price": top3_price,
        "trade_keys": list(trade_keys),
        "jeonse_keys": list(jeonse_keys)
    }

def format_new_trades_message(dong_name, new_trades, new_jeonse, all_trades, all_jeonse):
    """신규 신고 거래 메시지 생성"""
    lines = []

    if new_trades:
        lines.append(f"\n🏠 매매 신규 신고 {len(new_trades)}건")
        lines.append("─" * 25)
        for t in new_trades:
            apt_name = t.get("aptNm", "").strip()
            pyeong = get_pyeong(t.get("excluUseAr", "0"))
            price = get_price(t)
            deal_year = t.get("dealYear", "")
            deal_month = str(t.get("dealMonth", "")).zfill(2)
            deal_day = str(t.get("dealDay", "")).zfill(2)
            deal_date = f"{deal_year}{deal_month}{deal_day}"
            floor = t.get("floor", "")

            lines.append(f"\n🏢 {apt_name} {pyeong} {floor}층")
            lines.append(f"   계약일: {deal_year}.{deal_month}.{deal_day}")
            lines.append(f"   💰 매매가: {format_won(price)}원")

            # 직전 거래 비교
            prev = find_prev_trade(apt_name, pyeong, t.get("excluUseAr"), deal_date, all_trades)
            if prev:
                prev_price = get_price(prev)
                prev_month = str(prev.get("dealMonth", "")).zfill(2)
                diff, rate, arrow, sign = calc_change(price, prev_price)
                lines.append(f"   {arrow} 직전거래({prev.get('dealYear','')}.{prev_month}) 대비")
                lines.append(f"      {sign}{format_won(abs(diff))}원 ({sign}{rate}%)")
            else:
                lines.append(f"   ℹ️ 직전 거래 없음")

            # 전세가율
            apt_jeonse = [j for j in all_jeonse if j.get("aptNm", "").strip() == apt_name]
            jeonse_stats = get_jeonse_stats_for_dashboard(apt_jeonse)
            trade_stats = get_apt_stats_for_dashboard([t])
            rates = calc_jeonse_rate(trade_stats, jeonse_stats)
            if rates:
                rate_val = list(rates.values())[0]
                lines.append(f"   📊 전세가율: {rate_val}%")

    if new_jeonse:
        lines.append(f"\n🔑 전월세 신규 신고 {len(new_jeonse)}건")
        lines.append("─" * 25)
        for t in new_jeonse:
            apt_name = t.get("aptNm", "").strip()
            pyeong = get_pyeong(t.get("excluUseAr", "0"))
            deposit = get_deposit(t)
            monthly = t.get("monthlyRent", "0")
            contract_type = "월세" if monthly and str(monthly).strip() not in ["", "0"] else "전세"
            year = t.get("year", "")
            month = str(t.get("month", "")).zfill(2)
            day = str(t.get("day", "")).zfill(2)
            current_date = f"{year}{month}{day}"

            lines.append(f"\n🏢 {apt_name} {pyeong}")
            lines.append(f"   계약일: {year}.{month}.{day} [{contract_type}]")

            if contract_type == "전세":
                lines.append(f"   💰 전세보증금: {format_won(deposit)}원")
                prev = find_prev_jeonse(apt_name, pyeong, "전세", current_date, all_jeonse)
                if prev:
                    prev_dep = get_deposit(prev)
                    prev_month = str(prev.get("month", "")).zfill(2)
                    diff, rate, arrow, sign = calc_change(deposit, prev_dep)
                    lines.append(f"   {arrow} 직전전세({prev.get('year','')}.{prev_month}) 대비")
                    lines.append(f"      {sign}{format_won(abs(diff))}원 ({sign}{rate}%)")
                else:
                    lines.append(f"   ℹ️ 직전 거래 없음")

                # 매매 대비 전세가율
                apt_trades_list = [tr for tr in all_trades if tr.get("aptNm", "").strip() == apt_name]
                trade_stats = get_apt_stats_for_dashboard(apt_trades_list)
                jeonse_stat = {pyeong: {"min": deposit, "max": deposit, "count": 1}}
                rates = calc_jeonse_rate(trade_stats, jeonse_stat)
                if rates:
                    rate_val = list(rates.values())[0]
                    lines.append(f"   📊 전세가율: {rate_val}%")
            else:
                try:
                    monthly_int = int(str(monthly).replace(",", ""))
                except:
                    monthly_int = 0
                lines.append(f"   💰 보증금: {format_won(deposit)}원 / 월세: {format_won(monthly_int)}원")
                prev = find_prev_jeonse(apt_name, pyeong, "월세", current_date, all_jeonse)
                if prev:
                    prev_dep = get_deposit(prev)
                    try:
                        prev_monthly = int(str(prev.get("monthlyRent", "0")).replace(",", ""))
                    except:
                        prev_monthly = 0
                    prev_month = str(prev.get("month", "")).zfill(2)
                    diff_dep, rate_dep, arrow_dep, sign_dep = calc_change(deposit, prev_dep)
                    diff_mon, rate_mon, arrow_mon, sign_mon = calc_change(monthly_int, prev_monthly)
                    lines.append(f"   {arrow_dep} 직전월세({prev.get('year','')}.{prev_month}) 대비")
                    lines.append(f"      보증금 {sign_dep}{format_won(abs(diff_dep))}원 ({sign_dep}{rate_dep}%)")
                    lines.append(f"      월세 {sign_mon}{format_won(abs(diff_mon))}원 ({sign_mon}{rate_mon}%)")
                else:
                    lines.append(f"   ℹ️ 직전 거래 없음")

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

        trade_keys = set(make_trade_key(t) for t in dong_trades)
        jeonse_keys = set(make_jeonse_key(t) for t in dong_jeonse)

        dong_totals[dong_name] = len(dong_trades)
        all_dong_trades[dong_name] = dong_trades
        all_dong_jeonse[dong_name] = dong_jeonse
        all_trade_keys[dong_name] = trade_keys
        all_jeonse_keys[dong_name] = jeonse_keys
        print(f"{dong_name}: 매매 {len(dong_trades)}건 / 전세 {len(dong_jeonse)}건")

    top3_dongs = sorted(dong_totals.items(), key=lambda x: x[1], reverse=True)[:3]

    # 텔레그램 메시지 구성
    today_str = datetime.now().strftime("%Y.%m.%d")
    msg = f"🏙️ 강동구 부동산 일일 리포트\n📅 {today_str} 오전 8시\n\n"

    # 신규 신고 요약
    summary_lines = []
    for dong_name, _ in top3_dongs:
        prev_trade_keys = get_yesterday_keys(history, dong_name, "trade")
        prev_jeonse_keys = get_yesterday_keys(history, dong_name, "jeonse")
        new_trade_count = len(all_trade_keys[dong_name] - prev_trade_keys)
        new_jeonse_count = len(all_jeonse_keys[dong_name] - prev_jeonse_keys)
        summary_lines.append(f"  {dong_name}: 매매 +{new_trade_count}건 / 전월세 +{new_jeonse_count}건")

    msg += "📊 어제 신규 신고 현황\n"
    msg += "\n".join(summary_lines)
    msg += "\n" + "═" * 30

    # 동네별 신규 상세
    dong_data_list = []
    for dong_name, _ in top3_dongs:
        dong_trades = all_dong_trades[dong_name]
        dong_jeonse = all_dong_jeonse[dong_name]

        prev_trade_keys = get_yesterday_keys(history, dong_name, "trade")
        prev_jeonse_keys = get_yesterday_keys(history, dong_name, "jeonse")

        new_trades = [t for t in dong_trades if make_trade_key(t) not in prev_trade_keys]
        new_jeonse = [t for t in dong_jeonse if make_jeonse_key(t) not in prev_jeonse_keys]

        total_new = len(new_trades) + len(new_jeonse)
        msg += f"\n\n📍 {dong_name} (신규 {total_new}건)"

        if total_new == 0:
            msg += "\n  어제 신규 신고 없음"
        else:
            msg += format_new_trades_message(
                dong_name, new_trades, new_jeonse, dong_trades, dong_jeonse
            )

        # 대시보드용 데이터
        analysis = analyze_data(dong_trades)
        data = build_dong_data(
            dong_name, dong_trades, dong_jeonse, analysis,
            all_trade_keys[dong_name], all_jeonse_keys[dong_name]
        )
        if data:
            dong_data_list.append(data)

    # 저장
    output = {
        "updated_at": datetime.now().strftime("%Y년 %m월 %d일 %H:%M"),
        "top3_dongs": [d["dong"] for d in dong_data_list],
        "data": dong_data_list
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("data.json 저장 완료")

    update_history(dong_data_list)

    print(msg)
    send_telegram(msg)
    print("완료!")

if __name__ == "__main__":
    main()
