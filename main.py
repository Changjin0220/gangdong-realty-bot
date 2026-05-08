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

def get_apt_stats(trades):
    size_groups = defaultdict(list)
    for t in trades:
        try:
            area = float(t.get("excluUseAr", 0))
            pyeong = int(area / 3.3)
            group = f"{(pyeong // 10) * 10}평대"
            price = int(str(t.get("dealAmount", "0")).replace(",", ""))
            size_groups[group].append(price)
        except:
            pass
    result = {}
    for group, prices in size_groups.items():
        result[group] = {"min": min(prices), "max": max(prices), "count": len(prices)}
    return result

def get_jeonse_stats(jeonse_trades):
    size_groups = defaultdict(list)
    for t in jeonse_trades:
        try:
            area = float(t.get("excluUseAr", 0))
            pyeong = int(area / 3.3)
            group = f"{(pyeong // 10) * 10}평대"
            deposit = int(str(t.get("deposit", "0")).replace(",", ""))
            if t.get("contractType") == "전세" or not t.get("monthlyRent"):
                size_groups[group].append(deposit)
        except:
            pass
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
                rate = round((avg_전세 / avg_매매) * 100, 1)
                rates[group] = rate
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
        prices = []
        for t in trades:
            try:
                prices.append(int(str(t.get("dealAmount", "0")).replace(",", "")))
            except:
                pass
        return max(prices) if prices else 0

    top3_count = sorted(apt_trades.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    top3_price = sorted(apt_trades.items(), key=lambda x: max_price(x[1]), reverse=True)[:3]
    return {"top3_count": top3_count, "top3_price": top3_price, "apt_trades": apt_trades}

def build_dong_data(dong_name, trades, jeonse_trades, analysis):
    if not analysis:
        return None

    apt_trades = analysis["apt_trades"]

    # 전세 데이터를 아파트별로 분류
    apt_jeonse = defaultdict(list)
    for t in jeonse_trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_jeonse[name].append(t)

    def build_apt_data(name, apt_list):
        매매stats = get_apt_stats(apt_list)
        전세stats = get_jeonse_stats(apt_jeonse.get(name, []))
        jeonse_rate = calc_jeonse_rate(매매stats, 전세stats)
        prices = []
        for t in apt_list:
            try:
                prices.append(int(str(t.get("dealAmount", "0")).replace(",", "")))
            except:
                pass
        return {
            "name": name,
            "count": len(apt_list),
            "max_price": max(prices) if prices else 0,
            "stats": 매매stats,
            "jeonse_stats": 전세stats,
            "jeonse_rate": jeonse_rate
        }

    top3_count = [build_apt_data(name, apt_list) for name, apt_list in analysis["top3_count"]]
    top3_price = [build_apt_data(name, apt_list) for name, apt_list in analysis["top3_price"]]

    return {
        "dong": dong_name,
        "total_trades": len(trades),
        "top3_count": top3_count,
        "top3_price": top3_price
    }

def format_telegram_message(dong_data_list):
    today = datetime.now().strftime("%Y.%m.%d")
    msg = f"🏙️ 강동구 부동산 일일 리포트\n📅 {today} 오전 8시\n\n"
    msg += f"📊 거래량 TOP 3 동네: {', '.join([d['dong'] for d in dong_data_list])}\n"
    msg += "=" * 35 + "\n"

    for dong in dong_data_list:
        msg += f"\n📍 [{dong['dong']}] (총 {dong['total_trades']}건)\n"
        msg += "🔥 거래량 TOP3\n"
        for i, apt in enumerate(dong["top3_count"], 1):
            msg += f"  {i}위 {apt['name']} ({apt['count']}건)\n"
            for size, stat in sorted(apt["stats"].items()):
                rate = apt["jeonse_rate"].get(size, 0)
                rate_str = f" | 전세가율 {rate}%" if rate else ""
                msg += f"    {size}: {stat['min']:,}~{stat['max']:,}만원{rate_str}\n"
        msg += "💰 최고가 TOP3\n"
        for i, apt in enumerate(dong["top3_price"], 1):
            msg += f"  {i}위 {apt['name']} (최고 {apt['max_price']:,}만원)\n"
            for size, stat in sorted(apt["stats"].items()):
                rate = apt["jeonse_rate"].get(size, 0)
                rate_str = f" | 전세가율 {rate}%" if rate else ""
                msg += f"    {size}: {stat['min']:,}~{stat['max']:,}만원{rate_str}\n"
        msg += "\n"
    return msg

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        res = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk})
        print(f"텔레그램 전송: {res.status_code}")

def update_history(dong_data_list):
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 기존 히스토리 로드
    try:
        with open("history.json", "r", encoding="utf-8") as f:
            history = json.load(f)
    except:
        history = []

    # 오늘 데이터 추가
    history.append({
        "date": today,
        "data": dong_data_list
    })

    # 최근 90일만 유지
    history = history[-90:]

    with open("history.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print("history.json 저장 완료")

def main():
    print("강동구 부동산 알리미 시작...")
    months = get_recent_months(3)

    dong_totals = {}
    all_dong_trades = {}
    all_dong_jeonse = {}

    for dong_name, lawd_cd in DONG_NAMES.items():
        trades = []
        jeonse = []
        for month in months:
            trades += fetch_trade_data(lawd_cd, month)
            jeonse += fetch_jeonse_data(lawd_cd, month)

        dong_trades = [t for t in trades if dong_name in t.get("umdNm", "")]
        dong_jeonse = [t for t in jeonse if dong_name in t.get("umdNm", "")]

        dong_totals[dong_name] = len(dong_trades)
        all_dong_trades[dong_name] = dong_trades
        all_dong_jeonse[dong_name] = dong_jeonse
        print(f"{dong_name}: 매매 {len(dong_trades)}건 / 전세 {len(dong_jeonse)}건")

    top3_dongs = sorted(dong_totals.items(), key=lambda x: x[1], reverse=True)[:3]

    dong_data_list = []
    for dong_name, count in top3_dongs:
        trades = all_dong_trades[dong_name]
        jeonse = all_dong_jeonse[dong_name]
        analysis = analyze_data(trades)
        data = build_dong_data(dong_name, trades, jeonse, analysis)
        if data:
            dong_data_list.append(data)

    # data.json 저장
    output = {
        "updated_at": datetime.now().strftime("%Y년 %m월 %d일 %H:%M"),
        "top3_dongs": [d["dong"] for d in dong_data_list],
        "data": dong_data_list
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("data.json 저장 완료")

    # history.json 업데이트
    update_history(dong_data_list)

    # 텔레그램 발송
    msg = format_telegram_message(dong_data_list)
    print(msg)
    send_telegram(msg)
    print("완료!")

if __name__ == "__main__":
    main()
