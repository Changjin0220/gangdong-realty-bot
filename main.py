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
        print(f"API 오류 ({lawdcd}, {yearmonth}): {e}")
        return []

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
    return {"top3_count": top3_count, "top3_price": top3_price}

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

def build_json_data(dong_name, trades, analysis):
    if not analysis:
        return None
    
    apt_trades = defaultdict(list)
    for t in trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_trades[name].append(t)

    top3_count = []
    for name, apt_list in analysis["top3_count"]:
        top3_count.append({
            "name": name,
            "count": len(apt_list),
            "stats": get_apt_stats(apt_list)
        })

    top3_price = []
    for name, apt_list in analysis["top3_price"]:
        prices = []
        for t in apt_list:
            try:
                prices.append(int(str(t.get("dealAmount", "0")).replace(",", "")))
            except:
                pass
        top3_price.append({
            "name": name,
            "max_price": max(prices) if prices else 0,
            "count": len(apt_list),
            "stats": get_apt_stats(apt_list)
        })

    return {
        "dong": dong_name,
        "total_trades": len(trades),
        "top3_count": top3_count,
        "top3_price": top3_price
    }

def format_telegram_message(dong_data_list, top3_dongs):
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
                msg += f"    {size}: {stat['min']:,}~{stat['max']:,}만원\n"
        msg += "💰 최고가 TOP3\n"
        for i, apt in enumerate(dong["top3_price"], 1):
            msg += f"  {i}위 {apt['name']} (최고 {apt['max_price']:,}만원)\n"
            for size, stat in sorted(apt["stats"].items()):
                msg += f"    {size}: {stat['min']:,}~{stat['max']:,}만원\n"
        msg += "\n"
    return msg

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        res = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk
        })
        print(f"텔레그램 전송: {res.status_code}")

def main():
    print("강동구 부동산 알리미 시작...")
    months = get_recent_months(3)

    dong_totals = {}
    all_dong_trades = {}

    for dong_name, lawd_cd in DONG_NAMES.items():
        trades = []
        for month in months:
            trades += fetch_trade_data(lawd_cd, month)
        dong_trades = [t for t in trades if dong_name in t.get("umdNm", "")]
        dong_totals[dong_name] = len(dong_trades)
        all_dong_trades[dong_name] = dong_trades
        print(f"{dong_name}: {len(dong_trades)}건")

    top3_dongs = sorted(dong_totals.items(), key=lambda x: x[1], reverse=True)[:3]

    dong_data_list = []
    for dong_name, count in top3_dongs:
        trades = all_dong_trades[dong_name]
        analysis = analyze_data(trades)
        data = build_json_data(dong_name, trades, analysis)
        if data:
            dong_data_list.append(data)

    # JSON 저장
    output = {
        "updated_at": datetime.now().strftime("%Y년 %m월 %d일 %H:%M"),
        "top3_dongs": [d["dong"] for d in dong_data_list],
        "data": dong_data_list
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("data.json 저장 완료")

    # 텔레그램 발송
    msg = format_telegram_message(dong_data_list, top3_dongs)
    print(msg)
    send_telegram(msg)
    print("완료!")

if __name__ == "__main__":
    main()
