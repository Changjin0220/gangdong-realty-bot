import requests
import os
from datetime import datetime, timedelta
from collections import defaultdict

# 환경변수에서 키 가져오기
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
MOLIT_API_KEY = os.environ.get("MOLIT_API_KEY")

# 강동구 법정동 코드
GANGDONG_CODE = "11740"

# 강동구 주요 동 코드 (법정동코드 앞 5자리 = 구 코드, 전체는 8자리)
DONG_NAMES = {
    "길동": "1174010100",
    "둔촌동": "1174010200",
    "암사동": "1174010300",
    "성내동": "1174010400",
    "천호동": "1174010500",
    "강일동": "1174010600",
    "상일동": "1174010700",
    "명일동": "1174010800",
    "고덕동": "1174010900",
    "둔촌동": "1174011000",
}

def get_recent_months(n=6):
    """최근 n개월 계약년월 리스트 반환"""
    months = []
    now = datetime.now()
    for i in range(n):
        d = now - timedelta(days=30 * i)
        months.append(d.strftime("%Y%m"))
    return months

def fetch_trade_data(lawdcd, yearmonth):
    """국토부 API에서 실거래가 데이터 가져오기"""
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
    """거래 데이터 분석: TOP3 아파트, 평수별 가격 등"""
    
    # 아파트별 거래 집계
    apt_trades = defaultdict(list)
    for t in all_trades:
        name = t.get("aptNm", "").strip()
        if name:
            apt_trades[name].append(t)
    
    if not apt_trades:
        return None

    # 거래량 TOP3
    top3_by_count = sorted(apt_trades.items(), key=lambda x: len(x[1]), reverse=True)[:3]
    
    # 최고가 TOP3
    def max_price(trades):
        prices = []
        for t in trades:
            try:
                prices.append(int(str(t.get("dealAmount", "0")).replace(",", "")))
            except:
                pass
        return max(prices) if prices else 0
    
    top3_by_price = sorted(apt_trades.items(), key=lambda x: max_price(x[1]), reverse=True)[:3]
    
    return {
        "top3_count": top3_by_count,
        "top3_price": top3_by_price,
        "apt_trades": apt_trades
    }

def format_apt_summary(apt_name, trades):
    """아파트 한 개의 요약 텍스트 생성"""
    
    # 평수별 분류 (전용면적 기준)
    size_groups = defaultdict(list)
    for t in trades:
        try:
            area = float(t.get("excluUseAr", 0))
            pyeong = int(area / 3.3)
            # 10평대, 20평대, 30평대 등으로 묶기
            group = f"{(pyeong // 10) * 10}평대"
            price = int(str(t.get("dealAmount", "0")).replace(",", ""))
            size_groups[group].append(price)
        except:
            pass
    
    lines = [f"🏢 {apt_name} (총 {len(trades)}건)"]
    
    for group in sorted(size_groups.keys()):
        prices = size_groups[group]
        min_p = min(prices)
        max_p = max(prices)
        lines.append(f"  {group}: {min_p:,}~{max_p:,}만원")
    
    return "\n".join(lines)

def build_message(dong_name, analysis):
    """텔레그램 전송용 메시지 생성"""
    if not analysis:
        return f"📍 {dong_name}: 최근 거래 데이터 없음\n"
    
    today = datetime.now().strftime("%Y년 %m월 %d일")
    msg = f"""
📍 [{dong_name}] 부동산 리포트
📅 {today}
{'='*30}

🔥 거래량 TOP 3
"""
    for i, (name, trades) in enumerate(analysis["top3_count"], 1):
        msg += f"\n{i}위 " + format_apt_summary(name, trades) + "\n"
    
    msg += f"\n{'='*30}\n💰 최고가 TOP 3\n"
    
    for i, (name, trades) in enumerate(analysis["top3_price"], 1):
        msg += f"\n{i}위 " + format_apt_summary(name, trades) + "\n"
    
    return msg

def send_telegram(message):
    """텔레그램 메시지 전송"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # 메시지가 너무 길면 나눠서 전송 (텔레그램 4096자 제한)
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        res = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML"
        })
        print(f"텔레그램 전송: {res.status_code}")

def main():
    print("강동구 부동산 알리미 시작...")
    
    # 최근 3개월 데이터 수집
    months = get_recent_months(3)
    
    # 거래량 기준으로 동 순위 먼저 파악
    dong_totals = {}
    all_dong_trades = {}
    
    for dong_name, dong_code in DONG_NAMES.items():
        lawd_cd = dong_code[:5]  # 앞 5자리가 구+동 코드
        trades = []
        for month in months:
            trades += fetch_trade_data(lawd_cd, month)
        
        # 해당 동 거래만 필터링
        dong_trades = [t for t in trades if dong_name in t.get("umdNm", "")]
        dong_totals[dong_name] = len(dong_trades)
        all_dong_trades[dong_name] = dong_trades
        print(f"{dong_name}: {len(dong_trades)}건")
    
    # 거래량 TOP 3 동 선별
    top3_dongs = sorted(dong_totals.items(), key=lambda x: x[1], reverse=True)[:3]
    
    full_message = f"🏙️ 강동구 부동산 일일 리포트\n📅 {datetime.now().strftime('%Y.%m.%d')} 오전 8시\n\n"
    full_message += f"📊 거래량 TOP 3 동네: {', '.join([d[0] for d in top3_dongs])}\n"
    full_message += "="*35 + "\n"
    
    for dong_name, count in top3_dongs:
        trades = all_dong_trades[dong_name]
        analysis = analyze_data(trades)
        full_message += build_message(dong_name, analysis)
        full_message += "\n"
    
    print(full_message)
    send_telegram(full_message)
    print("완료!")

if __name__ == "__main__":
    main()
