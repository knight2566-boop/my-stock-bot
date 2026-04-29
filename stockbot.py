import yfinance as yf
import pandas as pd
import requests
import os
from io import StringIO
from bs4 import BeautifulSoup


class NasdaqDataFetcher:
    """나스닥 100 데이터를 가져오는 클래스 (백업 리스트 포함)"""

    def __init__(self):
        self.wiki_url = "https://wikipedia.org"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}

        # 최악의 경우를 대비한 나스닥 100 전체 종목 백업 리스트 (101개)
        self.backup_tickers = [
            "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "TSLA", "AVGO", "COST",
            "PEP", "ADBE", "LIN", "CSCO", "TMUS", "AMD", "CMCSA", "INTU", "TXN", "AMGN",
            "ISRG", "HON", "QCOM", "AMAT", "INTC", "BKNG", "VRTX", "SBUX", "MDLZ", "REGN",
            "ADP", "LRCX", "PANW", "MU", "ADI", "SNPS", "KLAC", "CDNS", "GILD", "ASML",
            "MELI", "PDD", "MAR", "NXPI", "ORLY", "PYPL", "CTAS", "MNST", "LULU", "CRWD",
            "ADSK", "ROST", "FTNT", "IDXX", "PAYX", "CPRT", "AEP", "DASH", "DXCM", "KDP",
            "ODFL", "MCHP", "GEHC", "KHC", "AZN", "EXC", "MRVL", "CTSH", "WDAY", "TEAM",
            "EA", "XEL", "MDB", "ZS", "BKR", "WBD", "ANSS", "TTWO", "FANG", "CDW",
            "VRSK", "BIIB", "ON", "CEG", "CSGP", "DDOG", "TTD", "ROP", "ABNB", "AXON",
            "PCAR", "EBAY", "CHTR", "TRI", "CCEP", "MDB", "VRSK", "SIRI", "DLTR", "WBA"
        ]

    def fetch_tickers(self):
        """웹에서 티커 수집 시도, 실패 시 백업 리스트 반환"""
        try:
            response = requests.get(
                self.wiki_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'id': 'constituents'}) or soup.find(
                'table', {'class': 'wikitable'})

            tickers = []
            if table:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if cols:
                        # 첫 번째 칸에서 티커 텍스트 추출
                        ticker = cols[0].text.strip()
                        tickers.append(ticker.replace('.', '-'))

                if len(tickers) > 50:  # 정상적으로 많이 가져온 경우만 반환
                    return sorted(list(set(tickers)))

            print("⚠️ 웹에서 표를 찾지 못해 내장된 백업 리스트를 사용합니다.")
        except Exception as e:
            print(f"⚠️ 연결 오류로 백업 리스트를 사용합니다: {e}")

        return self.backup_tickers

    def get_all_price_changes(self, tickers):
        """모든 종목의 등락률 일괄 수집"""
        print(f"📡 나스닥 100 종목({len(tickers)}개) 데이터를 분석 중...")
        # progress=False로 지저분한 로그 제거
        data = yf.download(tickers, period="5d",
                           group_by='ticker', progress=False)

        results = []
        for t in tickers:
            try:
                # 데이터가 Series인지 DataFrame인지 확인하여 처리
                hist = data[t].dropna() if len(tickers) > 1 else data.dropna()
                if len(hist) >= 2:
                    prev_close = hist["Close"].iloc[-2]
                    close = hist["Close"].iloc[-1]
                    change = round(
                        ((close - prev_close) / prev_close) * 100, 2)
                    results.append((t, change))
            except:
                continue
        return results

    def get_market_news(self, limit=3):
        """네이버 금융에서 실제 뉴스 헤드라인 수집"""
        try:
            url = "https://naver.com"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')

            # 네이버 금융 뉴스 목록의 articleSubject 태그 타겟팅
            news_elements = soup.select(
                'dl dt.articleSubject a, dl dd.articleSubject a')

            summaries = []
            for item in news_elements:
                if len(summaries) >= limit:
                    break
                title = item.get_text(strip=True)
                if not title:
                    continue
                if len(title) > 45:
                    title = title[:42] + "..."
                summaries.append(f"▫️ {title}")

            return summaries if summaries else ["▫️ 최신 뉴스를 찾을 수 없습니다."]
        except:
            return ["▫️ 뉴스 데이터를 불러오는 중 오류가 발생했습니다."]


class ReportGenerator:
    """순위 리스트 출력 클래스"""
    @staticmethod
    def generate_rank_report(ticker_results, name_map, theme_map, news_list):  # news_list 추가
        # (기존 테마 정렬 로직 동일...)
        theme_groups = {}
        for ticker, change in ticker_results:
            theme = theme_map.get(ticker, "기타/미분류")
            if theme not in theme_groups:
                theme_groups[theme] = []
            theme_groups[theme].append(change)

        theme_summary = []
        for theme, changes in theme_groups.items():
            avg_change = sum(changes) / len(changes)
            stocks_in_theme = [
                (t, c) for t, c in ticker_results if theme_map.get(t, "기타/미분류") == theme]
            sorted_stocks = sorted(
                stocks_in_theme, key=lambda x: x[1], reverse=True)
            theme_summary.append(
                {'theme': theme, 'avg': avg_change, 'stocks': sorted_stocks})

        theme_summary = sorted(
            theme_summary, key=lambda x: x['avg'], reverse=True)

        # --- 메시지 구성 시작 ---
        msg = "🏛 *[나스닥 100 테마 & 뉴스 리포트]*\n"
        msg += f"📅 일시: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n"
        msg += "=" * 30 + "\n"

        # 1. 뉴스 섹션 + 요청하신 링크 추가
        if news_list:
            msg += "📰 *주요 해외 증시 뉴스 (Naver)*\n"
            for news in news_list:
                msg += f"{news}\n"
            msg += "🔗 [네이버 증권 뉴스 바로가기](https://finance.naver.com/news/news_list.naver?mode=LSS3D&section_id=101&section_id2=258&section_id3=403)\n"
            msg += "=" * 30 + "\n\n"

        # 2. 오늘의 주도 테마 TOP 3
        msg += "🔥 *오늘의 주도 테마 TOP 3*\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, item in enumerate(theme_summary[:3]):
            sign = "+" if item['avg'] > 0 else ""
            msg += f"{medals[i]} {item['theme']}: *{sign}{item['avg']:.2f}%*\n"

        msg += "\n" + "=" * 30 + "\n\n"

        # (상세 리스트 출력 로직 동일...)
        for item in theme_summary:
            if item['avg'] > 0:
                avg_icon = "🔴"  # 상승 테마 헤더
            else:
                avg_icon = "🔵"  # 하락 테마 헤더 (파란 원)
            msg += f"{avg_icon} *{item['theme']}* (평균 {item['avg']:+.2f}%)\n"
            msg += "```\n"
            for ticker, change in item['stocks']:
                name = name_map.get(ticker, ticker)[:8]
                sign = "+" if change > 0 else ""
                display_text = f"{name}({ticker})"
                msg += f" • {display_text:<16} : {sign}{change:>6.2f}%\n"
            msg += "```\n"

        return msg


# --- [추가] 텔레그램 봇 클래스 ---


class TelegramBot:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        # self.api_url = f"https://telegram.org{self.token}/sendMessage"
        self.api_url = f"https://api.telegram.org/bot8708497760:AAHLZTNq7B0BJ8_9BCywAyxgqeZQhWQTwtw/sendMessage"

    def send_message(self, text):
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "Markdown"  # 표 정렬을 위해 마크다운 사용
            }
            # 주소가 잘 만들어졌는지 확인용 출력 (나중에 지우셔도 됩니다)
            print(f"연결 주소: {self.api_url}")

            # 전송 시도
            response = requests.post(self.api_url, data=payload)

            # 상세 에러 확인을 위한 코드 추가
            if response.status_code != 200:
                print(f"❌ 텔레그램 API 에러: {response.text}")
        except Exception as e:
            print(f"❌ 전송 실패: {e}")

# ===============================
# 해외뉴스 수집
# ===============================


class NewsFetcher:

    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def get_naver_world_news(self, limit=5):
        try:
            url = "https://finance.naver.com/world/"
            res = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")

            news = []
            items = soup.select("ul.news_list li")

            for item in items[:limit]:
                title = item.select_one("a").text.strip()
                news.append(f"▫️ {title}")

            return news

        except:
            return ["▫️ 뉴스 수집 실패"]


# --- 실행부 ---
if __name__ == "__main__":
    # --- [추가] 텔레그램 접속 정보 ---
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    # 전체 한글명 매핑 데이터
    NAME_MAP = {
        "AAPL": "애플", "MSFT": "마이크로소프트", "AMZN": "아마존", "NVDA": "엔비디아", "GOOGL": "구글A",
        "GOOG": "구글C", "META": "메타", "TSLA": "테슬라", "AVGO": "브로드컴", "COST": "코스트코",
        "PEP": "펩시코", "ADBE": "어도비", "LIN": "린데", "CSCO": "시스코", "TMUS": "티모바일",
        "AMD": "AMD", "CMCSA": "컴캐스트", "INTU": "인튜이트", "TXN": "텍사스인스트루먼트", "AMGN": "암젠",
        "ISRG": "인튜이티브", "HON": "허니웰", "QCOM": "퀄컴", "AMAT": "어플라이드", "INTC": "인텔",
        "BKNG": "부킹 홀딩스", "VRTX": "버텍스", "SBUX": "스타벅스", "MDLZ": "몬델리즈", "REGN": "리제네론",
        "ADP": "오토매틱데이터", "LRCX": "램리서치", "PANW": "팔로알토", "MU": "마이크론", "ADI": "아나로그디바이스",
        "SNPS": "시놉시스", "KLAC": "KLA", "CDNS": "케이던스", "GILD": "길리어드", "ASML": "ASML",
        "MELI": "메르카도", "PDD": "핀둬둬", "MAR": "메리어트", "NXPI": "NXP반도체", "ORLY": "오라일리",
        "PYPL": "페이팔", "CTAS": "신타스", "MNST": "몬스터베버리지", "LULU": "룰루레몬", "CRWD": "크라우드스트라이크",
        "ADSK": "오토데스크", "ROST": "로스스토어", "FTNT": "포티넷", "PAYX": "페이첵스", "CPRT": "코파트",
        "KDP": "큐리그닥터페퍼", "ODFL": "올드도미니언", "MCHP": "마이크로칩", "GEHC": "GE헬스케어", "KHC": "크래프트하인즈",
        "AZN": "아스트라제네카", "EXC": "엑셀론", "MRVL": "마벨", "CTSH": "코그니전트", "WDAY": "워크데이",
        "TEAM": "아틀라시안", "EA": "EA", "XEL": "엑셀에너지", "MDB": "몽고DB", "ZS": "지스케일러",
        "BKR": "베이커휴즈", "WBD": "워너브라더스", "VRSK": "베리스크", "BIIB": "바이오젠", "ON": "온세미",
        "CEG": "콘스텔레이션", "DDOG": "데이터독", "TTD": "트레이드데스크", "ROP": "로퍼", "ABNB": "에어비앤비",
        "AXON": "액슨", "PCAR": "파카", "EBAY": "이베이", "TRI": "톰슨로이터", "CCEP": "코카콜라EP", "ANSS": "앤시스"
    }

    THEME_MAP = {
        # 반도체 및 관련 장비
        "NVDA": "반도체", "AVGO": "반도체", "AMD": "반도체", "TXN": "반도체", "QCOM": "반도체",
        "AMAT": "반도체", "LRCX": "반도체", "MU": "반도체", "ADI": "반도체", "ASML": "반도체",
        "KLAC": "반도체", "NXPI": "반도체", "MRVL": "반도체", "MCHP": "반도체", "ON": "반도체",
        "ARM": "반도체", "INTC": "반도체", "MDB": "소프트웨어",

        # AI / 소프트웨어 / 클라우드
        "MSFT": "AI/소프트웨어", "ADBE": "AI/소프트웨어", "SNPS": "AI/소프트웨어", "CDNS": "AI/소프트웨어",
        "ADSK": "AI/소프트웨어", "WDAY": "AI/소프트웨어", "TEAM": "AI/소프트웨어", "ANSS": "AI/소프트웨어",
        "DDOG": "AI/소프트웨어", "TTD": "AI/소프트웨어", "ORCL": "AI/소프트웨어", "CRM": "AI/소프트웨어",
        "PLTR": "AI/소프트웨어", "GOOGL": "AI/플랫폼", "GOOG": "AI/플랫폼", "META": "AI/플랫폼",

        # 빅테크 및 전기차
        "AAPL": "빅테크", "AMZN": "빅테크", "TSLA": "전기차",

        # 소비재 / 유통 / 이커머스
        "COST": "유통/소비재", "PEP": "식음료/소비재", "SBUX": "식음료/소비재", "MDLZ": "식음료/소비재",
        "MNST": "식음료/소비재", "KDP": "식음료/소비재", "KHC": "식음료/소비재", "CCEP": "식음료/소비재",
        "LULU": "의류/소비재", "ROST": "유통/소비재", "ORLY": "유통/소비재", "AZO": "유통/소비재",
        "DASH": "이커머스/플랫폼", "BKNG": "여행/플랫폼", "MELI": "이커머스/플랫폼", "PDD": "이커머스/플랫폼",
        "MAR": "호텔/여행", "ABNB": "여행/플랫폼", "EBAY": "이커머스/플랫폼", "DLTR": "유통/소비재",
        "TGT": "유통/소비재", "WBA": "의약/유통",

        # 바이오 / 헬스케어
        "AMGN": "바이오", "VRTX": "바이오", "REGN": "바이오", "GILD": "바이오", "ISRG": "의료기기",
        "DXCM": "의료기기", "IDXX": "의료기기", "AZN": "바이오", "BIIB": "바이오", "GEHC": "의료기기",
        "MRNA": "바이오", "ALGN": "의료기기", "MDT": "의료기기", "RVMD": "바이오",

        # 사이버보안
        "PANW": "사이버보안", "CRWD": "사이버보안", "FTNT": "사이버보안", "ZS": "사이버보안",

        # 엔터테인먼트 / 미디어
        "NFLX": "미디어/콘텐츠", "WBD": "미디어/콘텐츠", "CMCSA": "미디어/콘텐츠", "CHTR": "미디어/콘텐츠",
        "SIRI": "미디어/콘텐츠", "TTWO": "게임", "EA": "게임",

        # 산업재 / 운송 / 금융서비스
        "HON": "산업재", "AXON": "산업재", "ADP": "비즈니스서비스", "PAYX": "비즈니스서비스",
        "VRSK": "비즈니스서비스", "TRI": "비즈니스서비스", "CTSH": "IT서비스", "CDW": "IT서비스",
        "ROP": "산업재/소프트웨어", "CSGP": "부동산서비스", "ODFL": "운송/물류", "CPRT": "운송/물류",
        "PCAR": "운송/물류", "FAST": "산업재/유통", "CSX": "운송/물류", "PYPL": "핀테크",

        # 에너지 및 유틸리티
        "LIN": "에너지/화학", "AEP": "에너지/유틸리티", "EXC": "에너지/유틸리티", "XEL": "에너지/유틸리티",
        "CEG": "에너지/유틸리티", "FANG": "에너지/에너지", "BKR": "에너지/서비스"
    }

    fetcher = NasdaqDataFetcher()

    # --- [추가] 봇 객체 생성 ---
    bot = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    # 1. 티커 수집 (실패해도 백업 리스트 사용)
    tickers = fetcher.fetch_tickers()

    # 2. 수익률 수집 및 등락률 계산
    ticker_results = fetcher.get_all_price_changes(tickers)

    # 3. 뉴스 데이터 수집 (추가)
    # news_list = fetcher.get_market_news()
    news_fetcher = NewsFetcher()
    news = news_fetcher.get_naver_world_news()

    # 3. 수익률순으로 리포트 생성 및 출력
    if ticker_results:
        report_text = ReportGenerator.generate_rank_report(
            ticker_results, NAME_MAP, THEME_MAP, news)
        print(report_text)

        # --- [수정 포인트] 여러 명에게 전송 ---
        for chat_id in TELEGRAM_CHAT_ID:
            try:
                # 각 ID별로 봇 객체를 생성하여 전송
                bot = TelegramBot(TELEGRAM_TOKEN, chat_id)
                bot.send_message(report_text)
                print(f"✅ {chat_id} 전송 성공!")
            except Exception as send_err:
                print(f"❌ {chat_id} 전송 실패: {send_err}")
        # ---------------------------------------
        # --- [추가] 메시지 전송 실행 ---
        # bot.send_message(report_text)
    else:
        print("데이터를 가져오는 데 실패했습니다. 인터넷 연결을 확인해 주세요.")
