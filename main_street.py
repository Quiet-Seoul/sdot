import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pymysql
import os
import pytz

# .env 파일 로드
load_dotenv()
api_key = os.getenv('SDOT_API_KEY')

# DB 연결 함수
def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8'
    )

# API 수집
def fetch_today_all_data(api_key: str, target_date: str) -> pd.DataFrame:
    all_data = []
    for page in range(1, 1000):
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/IotVdata018/{(page-1)*100+1}/{page*100}"
        response = requests.get(url)
        root = ET.fromstring(response.content)
        rows = root.findall(".//row")

        if not rows:
            break

        for row in rows:
            sensing_time_str = row.find("SENSING_TIME").text
            if not sensing_time_str.startswith(target_date):
                if sensing_time_str < target_date:
                    break
                continue

            record = {
                "MODEL_NM": row.findtext("MODEL_NM", default=None),
                "SERIAL_NO": row.findtext("SERIAL_NO", default=None),
                "SENSING_TIME": sensing_time_str,
                "REGION": row.find("REGION").text,
                "AUTONOMOUS_DISTRICT": row.find("AUTONOMOUS_DISTRICT").text,
                "ADMINISTRATIVE_DISTRICT": row.find("ADMINISTRATIVE_DISTRICT").text,
                "VISITOR_COUNT": int(row.find("VISITOR_COUNT").text),
                "REG_DTTM": row.find("REG_DTTM").text
            }
            all_data.append(record)

        if rows[-1].find("SENSING_TIME").text < target_date:
            break

    return pd.DataFrame(all_data)

# 메인거리 필터링
def filter_mainstreet_data(df_all: pd.DataFrame) -> pd.DataFrame:
    return df_all[df_all['REGION'] == "main_street"]

# 메인거리 전처리
def preprocess_mainstreet_data(df_main: pd.DataFrame) -> pd.DataFrame:
    district_map = {
        "Jongno-gu": "종로구", "Jung-gu": "중구", "Yongsan-gu": "용산구", "Seongdong-gu": "성동구",
        "Gwangjin-gu": "광진구", "Dongdaemun-gu": "동대문구", "Jungnang-gu": "중랑구", "Seongbuk-gu": "성북구",
        "Gangbuk-gu": "강북구", "Dobong-gu": "도봉구", "Nowon-gu": "노원구", "Eunpyeong-gu": "은평구",
        "Seodaemun-gu": "서대문구", "Mapo-gu": "마포구", "Yangcheon-gu": "양천구", "Gangseo-gu": "강서구",
        "Guro-gu": "구로구", "Geumcheon-gu": "금천구", "Yeongdeungpo-gu": "영등포구", "Dongjak-gu": "동작구",
        "Gwanak-gu": "관악구", "Seocho-gu": "서초구", "Gangnam-gu": "강남구", "Songpa-gu": "송파구", "Gangdong-gu": "강동구"
    }

    df_main.rename(columns={
        'MODEL_NM': '모델명',
        'SERIAL_NO': '시리얼번호',
        'SENSING_TIME': '측정시간',
        'REGION': '지역',
        'AUTONOMOUS_DISTRICT': '자치구',
        'ADMINISTRATIVE_DISTRICT': '행정동',
        'VISITOR_COUNT': '방문자수',
        'REG_DTTM': '등록일'
    }, inplace=True)

    df_main.drop(columns='등록일', inplace=True)
    df_main = df_main[df_main['시리얼번호'].notna()]
    df_main['시리얼번호'] = df_main['시리얼번호'].astype(int).astype(str)
    df_main['측정시간'] = df_main['측정시간'].str.replace('_', ' ', regex=False)
    df_main['구'] = df_main['자치구'].map(district_map)

    df_main = df_main[['시리얼번호', '측정시간', '행정동', '방문자수', '구']]
    df_main = df_main.sort_values('측정시간').reset_index(drop=True)
    return df_main

# DB 저장
def save_to_mainstreet_db(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT IGNORE INTO main_street 
        (serial_no, measuring_time, dong, visitor_count, district, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    kst_now = datetime.now(pytz.timezone('Asia/Seoul'))

    data = [
        (
            row['시리얼번호'],
            pd.to_datetime(row['측정시간']),
            row['행정동'],
            row['방문자수'],
            row['구'],
            kst_now
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ main_street 테이블에 {len(data)}건 삽입 완료!")

# 실행
if __name__ == '__main__':
    today = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    df_all = fetch_today_all_data(api_key, today)
    df_main_raw = filter_mainstreet_data(df_all)
    df_main = preprocess_mainstreet_data(df_main_raw)
    save_to_mainstreet_db(df_main)
