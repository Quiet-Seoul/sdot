import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pymysql
import os

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
def fetch_today_park_data(api_key: str, target_date: str) -> pd.DataFrame:
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

            region = row.find("REGION").text
            district = row.find("AUTONOMOUS_DISTRICT").text

            if (region == "parks" and district != "Seoul_Grand_Park") or \
               (region == "public_facilities" and district == "Seodaemun-gu"):
                all_data.append({
                    "SENSING_TIME": sensing_time_str,
                    "DISTRICT": district,
                    "NEIGHBORHOOD": row.find("ADMINISTRATIVE_DISTRICT").text,
                    "VISITOR_COUNT": int(row.find("VISITOR_COUNT").text),
                    "REG_DTTM": row.find("REG_DTTM").text
                })

        # 더 이상 target_date보다 작은 데이터면 중단
        if rows[-1].find("SENSING_TIME").text < target_date:
            break

    df_api = pd.DataFrame(all_data)
    return df_api

# 전처리
def preprocess_api_data(df_api: pd.DataFrame) -> pd.DataFrame:
    district_map = {
        "Jongno-gu": "종로구", "Jung-gu": "중구", "Yongsan-gu": "용산구", "Seongdong-gu": "성동구",
        "Gwangjin-gu": "광진구", "Dongdaemun-gu": "동대문구", "Jungnang-gu": "중랑구", "Seongbuk-gu": "성북구",
        "Gangbuk-gu": "강북구", "Dobong-gu": "도봉구", "Nowon-gu": "노원구", "Eunpyeong-gu": "은평구",
        "Seodaemun-gu": "서대문구", "Mapo-gu": "마포구", "Yangcheon-gu": "양천구", "Gangseo-gu": "강서구",
        "Guro-gu": "구로구", "Geumcheon-gu": "금천구", "Yeongdeungpo-gu": "영등포구", "Dongjak-gu": "동작구",
        "Gwanak-gu": "관악구", "Seocho-gu": "서초구", "Gangnam-gu": "강남구", "Songpa-gu": "송파구", "Gangdong-gu": "강동구"
        }
    
    park_name_map = {
        ('성동구', 'Seongsu1ga1(il)-dong'): '서울숲공원',
        ('성동구', 'Seongsu1ga1-dong'): '서울숲공원',
        ('서대문구', 'Cheonyeon-dong'): '서대문독립공원',
        ('강북구', 'Beon3-dong'): '북서울꿈의숲',
        ('강북구', 'Beon3(sam)-dong'): '북서울꿈의숲',
        ('송파구', 'Jamsil6(yuk)-dong'): '송파나루공원',
        ('송파구', 'Jamsil6-dong'): '송파나루공원',
        ('은평구', 'Nokbeon-dong'): '은평평화공원',
        ('강동구', 'Amsa3(sam)-dong'): '암사생태공원',
        ('강동구', 'Amsa3-dong'): '암사생태공원'
        }  
    
    df_api.rename(columns={
        'SENSING_TIME': '측정시간',
        'DISTRICT': '자치구',
        'NEIGHBORHOOD': '행정동',
        'VISITOR_COUNT': '방문자수',
        'REG_DTTM': '등록일'
    }, inplace=True)

    df_api.drop(columns='등록일', inplace=True)
    df_api['측정시간'] = df_api['측정시간'].str.replace('_', ' ', regex=False)
    df_api['구'] = df_api['자치구'].map(district_map)
    df_api['datetime'] = pd.to_datetime(df_api['측정시간'])
    df_api['공원명'] = df_api.apply(lambda x: park_name_map.get((x['구'], x['행정동']), '기타공원'), axis=1)

    df_api = df_api[['측정시간', '행정동', '방문자수', '구', '공원명']]
    df_api = df_api.sort_values('측정시간').reset_index(drop=True)
    return df_api

# DB 저장
def save_to_db(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO park_test (measuring_time, dong, visitor_count, district, park_name)
        VALUES (%s, %s, %s, %s, %s)
    """

    data = [
        (pd.to_datetime(row['측정시간']), row['행정동'], row['방문자수'], row['구'], row['공원명'])
        for idx, row in df.iterrows()
    ]

    cursor.executemany(insert_query, data)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"{len(data)}건 삽입 완료")

# 실행
if __name__ == '__main__':
    today = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    df_api = fetch_today_park_data(api_key, today)
    df_api = preprocess_api_data(df_api)

    save_to_db(df_api)
