import pandas as pd
import pymysql
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# .env 파일 로드
load_dotenv()

# DB 연결
def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8'
    )

# 혼잡도 계산 함수
def get_congestion_label(visitors, area_m2):
    if visitors == 0:
        return "여유"
    per_capita_area = area_m2 / visitors
    if per_capita_area >= 100:
        return "여유"
    elif per_capita_area >= 50:
        return "보통"
    elif per_capita_area >= 20:
        return "약간 혼잡"
    else:
        return "혼잡"

# 공원별 설정
park_settings = {
    "은평평화공원": {"area_m2": 42500, "stay_hours": 1, "scaling_factor": 25},
    "북서울꿈의숲": {"area_m2": 660000, "stay_hours": 3, "scaling_factor": 50},
    "서울숲공원": {"area_m2": 480994, "stay_hours": 3, "scaling_factor": 100},
    "암사생태공원": {"area_m2": 270279, "stay_hours": 3, "scaling_factor": 50},
    "송파나루공원": {"area_m2": 285757, "stay_hours": 2, "scaling_factor": 50},
    "서대문독립공원": {"area_m2": 44600, "stay_hours": 2, "scaling_factor": 20},
}

# 혼잡도 계산 및 DB 저장
def process_park_forecast(park_name, start_date, end_date):
    # 설정 가져오기
    settings = park_settings.get(park_name)
    if not settings:
        print(f"[{park_name}] 설정 없음, 스킵")
        return

    area_m2 = settings["area_m2"]
    stay_hours = settings["stay_hours"]
    scaling_factor = settings["scaling_factor"]

    conn = get_connection()
    cursor = conn.cursor()

    # park_forecast 데이터 불러오기
    query = """
        SELECT forecast_date, forecast_hour, yhat
        FROM park_forecast
        WHERE park_name = %s
        AND forecast_date BETWEEN %s AND %s
        ORDER BY forecast_date, forecast_hour
    """
    df = pd.read_sql(query, conn, params=[park_name, start_date, end_date])

    if df.empty:
        print(f"[{park_name}] 예측 데이터 없음, 스킵")
        cursor.close()
        conn.close()
        return

    # 현재 한국시간 가져오기
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)

    # 혼잡도 계산
    stay_history = []
    stay_population = 0
    insert_data = []

    for idx, row in df.iterrows():
        incoming = row['yhat'] * scaling_factor

        # 체류 인구 계산
        stay_history.append(incoming)
        stay_population += incoming

        if len(stay_history) > stay_hours:
            stay_population -= stay_history[-(stay_hours+1)]

        stay_population = max(stay_population, 0)

        # 혼잡도 매핑
        congestion = get_congestion_label(stay_population, area_m2)

        insert_data.append((
            park_name,
            row['forecast_date'],
            int(row['forecast_hour']),
            congestion,
            now_kst,
            now_kst
        ))

    # park_congestion에 저장
    insert_query = """
        INSERT INTO park_congestion (park_name, congestion_date, congestion_hour, congestion_level, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            congestion_level = VALUES(congestion_level),
            updated_at = VALUES(updated_at)
    """

    if insert_data:
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"[{park_name}] {len(insert_data)}건 혼잡도 데이터 삽입/업데이트 완료 (KST)")

    cursor.close()
    conn.close()

# 메인 실행
def main():
    park_list = [
        "은평평화공원", "북서울꿈의숲", "서울숲공원",
        "암사생태공원", "송파나루공원", "서대문독립공원"
    ]

    today = datetime.today().date()
    start_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=7)).strftime('%Y-%m-%d')

    for park in park_list:
        process_park_forecast(park, start_date, end_date)

if __name__ == '__main__':
    main()
