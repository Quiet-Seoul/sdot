import pandas as pd
import pymysql
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# .env 로드
load_dotenv()

# DB 연결 함수
def get_connection():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        charset='utf8'
    )

# 공원 혼잡도 기준
def get_park_congestion_label(visitors, area_m2):
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

# 거리 혼잡도 기준
def get_street_congestion_label(visitors, area_m2):
    if visitors == 0:
        return "여유"
    per_capita_area = area_m2 / visitors
    if per_capita_area >= 9.29:
        return "여유"
    elif per_capita_area >= 4.61:
        return "보통"
    elif per_capita_area >= 2.81:
        return "약간 혼잡"
    else:
        return "혼잡"

# 장소별 설정 (공원 + 거리)
place_settings = {
    # 공원
    "은평평화공원": {"type": "park", "area_m2": 42500, "stay_hours": 1, "scaling_factor": 25},
    "북서울꿈의숲": {"type": "park", "area_m2": 660000, "stay_hours": 3, "scaling_factor": 50},
    "서울숲공원": {"type": "park", "area_m2": 480994, "stay_hours": 3, "scaling_factor": 100},
    "암사생태공원": {"type": "park", "area_m2": 270279, "stay_hours": 3, "scaling_factor": 50},
    # "송파나루공원": {"type": "park", "area_m2": 285757, "stay_hours": 2, "scaling_factor": 50},
    "서대문독립공원": {"type": "park", "area_m2": 44600, "stay_hours": 2, "scaling_factor": 20},
    # 거리
    "이태원회나무길": {"type": "mainstreet", "area_m2": 12168.4, "stay_hours": 3, "scaling_factor": 50},
    "샤로수길": {"type": "mainstreet", "area_m2": 70056.9, "stay_hours": 3, "scaling_factor": 50},
}

# 혼잡도 계산 및 저장
def process_place_congestion(name, start_date, end_date):
    settings = place_settings.get(name)
    if not settings:
        print(f"[{name}] 설정 없음, 스킵")
        return

    place_type = settings["type"]
    area_m2 = settings["area_m2"]
    stay_hours = settings["stay_hours"]
    scaling_factor = settings["scaling_factor"]

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT forecast_date, forecast_hour, yhat
        FROM forecast
        WHERE name = %s AND type = %s
        AND forecast_date BETWEEN %s AND %s
        ORDER BY forecast_date, forecast_hour
    """
    df = pd.read_sql(query, conn, params=[name, place_type, start_date, end_date])

    if df.empty:
        print(f"[{name}] 예측 데이터 없음, 스킵")
        cursor.close()
        conn.close()
        return

    now_kst = datetime.now(pytz.timezone('Asia/Seoul'))
    stay_history = []
    stay_population = 0
    insert_data = []

    for _, row in df.iterrows():
        incoming = row['yhat'] * scaling_factor
        stay_history.append(incoming)
        stay_population += incoming

        if len(stay_history) > stay_hours:
            stay_population -= stay_history[-(stay_hours+1)]

        stay_population = max(stay_population, 0)

        # 혼잡도 라벨 선택
        if place_type == "park":
            label = get_park_congestion_label(stay_population, area_m2)
        else:
            label = get_street_congestion_label(stay_population, area_m2)

        insert_data.append((
            name, place_type,
            row['forecast_date'],
            int(row['forecast_hour']),
            label, now_kst, now_kst
        ))

    insert_query = """
        INSERT INTO congestion
        (name, type, congestion_date, congestion_hour, congestion_level, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            congestion_level = VALUES(congestion_level),
            updated_at = VALUES(updated_at)
    """

    if insert_data:
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"[{place_type.upper()}] {name} → {len(insert_data)}건 혼잡도 저장 완료")

    cursor.close()
    conn.close()

# 실행
def main():
    all_places = list(place_settings.keys())
    today = datetime.today().date()
    start_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=7)).strftime('%Y-%m-%d')

    for name in all_places:
        process_place_congestion(name, start_date, end_date)

if __name__ == '__main__':
    main()
