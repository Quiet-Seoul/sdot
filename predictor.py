import pandas as pd
import pickle
import os
import pymysql
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv

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

# 모델 불러오기
def load_model(filepath: str):
    with open(filepath, 'rb') as f:
        return pickle.load(f)

# 예측 결과 저장
def save_forecast_to_db(name: str, place_type: str, forecast_df: pd.DataFrame, start_date: str, end_date: str):
    conn = get_connection()
    cursor = conn.cursor()

    forecast_df['ds'] = forecast_df['ds'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')
    forecast_df['date'] = forecast_df['ds'].dt.date
    forecast_df['hour'] = forecast_df['ds'].dt.hour

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    now_kst = datetime.now(pytz.timezone('Asia/Seoul'))

    insert_query = """
        INSERT INTO forecast (name, type, forecast_date, forecast_hour, yhat, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            yhat = VALUES(yhat),
            updated_at = VALUES(updated_at)
    """

    insert_data = [
        (
            name, place_type, row['date'], row['hour'],
            max(0, round(row['yhat'], 2)), now_kst, now_kst
        )
        for _, row in forecast_df.iterrows()
        if start_dt <= row['date'] <= end_dt
    ]

    if insert_data:
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"[{place_type.upper()}] {name} → {len(insert_data)}건 저장 완료")

    cursor.close()
    conn.close()

# 실행
def main():
    model_dir = 'models'
    park_list = ['암사생태공원', '서울숲공원', '서대문독립공원', '북서울꿈의숲', '은평평화공원']

    main_street_map = {
        '4035': '샤로수길',
        '4020': '이태원회나무길'
    }

    today = datetime.today().date()
    start_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=7)).strftime('%Y-%m-%d')

    # 공원 처리
    for park in park_list:
        model_path = os.path.join(model_dir, f"{park.replace(' ', '_')}.pkl")
        if not os.path.exists(model_path):
            print(f"[{park}] 모델 없음")
            continue
        model = load_model(model_path)
        future = model.make_future_dataframe(periods=45*24, freq='h')
        forecast = model.predict(future)
        forecast['yhat'] = forecast['yhat'].clip(lower=0)
        save_forecast_to_db(park, 'park', forecast, start_date, end_date)

    # 거리 처리
    for serial_no, street_name in main_street_map.items():
        model_path = os.path.join('models_mainstreet', f"{street_name}.pkl")
        if not os.path.exists(model_path):
            print(f"[{serial_no}] 모델 없음")
            continue
        model = load_model(model_path)
        future = model.make_future_dataframe(periods=45*24, freq='H')
        forecast = model.predict(future)
        forecast['yhat'] = forecast['yhat'].clip(lower=0)
        save_forecast_to_db(street_name, 'mainstreet', forecast, start_date, end_date)

if __name__ == '__main__':
    main()
