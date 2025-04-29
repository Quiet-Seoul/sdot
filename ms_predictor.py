import pandas as pd
import pickle
import os
import pymysql
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .env 파일 로드
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

# 모델 불러오기
def load_model(filepath: str):
    with open(filepath, 'rb') as f:
        return pickle.load(f)

# 예측 결과를 DB에 저장 (UTC → KST 변환 + created_at/updated_at 직접 넣기)
def save_forecast_to_db(serial_no: str, forecast_df: pd.DataFrame, start_date: str, end_date: str):
    conn = get_connection()
    cursor = conn.cursor()

    # UTC → KST 변환
    forecast_df['ds'] = forecast_df['ds'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')

    # 날짜, 시간 필터링
    forecast_df['date'] = forecast_df['ds'].dt.date
    forecast_df['hour'] = forecast_df['ds'].dt.hour

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    # 현재 한국시간
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)

    # insert 쿼리
    insert_query = """
        INSERT INTO main_street_forecast (serial_no, forecast_date, forecast_hour, yhat, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            yhat = VALUES(yhat),
            updated_at = VALUES(updated_at)
    """

    insert_data = []

    for idx, row in forecast_df.iterrows():
        if start_dt <= row['date'] <= end_dt:
            insert_data.append((
                serial_no,
                row['date'],
                row['hour'],
                max(0, round(row['yhat'], 2)),
                now_kst,
                now_kst
            ))

    if insert_data:
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        print(f"[{serial_no}] {len(insert_data)}건 예측 데이터 삽입/업데이트 완료")

    cursor.close()
    conn.close()

# 메인 실행
def main():
    model_dir = 'models_mainstreet'
    main_street_list = ['4035', '4032', '4020']  # 시리얼 번호 리스트

    # 날짜 설정
    today = datetime.today().date()
    start_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (today + timedelta(days=7)).strftime('%Y-%m-%d')

    for serial_no in main_street_list:
        model_path = os.path.join(model_dir, f"mainstreet_{serial_no}.pkl")

        if not os.path.exists(model_path):
            print(f"[{serial_no}] 모델 없음, 스킵")
            continue

        # 모델 불러오기
        model = load_model(model_path)

        # 45일치 미래 예측
        future = model.make_future_dataframe(periods=45*24, freq='H')
        forecast = model.predict(future)

        # 음수값 보정
        forecast['yhat'] = forecast['yhat'].clip(lower=0)
        forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
        forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)

        # 예측 결과 저장
        save_forecast_to_db(serial_no, forecast, start_date, end_date)

if __name__ == '__main__':
    main()
