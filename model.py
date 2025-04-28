import pandas as pd
import pymysql
from prophet import Prophet
import json
from typing import List, Dict
import pickle
from datetime import datetime, timedelta
import os
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

# 공휴일 데이터 불러오기
def load_holidays(filepath: str) -> (pd.DataFrame, set):
    holidays_df = pd.read_csv(filepath)
    holidays_df['ds'] = pd.to_datetime(holidays_df['date'])
    holiday_dates = set(holidays_df['ds'].dt.date)
    holidays = holidays_df[['holiday', 'ds']].copy()
    holidays['lower_window'] = -1
    holidays['upper_window'] = 1
    return holidays, holiday_dates

# DB에서 방문자수 데이터 불러오기
def load_visitor_data_from_db() -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT measuring_time AS ds, visitor_count AS y, park_name
        FROM park
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df['ds'] = pd.to_datetime(df['ds'])
    df = df.drop_duplicates(subset=['ds', 'park_name'])  # 중복 제거
    return df

# Prophet 모델 생성
def build_prophet_model(holidays: pd.DataFrame) -> Prophet:
    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=False,
        yearly_seasonality=False,
        holidays=holidays,
        seasonality_mode='additive'
    )
    model.add_seasonality(
        name='daily_custom',
        period=1,
        fourier_order=15
    )
    model.add_seasonality(
        name='weekly_custom',
        period=7,
        fourier_order=10
    )
    return model

# 방문자 수 가중치 적용
def apply_holiday_weekend_weight(row, holiday_dates):
    current_day = row['ds'].date()
    if current_day in holiday_dates:
        return row['y'] * 3.0
    elif row['ds'].weekday() in [5, 6]:  # 토요일(5), 일요일(6)
        return row['y'] * 1.5
    else:
        return row['y']

# 모델 저장
def save_model(model, filepath: str) -> None:
    with open(filepath, 'wb') as f:
        pickle.dump(model, f)

def main():
    # 경로 설정
    holiday_data_path = 'dataset/kr_holidays_2023_2025.csv'
    model_dir = 'models'
    os.makedirs(model_dir, exist_ok=True)

    park_list = ['송파나루공원', '암사생태공원', '서울숲공원', '서대문독립공원', '북서울꿈의숲', '은평평화공원']

    # DB에서 데이터 불러오기
    df = load_visitor_data_from_db()
    holidays, holiday_dates = load_holidays(holiday_data_path)

    for park in park_list:
        df_park = df[df['park_name'] == park]
        if df_park.empty:
            print(f"[{park}] 데이터 없음, 스킵")
            continue

        # 데이터 가공
        df_prophet = df_park[['ds', 'y']].copy()
        df_prophet['y'] = df_prophet.apply(apply_holiday_weekend_weight, axis=1, holiday_dates=holiday_dates)

        # 모델 생성 및 학습
        model = build_prophet_model(holidays)
        model.fit(df_prophet)

        # 모델 저장
        safe_park_name = park.replace(' ', '_')
        model_path = os.path.join(model_dir, f"{safe_park_name}.pkl")
        save_model(model, model_path)
        print(f"[{park}] 학습 완료 및 저장: {model_path}")

if __name__ == '__main__':
    main()
