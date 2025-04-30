import pandas as pd
import pymysql
from prophet import Prophet
import pickle
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Tuple

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

# 공휴일 데이터 불러오기
def load_holidays(filepath: str) -> Tuple[pd.DataFrame, set]:
    holidays_df = pd.read_csv(filepath)
    holidays_df['ds'] = pd.to_datetime(holidays_df['date'])
    holiday_dates = set(holidays_df['ds'].dt.date)
    holidays = holidays_df[['holiday', 'ds']].copy()
    holidays['lower_window'] = -1
    holidays['upper_window'] = 1
    return holidays, holiday_dates

# 데이터 불러오기
def load_data_from_db(table: str, name_col: str) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
        SELECT measuring_time AS ds, visitor_count AS y, {name_col}
        FROM {table}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.drop_duplicates(subset=['ds', name_col])
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
    model.add_seasonality('daily_custom', period=1, fourier_order=15)
    model.add_seasonality('weekly_custom', period=7, fourier_order=10)
    return model

# 공휴일/주말 가중치
def apply_holiday_weekend_weight(row, holiday_dates):
    current_day = row['ds'].date()
    if current_day in holiday_dates:
        return row['y'] * 3.0
    elif row['ds'].weekday() in [5, 6]:
        return row['y'] * 1.5
    else:
        return row['y']

# 모델 저장
def save_model(model, filepath: str) -> None:
    with open(filepath, 'wb') as f:
        pickle.dump(model, f)

# 실행
def main():
    holiday_data_path = 'dataset/kr_holidays_2023_2025.csv'
    holidays, holiday_dates = load_holidays(holiday_data_path)

    # 공원 처리
    park_list = ['송파나루공원', '암사생태공원', '서울숲공원', '서대문독립공원', '북서울꿈의숲', '은평평화공원']
    df_park = load_data_from_db('park_data', 'park_name')

    os.makedirs('models', exist_ok=True)

    for park in park_list:
        df_one = df_park[df_park['park_name'] == park]
        if df_one.empty:
            print(f"[{park}] 데이터 없음, 스킵")
            continue

        df_prophet = df_one[['ds', 'y']].copy()
        df_prophet['y'] = df_prophet.apply(apply_holiday_weekend_weight, axis=1, holiday_dates=holiday_dates)

        model = build_prophet_model(holidays)
        model.fit(df_prophet)

        model_path = os.path.join('models', f"{park.replace(' ', '_')}.pkl")
        save_model(model, model_path)
        print(f"[PARK] {park} 모델 저장 완료")

    # 거리 처리
    main_street_map = {
        '4035': '샤로수길',
        '4020': '이태원회나무길'
    }
    serial_list = list(main_street_map.keys())
    df_street = load_data_from_db('main_street', 'serial_no')

    os.makedirs('models_mainstreet', exist_ok=True)

    for serial in serial_list:
        df_one = df_street[df_street['serial_no'] == serial]
        if df_one.empty:
            print(f"[{serial}] 거리 데이터 없음, 스킵")
            continue

        df_prophet = df_one[['ds', 'y']].copy()
        df_prophet['y'] = df_prophet.apply(apply_holiday_weekend_weight, axis=1, holiday_dates=holiday_dates)

        model = build_prophet_model(holidays)
        model.fit(df_prophet)

        street_name = main_street_map.get(serial, f"unknown_{serial}")
        model_path = os.path.join('models_mainstreet', f"{street_name}.pkl")
        save_model(model, model_path)
        print(f"[STREET] {street_name} 모델 저장 완료")

if __name__ == '__main__':
    main()
