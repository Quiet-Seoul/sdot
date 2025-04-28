# 필요한 패키지
import pandas as pd
from prophet import Prophet
import json
from typing import List, Dict
import pickle
from datetime import datetime, timedelta
import os

# 방문자수 불러오기
def load_visitor_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df = df[['측정시간', '방문자수', '공원명']].copy()
    df.rename(columns={'측정시간': 'ds', '방문자수': 'y'}, inplace=True)
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.drop_duplicates(subset=['ds', '공원명'])  # 중복 제거
    return df

# 공휴일 데이터
def load_holidays(filepath: str) -> (pd.DataFrame, set):
    holidays_df = pd.read_csv(filepath)
    holidays_df['ds'] = pd.to_datetime(holidays_df['date'])
    holiday_dates = set(holidays_df['ds'].dt.date)
    holidays = holidays_df[['holiday', 'ds']].copy()
    holidays['lower_window'] = -1
    holidays['upper_window'] = 1
    return holidays, holiday_dates

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

# 방문자 수 가중치 
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

# 모델 학습 및 예측
def train_and_forecast(df: pd.DataFrame, model: Prophet, predict_hours: int) -> pd.DataFrame:
    model.fit(df)
    future = model.make_future_dataframe(periods=predict_hours, freq='H')
    forecast = model.predict(future)
    forecast['yhat'] = forecast['yhat'].clip(lower=0)
    forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
    forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)
    return forecast

# 예측 결과 JSON 포맷 변환
def format_forecast_to_json(forecast_df: pd.DataFrame, start_date: str, end_date: str) -> List[Dict]:

    forecast_df['date'] = forecast_df['ds'].dt.date
    forecast_df['hour'] = forecast_df['ds'].dt.hour

    target_dates = pd.date_range(start=start_date, end=end_date).date
    result = []

    for date in target_dates:
        df_day = forecast_df[forecast_df['date'] == date]
        day_data = {'day': date.strftime('%Y-%m-%d')}
        for hour in range(24):
            value = df_day[df_day['hour'] == hour]['yhat']
            if not value.empty:
                day_data[hour] = round(value.values[0], 2)
            else:
                day_data[hour] = 0
        result.append(day_data)

    return result

# JSON 파일 저장
def save_json(data: List[Dict], output_path: str) -> None:
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    # 파일 경로 설정
    visitor_data_path = 'dataset/park_data.csv'
    holiday_data_path = 'dataset/kr_holidays_2023_2025.csv'
    model_dir = 'models' 
    os.makedirs(model_dir, exist_ok=True)  

    park_list = ['송파나루공원', '암사생태공원', '서울숲공원', '서대문독립공원', '북서울꿈의숲', '은평평화공원']
    
    df = load_visitor_data(visitor_data_path)
    holidays, holiday_dates = load_holidays(holiday_data_path)

    for park in park_list:
        df_park = df[df['공원명'] == park]
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