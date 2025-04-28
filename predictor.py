# 필요한 패키지
import pandas as pd
import pickle
import json
import os
from typing import List, Dict
from datetime import datetime, timedelta

# 모델 불러오기
def load_model(filepath: str):
    with open(filepath, 'rb') as f:
        return pickle.load(f)

# 예측 결과 JSON 변환 
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

# JSON 저장
def save_json(data: List[Dict], output_path: str) -> None:
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# 메인 실행
def main():
    model_dir = 'models'
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    park_list = ['송파나루공원', '암사생태공원', '서울숲공원', '서대문독립공원', '북서울꿈의숲', '은평평화공원']

    # 오늘 기준으로 날짜 자동 계산
    today = datetime.today().date()
    start_date = today + timedelta(days=1)
    end_date = start_date + timedelta(days=6)
    today_str = today.strftime('%Y-%m-%d') 
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    for park in park_list:
        safe_park_name = park.replace(' ', '_')
        model_path = os.path.join(model_dir, f"{safe_park_name}.pkl")

        if not os.path.exists(model_path):
            print(f"[{park}] 모델 없음, 스킵")
            continue

        # 모델 불러오기
        model = load_model(model_path)

        # 45일(시간 단위) 미래 예측
        future = model.make_future_dataframe(periods=45*24, freq='H')
        forecast = model.predict(future)

        # 음수값 보정
        forecast['yhat'] = forecast['yhat'].clip(lower=0)
        forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
        forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)

        # 날짜 필터링 (start_date ~ end_date)
        forecast_json = format_forecast_to_json(forecast, start_date=start_date_str, end_date=end_date_str)

        # JSON 파일 저장
        output_path = os.path.join(output_dir, f"{safe_park_name}_{today_str}_forecast.json")
        save_json(forecast_json, output_path)
        print(f"[{park}] 예측 완료 및 저장: {output_path}")

if __name__ == '__main__':
    main()
