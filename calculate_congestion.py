import pandas as pd
import json
import os
from datetime import datetime, timedelta

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

def process_park_forecast(park_name, input_path, output_path):
    # 설정 가져오기
    settings = park_settings.get(park_name)
    if not settings:
        print(f"[{park_name}] 설정 없음, 스킵")
        return

    area_m2 = settings["area_m2"]
    stay_hours = settings["stay_hours"]
    scaling_factor = settings["scaling_factor"]

    # JSON 읽기
    with open(input_path, 'r', encoding='utf-8') as f:
        json_list = json.load(f)

    result = []

    for day_entry in json_list:
        day_result = {'day': day_entry['day']}
        stay_history = []
        stay_population = 0

        for hour in range(24):
            incoming = day_entry.get(str(hour), 0)

            # 1. 스케일링
            incoming *= scaling_factor

            # 2. 체류 시간만큼 인구 계산
            stay_history.append(incoming)
            stay_population += incoming

            if hour >= stay_hours:
                stay_population -= stay_history[hour - stay_hours]

            stay_population = max(stay_population, 0)

            # 3. 혼잡도 매핑
            congestion = get_congestion_label(stay_population, area_m2)
            day_result[str(hour)] = congestion

        result.append(day_result)

    # 저장
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[{park_name}] 혼잡도 계산 완료 및 저장: {output_path}")

def main():
    input_dir = 'output'  # 예측 결과 JSON 저장된 폴더
    output_dir = 'output_congestion' 

    park_list = [
        "은평평화공원", "북서울꿈의숲", "서울숲공원",
        "암사생태공원", "송파나루공원", "서대문독립공원"
    ]

    today = datetime.today().date()
    today_str = today.strftime('%Y-%m-%d') 
    start_date = today + timedelta(days=1)
    start_date_str = start_date.strftime('%Y-%m-%d') 

    for park in park_list:
        safe_park_name = park.replace(' ', '_')
        input_path = os.path.join(input_dir, f"{safe_park_name}_{today_str}_forecast.json")
        output_path = os.path.join(output_dir, f"{safe_park_name}_{today_str}_congestion.json")

        if not os.path.exists(input_path):
            print(f"[{park}] 예측 JSON 없음, 스킵")
            continue

        process_park_forecast(park, input_path, output_path)

if __name__ == '__main__':
    main()
