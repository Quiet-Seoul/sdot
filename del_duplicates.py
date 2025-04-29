import pandas as pd

# CSV 파일 불러오기
df = pd.read_csv('dataset/2025Q2_공원데이터.csv')

# measuring_time, dong, park_name, district, visitor_count 이 5개 컬럼 기준으로 중복 제거
df_dedup = df.drop_duplicates(subset=['측정시간', '행정동', '방문자수', '구', '공원명'])

# 결과 저장
df_dedup.to_csv('dataset/park/2025Q2_공원데이터_clean.csv', index=False)

print(f"중복 제거 완료! {len(df) - len(df_dedup)}건 제거됨.")
