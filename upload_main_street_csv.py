import pandas as pd
import pymysql
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

# CSV 파일 경로
csv_file_path = 'dataset/main_street/2025Q2_메인거리데이터.csv'  

# 1. CSV 읽기
df = pd.read_csv(csv_file_path)
df.columns = ['serial_no', 'measuring_time', 'region', 'dong', 'visitor_count', 'district']

# measuring_time을 datetime 타입으로 변환
df['measuring_time'] = pd.to_datetime(df['measuring_time'])

# 2. DB 연결
conn = get_connection()
cursor = conn.cursor()

# insert 쿼리
insert_query = """
    INSERT INTO main_street (serial_no, measuring_time, region, dong, visitor_count, district)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# 삽입할 데이터 준비
data = [
    (row['serial_no'], row['measuring_time'], row['region'], row['dong'], row['visitor_count'], row['district'])
    for idx, row in df.iterrows()
]

# 3. 데이터 삽입
cursor.executemany(insert_query, data)
conn.commit()

# 4. 연결 종료
cursor.close()
conn.close()

print(f"✅ main_street 테이블에 CSV 데이터 {len(data)}건 삽입 완료!")
