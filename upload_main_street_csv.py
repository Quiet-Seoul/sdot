import pandas as pd
import pymysql
import os
import pytz
from dotenv import load_dotenv
from datetime import datetime

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
csv_file_path = 'dataset/main_street/2025Q2_메인거리데이터_clean.csv'

# 1. CSV 읽기
df = pd.read_csv(csv_file_path, usecols=['시리얼', '측정시간', '행정동', '방문자수', '구'])
df.columns = ['serial_no', 'measuring_time', 'dong', 'visitor_count', 'district']

# measuring_time을 datetime 타입으로 변환
df['measuring_time'] = pd.to_datetime(df['measuring_time'])

# created_at 컬럼에 한국 시간 넣기
kst_now = datetime.now(pytz.timezone('Asia/Seoul'))
df['created_at'] = kst_now

# 2. DB 연결
conn = get_connection()
cursor = conn.cursor()

# insert 쿼리
insert_query = """
    INSERT IGNORE INTO main_street (serial_no, measuring_time, dong, visitor_count, district, created_at)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# 삽입할 데이터 준비
data = [
    (row['serial_no'], row['measuring_time'], row['dong'], row['visitor_count'], row['district'], row['created_at'])
    for idx, row in df.iterrows()
]

# 3. 데이터 삽입
cursor.executemany(insert_query, data)
conn.commit()

# 4. 연결 종료
cursor.close()
conn.close()

print(f"✅ main_street 테이블에 CSV 데이터 {len(data)}건 삽입 완료!")
