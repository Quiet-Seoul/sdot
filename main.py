# main.py - 전체 파이프라인 실행 스크립트

import os

print("\n[1/4] 🔄 실시간 데이터 수집 및 DB 저장 중...")
os.system("python update_db.py")

print("\n[2/4] 🤖 Prophet 모델 학습 중...")
os.system("python model.py")

print("\n[3/4] 📈 예측값 생성 및 저장 중...")
os.system("python predictor.py")

print("\n[4/4] 📊 혼잡도 계산 및 저장 중...")
os.system("python calculate_congestion.py")

print("\n✅ 모든 작업 완료!")
