import os

PYTHON = "/home/ubuntu/sdot/venv/bin/python"

print("\n[1/4] 🔄 실시간 데이터 수집 및 DB 저장 중...")
os.system(f"{PYTHON} update_db.py")

print("\n[2/4] 🤖 Prophet 모델 학습 중...")
os.system(f"{PYTHON} model.py")

print("\n[3/4] 📈 예측값 생성 및 저장 중...")
os.system(f"{PYTHON} predictor.py")

print("\n[4/4] 📊 혼잡도 계산 및 저장 중...")
os.system(f"{PYTHON} calculate_congestion.py")

print("\n✅ 모든 작업 완료!")
