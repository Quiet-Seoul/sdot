import subprocess
import sys
import os

def run_update_all_data(python_path):
    print("\n=== 1. 어제 데이터 수집 및 park, main_street 테이블 저장 (update_all_data.py) ===")
    subprocess.run([python_path, "update_all_data.py"], check=True)

def run_model_train(python_path):
    print("\n=== 2. Prophet 모델 학습 및 저장 (model.py) ===")
    subprocess.run([python_path, "model.py"], check=True)

def run_predictor(python_path):
    print("\n=== 3. 예측 결과 park_forecast 테이블 저장 (predictor.py) ===")
    subprocess.run([python_path, "predictor.py"], check=True)

def run_calculate_congestion(python_path):
    print("\n=== 4. 혼잡도 계산 및 park_congestion 테이블 저장 (calculate_congestion.py) ===")
    subprocess.run([python_path, "calculate_congestion.py"], check=True)

def main():
    # 현재 실행 중인 Python 경로 사용
    python_path = sys.executable
    print(f"🛠️  현재 사용하는 Python 경로: {python_path}")

    run_update_all_data(python_path)
    run_model_train(python_path)
    run_predictor(python_path)
    run_calculate_congestion(python_path)
    print("\n✅ 모든 과정 완료!")

if __name__ == '__main__':
    main()
