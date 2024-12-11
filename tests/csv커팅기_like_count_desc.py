import pandas as pd
import os
import sys


def main():
    # 사용자로부터 입력 파일 이름 받기
    input_file = input("처리할 CSV 파일의 경로와 이름을 입력하세요: ").strip()

    # 파일 존재 여부 확인
    if not os.path.isfile(input_file):
        print(f"오류: 파일 '{input_file}'이(가) 존재하지 않습니다.")
        sys.exit(1)

    # 입력 파일의 디렉토리와 기본 이름 분리
    input_dir, input_filename = os.path.split(input_file)
    base_name, ext = os.path.splitext(input_filename)

    try:
        # CSV 파일 불러오기
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"오류: CSV 파일을 읽는 중 문제가 발생했습니다. {e}")
        sys.exit(1)

    # 'like_count' 컬럼 존재 여부 확인
    if "like_count" not in df.columns:
        print("오류: 'like_count' 컬럼이 CSV 파일에 존재하지 않습니다.")
        sys.exit(1)

    # 'like_count' 기준으로 내림차순 정렬 후 상위 300개 선택
    top_300 = df.sort_values(by="like_count", ascending=False).head(300)

    # 상위 300개를 새로운 CSV 파일로 저장
    top_300_filename = f"{base_name}_top_300.csv"
    top_300_path = os.path.join(input_dir, top_300_filename)
    try:
        top_300.to_csv(top_300_path, index=False)
        print(f"상위 300개 데이터를 '{top_300_filename}' 파일로 저장했습니다.")
    except Exception as e:
        print(f"오류: '{top_300_filename}' 파일을 저장하는 중 문제가 발생했습니다. {e}")
        sys.exit(1)

    # 상위 300개 데이터를 100개씩 나누어 3개의 파일로 저장
    for i in range(3):
        start_idx = i * 100
        end_idx = start_idx + 100
        part = top_300.iloc[start_idx:end_idx]

        part_filename = f"{base_name}_part_{i+1}.csv"
        part_path = os.path.join(input_dir, part_filename)
        try:
            part.to_csv(part_path, index=False)
            print(f"{i+1}번째 파트를 '{part_filename}' 파일로 저장했습니다.")
        except Exception as e:
            print(
                f"오류: '{part_filename}' 파일을 저장하는 중 문제가 발생했습니다. {e}"
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
