import pandas as pd
import os
import sys

# 상수를 정의합니다. 필요에 따라 변경할 수 있습니다.
SPLIT_SIZE = 100  # 파일을 분할할 때 한 파일당 최대 데이터 개수


def main():
    # 사용자로부터 입력 파일 이름 받기
    input_file = input("처리할 CSV 파일의 경로와 이름을 입력하세요: ").strip()

    # 파일 존재 여부 확인
    if not os.path.isfile(input_file):
        print(f"오류: 파일 '{input_file}'이(가) 존재하지 않습니다.")
        sys.exit(1)

    # 사용자로부터 상위 n개의 데이터 수 입력 받기
    while True:
        n_input = input(
            "상위 몇 개의 데이터를 추출하시겠습니까? 기본값은 1입니다: "
        ).strip()
        if not n_input:
            TOP_N = 1
            break
        try:
            TOP_N = int(n_input)
            if TOP_N < 1:
                print("오류: n은 1 이상의 정수여야 합니다.")
                continue
            break
        except ValueError:
            print("오류: 유효한 정수를 입력해주세요.")

    # 입력 파일의 디렉토리와 기본 이름 분리
    input_dir, input_filename = os.path.split(input_file)
    base_name, ext = os.path.splitext(input_filename)

    try:
        # CSV 파일 불러오기
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"오류: CSV 파일을 읽는 중 문제가 발생했습니다. {e}")
        sys.exit(1)

    # 'published_at' 컬럼 존재 여부 확인
    if "published_at" not in df.columns:
        print("오류: 'published_at' 컬럼이 CSV 파일에 존재하지 않습니다.")
        sys.exit(1)

    # 'published_at' 컬럼을 datetime 형식으로 변환
    try:
        df["published_at"] = pd.to_datetime(df["published_at"])
    except Exception as e:
        print(
            f"오류: 'published_at' 컬럼을 datetime 형식으로 변환하는 중 문제가 발생했습니다. {e}"
        )
        sys.exit(1)

    # 'published_at' 기준으로 내림차순 정렬 후 상위 n개 선택
    top_n = df.sort_values(by="published_at", ascending=False).head(TOP_N)

    # 상위 n개를 새로운 CSV 파일로 저장
    recent_filename = f"{base_name}_recent_{TOP_N}.csv"
    recent_path = os.path.join(input_dir, recent_filename)
    try:
        top_n.to_csv(recent_path, index=False)
        print(f"상위 {TOP_N}개 데이터를 '{recent_filename}' 파일로 저장했습니다.")
    except Exception as e:
        print(f"오류: '{recent_filename}' 파일을 저장하는 중 문제가 발생했습니다. {e}")
        sys.exit(1)

    # n이 1을 초과할 경우, 데이터를 SPLIT_SIZE 단위로 분할하여 저장
    if TOP_N > 1:
        total_parts = (TOP_N - 1) // SPLIT_SIZE + 1  # 필요한 파일 개수 계산
        for i in range(total_parts):
            start_idx = i * SPLIT_SIZE
            end_idx = start_idx + SPLIT_SIZE
            segment = top_n.iloc[start_idx:end_idx]

            segment_number = i + 1
            segment_filename = f"{base_name}_recent_segment_{segment_number}.csv"
            segment_path = os.path.join(input_dir, segment_filename)
            try:
                segment.to_csv(segment_path, index=False)
                print(
                    f"{segment_number}번째 세그먼트를 '{segment_filename}' 파일로 저장했습니다."
                )
            except Exception as e:
                print(
                    f"오류: '{segment_filename}' 파일을 저장하는 중 문제가 발생했습니다. {e}"
                )
                sys.exit(1)


if __name__ == "__main__":
    main()
