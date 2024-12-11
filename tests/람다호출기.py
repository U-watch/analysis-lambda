import boto3
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# AWS Lambda 클라이언트 생성
lambda_client = boto3.client("lambda", region_name="ap-northeast-2")

# Lambda 함수 ARN
LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:677276074069:function:getVideos"

# 공통 요청 데이터 (access_token과 refresh_token을 여기에 입력)
ACCESS_TOKEN = "ya29.a0AeDClZA3TZb5NhSuUOfEpuL-W5Rur7wHDyhCnO-iUIsc3T_9FY7VileTOjQrje1VMGFk9Y2Khb0cC9U7rbDlAfUMoqH4u0MfylwWWxg0bikjDp7mkD2kj7CfzHkwR0jk17FThNAEKQLhn8FFxo92XNMq69scFuAlDfSyjKLtaCgYKAdcSARISFQHGX2Miu-yUOQRNT3RvwuL1imdleQ0175"
REFRESH_TOKEN = "1//0eDqntD4KHzaaCgYIARAAGA4SNwF-L9IrEsZ_aTQKNBqZLiJb51SmKEWWZ_ELui8giriCAWsU3EnGoshxoCpOH1II-arBpOSNOtY"

# 호출할 video_id 리스트
video_ids = ["ISeSjFBMZFs"]


def invoke_lambda(video_id):
    payload = {
        "access_token": ACCESS_TOKEN,
        "refresh_token": REFRESH_TOKEN,
        "video_id": video_id,
    }

    try:
        response = lambda_client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",  # 동기 호출
            Payload=json.dumps(payload),
        )

        # 응답 읽기
        response_payload = response["Payload"].read()
        result = json.loads(response_payload)
        return video_id, result
    except Exception as e:
        return video_id, {"error": str(e)}


def main():
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 각 video_id에 대해 Lambda 호출 작업 제출
        future_to_video = {
            executor.submit(invoke_lambda, vid): vid for vid in video_ids
        }

        for future in as_completed(future_to_video):
            video_id = future_to_video[future]
            try:
                vid, result = future.result()
                results[vid] = result
                print(f"Video ID: {vid}, Result: {result}")
            except Exception as exc:
                results[video_id] = {"error": str(exc)}
                print(f"Video ID: {video_id} generated an exception: {exc}")

    # 모든 결과를 딕셔너리로 반환하거나 필요한 방식으로 처리
    return results


if __name__ == "__main__":
    all_results = main()
    # 예: 결과를 JSON 파일로 저장
    with open("lambda_results.json", "w") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)
