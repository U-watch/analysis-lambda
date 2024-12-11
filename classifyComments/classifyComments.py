import io
import csv
import re, textwrap
import traceback
import os
import json
import time
from typing import List, Dict, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import boto3
import openai

# 환경 변수에서 설정 불러오기
# S3_BUCKET = os.environ.get("S3_BUCKET")
# OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not S3_BUCKET or not OPENAI_API_KEY:
    raise EnvironmentError(
        "필수 환경 변수가 설정되지 않았습니다: S3_BUCKET, OPENAI_API_KEY"
    )

# OpenAI API 키 설정
openai_client = oclient = openai.OpenAI(api_key=OPENAI_API_KEY)
s3_client = boto3.client("s3")


def fetch_comments_from_s3(video_id: str) -> List[Dict[str, Any]]:
    """
    S3에서 댓글 CSV 파일을 가져와 리스트로 반환합니다.
    파일명은 'comments_{video_id}.csv'로 가정합니다.
    """
    filename = f"unclassified/comments_{video_id}.csv"
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=filename)
        content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        comments = [row for row in reader]
        print(f"S3에서 '{filename}' 파일을 성공적으로 가져왔습니다.")
        return comments
    except s3_client.exceptions.NoSuchKey:
        print(f"S3 버킷 '{S3_BUCKET}'에 '{filename}' 파일이 존재하지 않습니다.")
        return []
    except Exception as e:
        print(f"S3에서 파일을 가져오는 중 오류 발생: {e}")
        return []


def upload_comments_to_s3(file_obj: io.StringIO, video_id: str) -> str:
    """
    분류된 댓글 데이터를 새로운 CSV 파일로 S3에 업로드하고 S3 URI를 반환합니다.
    파일명은 'classified_comments_{video_id}.csv'로 설정합니다.
    """
    filename = f"classified/classified_comments_{video_id}.csv"
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=file_obj.getvalue())
        s3_uri = f"s3://{S3_BUCKET}/{filename}"
        print(f"분류된 CSV 파일이 S3에 업로드되었습니다: {s3_uri}")
        return s3_uri
    except Exception as e:
        print(f"S3 업로드 중 오류 발생: {e}")
        return ""


@retry(
    stop=stop_after_attempt(3),  # 최대 5번 재시도
    wait=wait_exponential(
        multiplier=1, min=60, max=70
    ),  # 재시도 간 대기 시간 (2초 ~ 10초)
    retry=retry_if_exception_type(Exception),  # 모든 예외에 대해 재시도
)
def call_openai_api(prompt: str, response_format: Dict) -> Dict:
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that classifies YouTube comments.",
                },
                {"role": "user", "content": textwrap.dedent(prompt)},
            ],
            response_format=response_format,
            temperature=0,
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"OpenAI API 호출 중 오류 발생: {e}")
        raise


def classify_comments(
    comments: List[Dict[str, Any]], batch_size: int = 50
) -> List[Dict[str, Any]]:
    """
    GPT API를 사용하여 댓글을 분류하고 키워드를 추출합니다.
    댓글을 배치 단위로 처리합니다.
    """

    # 프롬프트 템플릿 정의
    classification_prompt = """
    Classify the following YouTube comments (given in the format `comment_id:comment`) into the specified categories with detailed output for each dimension:
    1. **Sentiment**: Assign one of the following emotions: JOY, ANGER, SADNESS, SURPRISE, FEAR, DISGUST.
    2. **PositiveStatus**: Determine the overall sentiment as POSITIVE, NEGATIVE, or NEUTRAL.
    3. **CommentCategory**: Classify the comment into one of these categories: REACTION, FEEDBACK, QUESTION, SPAM, INSULT.

    Additionally:
    - Focus on retaining the exact characters in `comment_id` without any changes or omissions.
    - Ensure differentiation between IDs with similar but distinct strings, as shown in the input example above.
    - Extract exactly 3 keywords that best represent the comment's content, excluding meaningless symbols, common stopwords, and adjectives. Keywords should be nouns or verbs.
    
    **Comments: **
    """

    # Enum 기준 정의
    SENTIMENT_ENUM = ["JOY", "ANGER", "SADNESS", "SURPRISE", "FEAR", "DISGUST"]
    POSITIVE_STATUS_ENUM = ["POSITIVE", "NEGATIVE", "NEUTRAL"]
    COMMENT_CATEGORY_ENUM = ["REACTION", "FEEDBACK", "QUESTION", "SPAM", "INSULT"]

    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "classified_comments",
            "schema": {
                "type": "object",
                "properties": {
                    "comments": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "comment_id": {
                                    "type": "string",
                                },
                                "comment": {
                                    "type": "string",
                                },
                                "sentiment": {
                                    "type": "string",
                                    "enum": SENTIMENT_ENUM,
                                },
                                "positive_status": {
                                    "type": "string",
                                    "enum": POSITIVE_STATUS_ENUM,
                                },
                                "comment_category": {
                                    "type": "string",
                                    "enum": COMMENT_CATEGORY_ENUM,
                                },
                                "keywords": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                            "required": [
                                "comment_id",
                                "comment",
                                "sentiment",
                                "positive_status",
                                "comment_category",
                                "keywords",
                            ],
                            "additionalProperties": False,
                        },
                    }
                },
                "required": ["comments"],
                "additionalProperties": False,
            },
            "strict": True,
        },
    }

    for i in range(0, len(comments), batch_size):
        batch_comments = comments[i : i + batch_size]
        for comment in batch_comments:
            classification_prompt += (
                f"{comment['comment_id']} : {comment['text_display']},"
            )

        try:
            # response = openai_client.chat.completions.create(
            #     model="gpt-4o-mini",
            #     messages=[
            #         {
            #             "role": "system",
            #             "content": "You are a helpful assistant that classifies YouTube comments.",
            #         },
            #         {"role": "user", "content": textwrap.dedent(classification_prompt)},
            #     ],
            #     response_format=response_format,
            #     temperature=0,
            # )

            # print("사용된 토큰 수:", response.usage.total_tokens)

            # classified = json.loads(response.choices[0].message.content)

            classified = call_openai_api(classification_prompt, response_format)

            # 분류용 딕셔너리 생성
            classification_dict = {}
            for comment in classified["comments"]:
                classification_dict[comment["comment_id"]] = comment

            for comment in batch_comments:
                class_result = classification_dict.get(comment["comment_id"], {})
                comment["sentiment"] = (
                    class_result["sentiment"]
                    if class_result.get("sentiment") in SENTIMENT_ENUM
                    else ""
                )
                comment["positive_status"] = (
                    class_result["positive_status"]
                    if class_result.get("positive_status") in POSITIVE_STATUS_ENUM
                    else ""
                )
                comment["comment_category"] = (
                    class_result["comment_category"]
                    if class_result.get("comment_category") in COMMENT_CATEGORY_ENUM
                    else ""
                )
                comment["keywords"] = class_result.get("keywords", [])

        except Exception as e:
            print("!" * 100, f"\nGPT API 호출 중 오류 발생: {e}")
            traceback.print_exc()
            # print(
            #     "\n", response.choices[0].message.content, "\n", "-" * 100, end="\n\n"
            # )

            # 오류 발생 시 기본값 설정
            for comment in batch_comments:
                comment["sentiment"] = "UNKNOWN"
                comment["positive_status"] = "UNKNOWN"
                comment["comment_category"] = "UNKNOWN"
                comment["keywords"] = []
    return comments


def save_comments_to_csv(comments: List[Dict[str, Any]]) -> io.StringIO:
    """
    댓글 데이터를 CSV 파일로 저장하고 StringIO 객체를 반환합니다.
    """
    if not comments:
        print("저장할 댓글이 없습니다.")
        return None

    # CSV 헤더 정의
    headers = list(comments[0].keys())

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()

    for comment in comments:
        writer.writerow(comment)

    output.seek(0)
    return output


def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    이벤트는 JSON 형식으로 `video_id`를 포함해야 합니다.
    추가로 `batch_size`를 포함할 수 있습니다.
    """
    # 이벤트에서 필요한 데이터 추출
    try:
        video_id = event["video_id"]
        batch_size = int(event.get("batch_size", 20))  # 배치 크기 변수 추가, 기본값 20
    except KeyError as e:
        return {"statusCode": 400, "body": json.dumps(f"Missing parameter: {e}")}
    except ValueError:
        return {"statusCode": 400, "body": json.dumps("Invalid batch_size parameter.")}

    start_time = time.time()

    # S3에서 댓글 데이터 가져오기
    comments = fetch_comments_from_s3(video_id)
    if not comments:
        return {
            "statusCode": 404,
            "body": json.dumps(
                f"Video with ID '{video_id}' not found or no comments available."
            ),
        }
    # 댓글 분류
    classified_comments = classify_comments(comments, batch_size=batch_size)

    # 분류된 댓글을 CSV 파일로 저장
    csv_file = save_comments_to_csv(classified_comments)
    if not csv_file:
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to save comments to CSV."),
        }

    # CSV 파일을 S3에 업로드
    s3_uri = upload_comments_to_s3(csv_file, video_id)
    if not s3_uri:
        return {"statusCode": 500, "body": json.dumps("Failed to upload CSV to S3.")}

    end_time = time.time()
    elapsed_time = end_time - start_time

    # 성공 응답 반환
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "댓글 분류 및 S3 업로드가 성공적으로 완료되었습니다.",
                "s3_uri": s3_uri,
                "elapsed_time_seconds": round(elapsed_time, 2),
            },
            ensure_ascii=False,
        ),
    }


def main():
    # 가상의 Lambda 이벤트 생성
    event = {
        "video_id": "SWzyztkoagc_recent_segment_1",
        "batch_size": 100,
    }

    # 가상의 컨텍스트 (필요 시 추가)
    context = None

    # Lambda 핸들러 호출
    response = lambda_handler(event, context)

    # 응답 출력
    print("Lambda 함수 응답:")
    print(json.dumps(response, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()
