import io
import csv
import re, textwrap
import traceback
import os
import json
import time
import logging
from typing import List, Dict, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import boto3
import openai

# Configure Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# 환경 변수에서 설정 불러오기
S3_BUCKET = os.environ.get("S3_BUCKET")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not S3_BUCKET or not OPENAI_API_KEY:
    logger.critical("필수 환경 변수가 설정되지 않았습니다: S3_BUCKET, OPENAI_API_KEY")
    raise EnvironmentError(
        "필수 환경 변수가 설정되지 않았습니다: S3_BUCKET, OPENAI_API_KEY"
    )

# OpenAI API 키 설정
openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
s3_client = boto3.client("s3")


def fetch_comments_from_s3(video_id: str) -> List[Dict[str, Any]]:
    """
    S3에서 댓글 CSV 파일을 가져와 리스트로 반환합니다.
    파일명은 'comments_{video_id}.csv'로 가정합니다.
    """
    filename = f"unclassified/comments_{video_id}.csv"
    logger.info(
        f"Fetching comments from S3 bucket '{S3_BUCKET}' with filename '{filename}'."
    )
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=filename)
        content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        comments = [row for row in reader]
        logger.info(
            f"S3에서 '{filename}' 파일을 성공적으로 가져왔습니다. 총 댓글 수: {len(comments)}"
        )
        return comments
    except s3_client.exceptions.NoSuchKey:
        logger.warning(
            f"S3 버킷 '{S3_BUCKET}'에 '{filename}' 파일이 존재하지 않습니다."
        )
        return []
    except Exception as e:
        logger.error(f"S3에서 파일을 가져오는 중 오류 발생: {e}", exc_info=True)
        return []


def upload_comments_batch_to_s3(
    file_obj: io.StringIO, video_id: str, batch_number: int
) -> str:
    """
    분류된 댓글 데이터를 배치별 CSV 파일로 S3에 업로드하고 S3 URI를 반환합니다.
    파일명은 'classified/classified_comments_{video_id}_batch_{batch_number}.csv'로 설정합니다.
    """
    filename = (
        f"classified/{video_id}/classified_comments_{video_id}_batch_{batch_number}.csv"
    )
    logger.info(f"Uploading batch {batch_number} to S3 with filename '{filename}'.")
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=file_obj.getvalue())
        s3_uri = f"s3://{S3_BUCKET}/{filename}"
        logger.info(f"배치 {batch_number}의 CSV 파일이 S3에 업로드되었습니다: {s3_uri}")
        return s3_uri
    except Exception as e:
        logger.error(
            f"S3 업로드 중 오류 발생 for batch {batch_number}: {e}", exc_info=True
        )
        return ""


@retry(
    stop=stop_after_attempt(3),  # 최대 3번 재시도
    wait=wait_exponential(
        multiplier=1, min=60, max=70
    ),  # 재시도 간 대기 시간 (60초 ~ 70초)
    retry=retry_if_exception_type(Exception),  # 모든 예외에 대해 재시도
)
def call_openai_api(prompt: str, response_format: Dict) -> Dict:
    """
    OpenAI API를 호출하여 응답을 반환합니다.
    """
    logger.debug("Calling OpenAI API.")
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
        logger.debug("OpenAI API 응답을 성공적으로 받았습니다.")
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logger.error(f"OpenAI API 호출 중 오류 발생: {e}", exc_info=True)
        raise


def classify_and_upload_comments(
    comments: List[Dict[str, Any]], video_id: str, batch_size: int = 50
) -> List[str]:
    """
    GPT API를 사용하여 댓글을 분류하고 키워드를 추출한 후, 각 배치를 S3에 업로드합니다.
    각 배치 업로드의 S3 URI를 리스트로 반환합니다.
    """
    logger.info(
        f"Starting classification and upload for video_id: {video_id} with batch_size: {batch_size}"
    )

    classification_prompt_template = """
    Classify the following YouTube comments (given in the format `comment_id:comment`) into the specified categories with detailed output for each dimension:
    1. **Sentiment**: Assign one of the following emotions: JOY, ANGER, SADNESS, SURPRISE, FEAR, DISGUST.
    2. **PositiveStatus**: Determine the overall sentiment as POSITIVE, NEGATIVE, or NEUTRAL.
    3. **CommentCategory**: Classify the comment into one of these categories: REACTION, FEEDBACK, QUESTION, SPAM, INSULT.

    Additionally:
    - Focus on retaining the exact characters in `comment_id` without any changes or omissions.
    - Ensure differentiation between IDs with similar but distinct strings, as shown in the input example above.
    - Extract exactly 3 keywords that best represent the comment's content, excluding meaningless symbols, common stopwords, and adjectives. Keywords should be nouns or verbs.

    **Comments:**
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

    s3_uris = []
    total_batches = (len(comments) + batch_size - 1) // batch_size
    logger.info(f"Total batches to process: {total_batches}")

    for batch_num, i in enumerate(range(0, len(comments), batch_size), start=1):
        batch_comments = comments[i : i + batch_size]
        logger.info(
            f"Processing batch {batch_num}/{total_batches} with {len(batch_comments)} comments."
        )

        prompt = classification_prompt_template

        for comment in batch_comments:
            prompt += f"{comment['comment_id']} : {comment['text_display']},\n"

        try:
            classified = call_openai_api(prompt, response_format)

            # 분류용 딕셔너리 생성
            classification_dict = {
                c["comment_id"]: c for c in classified.get("comments", [])
            }

            # 업데이트된 댓글에 분류 정보 추가
            for comment in batch_comments:
                class_result = classification_dict.get(comment["comment_id"], {})
                comment["sentiment"] = (
                    class_result.get("sentiment", "UNKNOWN")
                    if class_result.get("sentiment") in SENTIMENT_ENUM
                    else "UNKNOWN"
                )
                comment["positive_status"] = (
                    class_result.get("positive_status", "UNKNOWN")
                    if class_result.get("positive_status") in POSITIVE_STATUS_ENUM
                    else "UNKNOWN"
                )
                comment["comment_category"] = (
                    class_result.get("comment_category", "UNKNOWN")
                    if class_result.get("comment_category") in COMMENT_CATEGORY_ENUM
                    else "UNKNOWN"
                )
                comment["keywords"] = class_result.get("keywords", [])

            logger.debug(f"Batch {batch_num}: Classification completed.")

            # CSV 파일 생성
            csv_file = save_comments_to_csv(batch_comments)
            if not csv_file:
                logger.error(f"Batch {batch_num}: CSV 파일 생성 실패.")
                continue

            # S3에 업로드
            s3_uri = upload_comments_batch_to_s3(csv_file, video_id, batch_num)
            if s3_uri:
                s3_uris.append(s3_uri)
                logger.info(f"Batch {batch_num}: Uploaded to {s3_uri}")
            else:
                logger.error(f"Batch {batch_num}: S3 업로드 실패.")

        except Exception as e:
            logger.error(
                f"Batch {batch_num}: GPT API 호출 중 오류 발생: {e}", exc_info=True
            )
            # 오류 발생 시 기본값 설정
            for comment in batch_comments:
                comment["sentiment"] = "UNKNOWN"
                comment["positive_status"] = "UNKNOWN"
                comment["comment_category"] = "UNKNOWN"
                comment["keywords"] = []

    logger.info("Classification and upload process completed.")
    return s3_uris


def save_comments_to_csv(comments: List[Dict[str, Any]]) -> io.StringIO:
    """
    댓글 데이터를 CSV 파일로 저장하고 StringIO 객체를 반환합니다.
    """
    if not comments:
        logger.warning("저장할 댓글이 없습니다.")
        return None

    # CSV 헤더 정의
    headers = list(comments[0].keys())

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()

    for comment in comments:
        writer.writerow(comment)

    output.seek(0)
    logger.debug("CSV 파일이 성공적으로 생성되었습니다.")
    return output


def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    이벤트는 JSON 형식으로 `video_id`를 포함해야 합니다.
    추가로 `batch_size`를 포함할 수 있습니다.
    """
    logger.info("Lambda function invoked.")
    # 이벤트에서 필요한 데이터 추출
    try:
        video_id = event["video_id"]
        batch_size = int(event.get("batch_size", 20))  # 배치 크기 변수 추가, 기본값 20
        logger.debug(f"Received video_id: {video_id}, batch_size: {batch_size}")
    except KeyError as e:
        logger.error(f"Missing parameter: {e}")
        return {"statusCode": 400, "body": json.dumps(f"Missing parameter: {e}")}
    except ValueError:
        logger.error("Invalid batch_size parameter.")
        return {"statusCode": 400, "body": json.dumps("Invalid batch_size parameter.")}

    start_time = time.time()

    # S3에서 댓글 데이터 가져오기
    comments = fetch_comments_from_s3(video_id)
    if not comments:
        logger.warning(
            f"Video with ID '{video_id}' not found or no comments available."
        )
        return {
            "statusCode": 404,
            "body": json.dumps(
                f"Video with ID '{video_id}' not found or no comments available."
            ),
        }

    # 댓글 분류 및 배치별 S3 업로드
    s3_uris = classify_and_upload_comments(comments, video_id, batch_size=batch_size)

    if not s3_uris:
        logger.error("Failed to classify and upload comments.")
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to classify and upload comments."),
        }

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Processing completed in {elapsed_time:.2f} seconds.")

    # 성공 응답 반환
    response = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "댓글 분류 및 배치별 S3 업로드가 성공적으로 완료되었습니다.",
                "s3_uris": s3_uris,
                "elapsed_time_seconds": round(elapsed_time, 2),
            },
            ensure_ascii=False,
        ),
    }
    logger.info("Lambda function completed successfully.")
    return response


def main():
    # 가상의 Lambda 이벤트 생성
    event = {
        "video_id": "5IWvoKOLX4Y",
        "batch_size": 100,
    }

    # 가상의 컨텍스트 (필요 시 추가)
    context = None

    # Lambda 핸들러 호출
    response = lambda_handler(event, context)

    # 응답 출력
    logger.info("Lambda 함수 응답:")
    print(json.dumps(response, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()
