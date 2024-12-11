import json
import csv
import io
import os
import time
from dataclasses import dataclass, asdict
from typing import Optional, List

import boto3
import google.oauth2.credentials
import googleapiclient.discovery
import googleapiclient.errors

# 경고 메시지 숨기기
import warnings

warnings.filterwarnings("ignore")

# 사용할 OAuth2 범위 정의
SCOPES = [
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

# 환경 변수에서 클라이언트 ID와 시크릿을 가져옵니다.
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
S3_BUCKET = os.environ.get("S3_BUCKET")

if not CLIENT_ID or not CLIENT_SECRET or not S3_BUCKET:
    raise EnvironmentError(
        "필수 환경 변수가 설정되지 않았습니다: CLIENT_ID, CLIENT_SECRET, S3_BUCKET"
    )


@dataclass
class YouTubeVideo:
    """YouTube 동영상 정보를 담는 클래스"""

    id: str
    published_at: str
    channel_id: str
    title: str
    description: str
    thumbnail_url: str
    thumbnail_width: int
    thumbnail_height: int
    channel_title: str
    category_id: str
    live_broadcast_content: str
    view_count: int
    like_count: int
    comment_count: int

    @staticmethod
    def from_dict(data: dict) -> "YouTubeVideo":
        snippet = data.get("snippet", {})
        statistics = data.get("statistics", {})
        thumbnails = snippet.get("thumbnails", {}).get("standard", {})

        return YouTubeVideo(
            id=data.get("id", ""),
            published_at=snippet.get("publishedAt", ""),
            channel_id=snippet.get("channelId", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description", ""),
            thumbnail_url=thumbnails.get("url", ""),
            thumbnail_width=thumbnails.get("width", 0),
            thumbnail_height=thumbnails.get("height", 0),
            channel_title=snippet.get("channelTitle", ""),
            category_id=snippet.get("categoryId", ""),
            live_broadcast_content=snippet.get("liveBroadcastContent", ""),
            view_count=int(statistics.get("viewCount", 0)),
            like_count=int(statistics.get("likeCount", 0)),
            comment_count=int(statistics.get("commentCount", 0)),
        )


@dataclass
class AuthorChannelId:
    """작성자의 채널 ID를 저장하는 클래스"""

    value: str


@dataclass
class YouTubeComment:
    """YouTube 댓글 정보를 담는 클래스"""

    comment_id: str
    channel_id: str
    video_id: str
    text_display: str
    text_original: str
    author_display_name: str
    author_profile_image_url: str
    author_channel_url: str
    author_channel_id: Optional[AuthorChannelId]
    like_count: int
    published_at: str
    updated_at: str
    reply: bool

    @staticmethod
    def from_dict(data: dict, reply: bool = False) -> "YouTubeComment":
        author_channel_id_data = data.get("authorChannelId", {})
        author_channel_id = (
            AuthorChannelId(value=author_channel_id_data.get("value", ""))
            if author_channel_id_data
            else None
        )

        return YouTubeComment(
            comment_id=data.get("comment_id", ""),
            channel_id=data.get("channelId", ""),
            video_id=data.get("videoId", ""),
            text_display=data.get("textDisplay", ""),
            text_original=data.get("textOriginal", ""),
            author_display_name=data.get("authorDisplayName", ""),
            author_profile_image_url=data.get("authorProfileImageUrl", ""),
            author_channel_url=data.get("authorChannelUrl", ""),
            author_channel_id=author_channel_id,
            like_count=data.get("likeCount", 0),
            published_at=data.get("publishedAt", ""),
            updated_at=data.get("updatedAt", ""),
            reply=reply,
        )


def authenticate(
    refresh_token: str, access_token: str
) -> google.oauth2.credentials.Credentials:
    """
    제공된 refresh_token과 access_token을 사용하여 자격 증명을 생성합니다.
    """
    credentials = google.oauth2.credentials.Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=SCOPES,
    )
    return credentials


def build_youtube_client(credentials):
    """
    YouTube API 클라이언트를 생성하여 반환합니다.
    """
    youtube_client = googleapiclient.discovery.build(
        "youtube", "v3", credentials=credentials, cache_discovery=False
    )
    return youtube_client


def get_video_info(youtube_client, video_id) -> Optional[YouTubeVideo]:
    """
    주어진 동영상 ID의 정보를 가져와 YouTubeVideo 객체로 반환합니다.
    """
    try:
        response = (
            youtube_client.videos()
            .list(part="snippet,statistics", id=video_id)
            .execute()
        )
        items = response.get("items", [])
        if not items:
            print(f"ID가 '{video_id}'인 동영상을 찾을 수 없습니다.")
            return None
        video_data = items[0]
        video_info = YouTubeVideo.from_dict(video_data)
        return video_info
    except googleapiclient.errors.HttpError as e:
        print(f"오류 발생: {e}")
        return None


def crawl_comments(youtube_client, video_id) -> List[YouTubeComment]:
    """
    주어진 동영상 ID의 모든 댓글과 답글을 크롤링합니다.
    YouTubeComment 객체 리스트를 반환합니다.
    """
    comments = []
    video_title = get_video_title(youtube_client, video_id)
    if not video_title:
        return comments

    print(f"동영상: '{video_title}' (ID: {video_id})의 댓글을 가져오는 중...")

    # 댓글 스레드 초기 요청
    request = youtube_client.commentThreads().list(
        part="id,snippet", videoId=video_id, maxResults=100, textFormat="plainText"
    )

    while request:
        try:
            response = request.execute()
        except googleapiclient.errors.HttpError as e:
            print(f"HTTP 오류 발생: {e}")
            break

        for thread in response.get("items", []):
            top_comment_snippet = thread["snippet"]["topLevelComment"]["snippet"]
            top_comment_data = {
                "comment_id": thread["id"],
                "channelId": top_comment_snippet.get("channelId", ""),
                "videoId": video_id,
                "textDisplay": top_comment_snippet.get("textDisplay", ""),
                "textOriginal": top_comment_snippet.get("textOriginal", ""),
                "authorDisplayName": top_comment_snippet.get("authorDisplayName", ""),
                "authorProfileImageUrl": top_comment_snippet.get(
                    "authorProfileImageUrl", ""
                ),
                "authorChannelUrl": top_comment_snippet.get("authorChannelUrl", ""),
                "authorChannelId": top_comment_snippet.get("authorChannelId", {}),
                "likeCount": top_comment_snippet.get("likeCount", 0),
                "publishedAt": top_comment_snippet.get("publishedAt", ""),
                "updatedAt": top_comment_snippet.get("updatedAt", ""),
            }

            top_comment = YouTubeComment.from_dict(top_comment_data, reply=False)
            comments.append(top_comment)

            # 답글이 있는지 확인
            if thread["snippet"]["totalReplyCount"] > 0:
                reply_request = youtube_client.comments().list(
                    part="snippet",
                    parentId=thread["id"],
                    maxResults=100,
                    textFormat="plainText",
                )
                while reply_request:
                    try:
                        reply_response = reply_request.execute()
                    except googleapiclient.errors.HttpError as e:
                        print(f"HTTP 오류 발생: {e}")
                        break

                    for reply in reply_response.get("items", []):
                        reply_snippet = reply["snippet"]
                        reply_data = {
                            "comment_id": reply["id"],
                            "channelId": reply_snippet.get("channelId", ""),
                            "videoId": video_id,
                            "textDisplay": reply_snippet.get("textDisplay", ""),
                            "textOriginal": reply_snippet.get("textOriginal", ""),
                            "authorDisplayName": reply_snippet.get(
                                "authorDisplayName", ""
                            ),
                            "authorProfileImageUrl": reply_snippet.get(
                                "authorProfileImageUrl", ""
                            ),
                            "authorChannelUrl": reply_snippet.get(
                                "authorChannelUrl", ""
                            ),
                            "authorChannelId": reply_snippet.get("authorChannelId", {}),
                            "likeCount": reply_snippet.get("likeCount", 0),
                            "publishedAt": reply_snippet.get("publishedAt", ""),
                            "updatedAt": reply_snippet.get("updatedAt", ""),
                        }

                        reply_comment = YouTubeComment.from_dict(reply_data, reply=True)
                        comments.append(reply_comment)

                    # 답글의 다음 페이지가 있는지 확인
                    if "nextPageToken" in reply_response:
                        reply_request = youtube_client.comments().list(
                            part="snippet",
                            parentId=thread["id"],
                            maxResults=100,
                            pageToken=reply_response["nextPageToken"],
                            textFormat="plainText",
                        )
                    else:
                        reply_request = None

        # 댓글 스레드의 다음 페이지가 있는지 확인
        if "nextPageToken" in response:
            request = youtube_client.commentThreads().list(
                part="id,snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=response["nextPageToken"],
                textFormat="plainText",
            )
        else:
            request = None

    return comments


def get_video_title(youtube_client, video_id):
    """
    주어진 동영상 ID의 제목을 가져옵니다.
    """
    try:
        response = youtube_client.videos().list(part="snippet", id=video_id).execute()
        items = response.get("items", [])
        if not items:
            print(f"ID가 '{video_id}'인 동영상을 찾을 수 없습니다.")
            return None
        return items[0]["snippet"]["title"]
    except googleapiclient.errors.HttpError as e:
        print(f"오류 발생: {e}")
        return None


def save_comments_to_csv(comments: List[YouTubeComment], video_id: str) -> io.StringIO:
    """
    YouTubeComment 객체 리스트를 CSV 파일로 저장합니다.
    메모리 내에서 StringIO 객체로 반환합니다.
    """
    if not comments:
        print("저장할 댓글이 없습니다.")
        return None

    # CSV 파일 이름 정의 (날짜 제거)
    filename = f"comments_{video_id}.csv"

    # CSV 헤더 정의 (한글 및 영어 병기)
    headers = [
        "comment_id",
        "channel_id",
        "video_id",
        "text_display",
        "text_original",
        "author_display_name",
        "author_profile_image_url",
        "author_channel_url",
        "author_channel_id",
        "like_count",
        "published_at",
        "updated_at",
        "reply",
    ]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)  # 헤더 작성

    for comment in comments:
        writer.writerow(
            [
                comment.comment_id,
                comment.channel_id,
                comment.video_id,
                comment.text_display,
                comment.text_original,
                comment.author_display_name,
                comment.author_profile_image_url,
                comment.author_channel_url,
                comment.author_channel_id.value if comment.author_channel_id else "",
                comment.like_count,
                comment.published_at,
                comment.updated_at,
                "reply" if comment.reply else "comment",
            ]
        )

    output.seek(0)
    return output


def upload_to_s3(file_obj: io.StringIO, filename: str) -> str:
    """
    StringIO 객체를 S3 버킷에 업로드하고, 업로드된 파일의 S3 URI를 반환합니다.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=filename, Body=file_obj.getvalue())
        s3_uri = f"s3://{S3_BUCKET}/{filename}"
        print(f"CSV 파일이 S3에 업로드되었습니다: {s3_uri}")
        return s3_uri
    except Exception as e:
        print(f"S3 업로드 중 오류 발생: {e}")
        return None


def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    이벤트는 JSON 형식으로 `refresh_token`, `access_token`, `video_id`를 포함해야 합니다.
    """
    # 이벤트에서 필요한 데이터 추출
    try:
        refresh_token = event["refresh_token"]
        access_token = event["access_token"]
        video_id = event["video_id"]
    except KeyError as e:
        return {"statusCode": 400, "body": json.dumps(f"Missing parameter: {e}")}

    start_time = time.time()

    # 인증 및 YouTube 클라이언트 생성
    credentials = authenticate(refresh_token, access_token)
    youtube_client = build_youtube_client(credentials)

    # 동영상 정보 가져오기
    video_info = get_video_info(youtube_client, video_id)
    if not video_info:
        return {
            "statusCode": 404,
            "body": json.dumps(f"Video with ID '{video_id}' not found."),
        }

    # 동영상 정보 출력 (옵션)
    # print_video_info(video_info)  # Lambda에서는 로그로 출력됩니다.

    # 댓글 크롤링
    comments = crawl_comments(youtube_client, video_id)

    # 댓글을 CSV 파일로 저장
    csv_file = save_comments_to_csv(comments, video_id)
    if not csv_file:
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to save comments to CSV."),
        }

    # CSV 파일을 S3에 업로드
    # 파일 이름을 video_id만 포함하도록 수정
    filename = f"unclassified/comments_{video_id}.csv"
    s3_uri = upload_to_s3(csv_file, filename)
    if not s3_uri:
        return {"statusCode": 500, "body": json.dumps("Failed to upload CSV to S3.")}

    end_time = time.time()
    elapsed_time = end_time - start_time

    # 성공 응답 반환
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "댓글 수집 및 S3 업로드가 성공적으로 완료되었습니다.",
                "s3_uri": s3_uri,
                "elapsed_time_seconds": round(elapsed_time, 2),
            },
            ensure_ascii=False,
        ),
    }
