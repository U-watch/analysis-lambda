import os
import json
import logging
import csv
import io
from dataclasses import dataclass, asdict
from typing import List, Optional

import boto3
import google.oauth2.credentials
import googleapiclient.discovery
import googleapiclient.errors

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 버킷 이름 환경 변수에서 가져오기
S3_BUCKET = os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    raise EnvironmentError("필수 환경 변수가 설정되지 않았습니다: S3_BUCKET")


@dataclass
class YouTubeChannel:
    """YouTube 채널 정보를 담는 클래스"""

    id: str  # 채널의 고유 ID
    title: str  # 채널 제목
    description: str  # 채널 설명
    custom_url: Optional[str]  # 커스텀 URL
    published_at: str  # 채널 개설 일시 (ISO 8601 형식)
    thumbnail_url: str  # 채널 썸네일 URL
    country: Optional[str]  # 채널 국가
    view_count: int  # 전체 조회수
    subscriber_count: Optional[int]  # 구독자 수
    video_count: int  # 업로드된 동영상 수
    uploads_playlist_id: str  # 업로드된 동영상 플레이리스트 ID

    @staticmethod
    def from_dict(data: dict) -> "YouTubeChannel":
        snippet = data.get("snippet", {})
        statistics = data.get("statistics", {})
        content_details = data.get("contentDetails", {})
        thumbnails = snippet.get("thumbnails", {}).get("default", {})

        return YouTubeChannel(
            id=data.get("id", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description", ""),
            custom_url=snippet.get("customUrl"),
            published_at=snippet.get("publishedAt", ""),
            thumbnail_url=thumbnails.get("url", ""),
            country=snippet.get("country"),
            view_count=int(statistics.get("viewCount", 0)),
            subscriber_count=(
                int(statistics.get("subscriberCount", 0))
                if "subscriberCount" in statistics
                else None
            ),
            video_count=int(statistics.get("videoCount", 0)),
            uploads_playlist_id=content_details.get("relatedPlaylists", {}).get(
                "uploads", ""
            ),
        )


@dataclass
class YouTubeVideo:
    """YouTube 동영상 정보를 담는 클래스"""

    id: str  # 동영상의 고유 ID
    published_at: str  # 동영상 게시 일시 (ISO 8601 형식)
    channel_id: str  # 동영상 업로드자의 채널 ID
    title: str  # 동영상 제목
    description: str  # 동영상 설명
    duration: str  # 동영상 길이 (ISO 8601 형식)
    thumbnail_url: str  # 썸네일 URL
    thumbnail_width: int  # 썸네일 너비
    thumbnail_height: int  # 썸네일 높이
    channel_title: str  # 채널 이름
    category_id: str  # 동영상 카테고리 ID
    live_broadcast_content: str  # 라이브 방송 여부 ('none', 'upcoming', 'live')
    view_count: int  # 조회수
    like_count: int  # 좋아요 수
    comment_count: int  # 댓글 수

    @staticmethod
    def from_dict(data: dict) -> "YouTubeVideo":
        snippet = data.get("snippet", {})
        statistics = data.get("statistics", {})
        duration = data.get("contentDetails", {}).get("duration", "PT0S")
        thumbnails = (
            snippet.get("thumbnails", {}).get("standard", {})
            or snippet.get("thumbnails", {}).get("high", {})
            or snippet.get("thumbnails", {}).get("default", {})
        )

        return YouTubeVideo(
            id=data.get("id", ""),
            published_at=snippet.get("publishedAt", ""),
            channel_id=snippet.get("channelId", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description", ""),
            duration=duration,
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


def build_youtube_client(
    access_token: str,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    token_uri: str,
    scopes: List[str],
) -> googleapiclient.discovery.Resource:
    """
    YouTube API 클라이언트를 빌드하는 함수.

    Args:
        access_token (str): 접근 토큰
        refresh_token (str): 갱신 토큰
        client_id (str): Google API 클라이언트 ID
        client_secret (str): Google API 클라이언트 시크릿
        token_uri (str): 토큰 URI
        scopes (list): API 접근 권한 스코프 목록

    Returns:
        googleapiclient.discovery.Resource: 빌드된 YouTube API 클라이언트
    """
    credentials = google.oauth2.credentials.Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )

    youtube_client = googleapiclient.discovery.build(
        "youtube",
        "v3",
        credentials=credentials,
    )

    return youtube_client


def get_channel_details(
    youtube_client: googleapiclient.discovery.Resource, handle: Optional[str] = None
) -> YouTubeChannel:
    """
    YouTube 채널 정보를 가져오는 함수.

    Args:
        youtube_client (googleapiclient.discovery.Resource): YouTube API 클라이언트
        handle (str, optional): 채널 핸들. 기본값은 None (mine=True로 본인 채널 사용).

    Returns:
        YouTubeChannel: 채널 정보 객체
    """
    try:
        if handle:
            channel_response = (
                youtube_client.channels()
                .list(part="snippet,statistics,contentDetails", forHandle=handle)
                .execute()
            )
        else:
            channel_response = (
                youtube_client.channels()
                .list(part="snippet,statistics,contentDetails", mine=True)
                .execute()
            )
        logger.info(f"channel_response: {channel_response}")

        if not channel_response.get("items"):
            raise ValueError(
                "No channel found with the provided handle or authenticated user."
            )

        channel = YouTubeChannel.from_dict(channel_response["items"][0])
        return channel
    except googleapiclient.errors.HttpError as e:
        logger.error(f"Failed to fetch channel details: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while fetching channel details: {e}")
        raise


def get_video_details_batch(
    youtube_client: googleapiclient.discovery.Resource, video_ids: List[str]
) -> List[dict]:
    """
    여러 동영상의 상세 정보를 한 번에 가져오는 함수.

    Args:
        youtube_client (googleapiclient.discovery.Resource): YouTube API 클라이언트
        video_ids (List[str]): 동영상 ID 리스트

    Returns:
        List[dict]: 동영상 상세 정보 리스트
    """
    try:
        video_response = (
            youtube_client.videos()
            .list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids),
                maxResults=50,
            )
            .execute()
        )
        logger.info(f"video_response batch: {video_response}")

        return video_response.get("items", [])
    except googleapiclient.errors.HttpError as e:
        logger.error(f"Failed to fetch video details batch: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while fetching video details batch: {e}")
        raise


def get_videos_from_playlist(
    youtube_client: googleapiclient.discovery.Resource, playlist_id: str
) -> List[YouTubeVideo]:
    """
    주어진 플레이리스트에서 동영상 목록을 가져오는 함수.

    Args:
        youtube_client (googleapiclient.discovery.Resource): YouTube API 클라이언트
        playlist_id (str): 가져올 플레이리스트 ID

    Returns:
        list: YouTubeVideo 객체 리스트
    """
    try:
        videos = []
        next_page_token = None

        while True:
            playlist_response = (
                youtube_client.playlistItems()
                .list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token,
                )
                .execute()
            )
            logger.info(f"playlist_response: {playlist_response}")

            video_ids = [
                item["contentDetails"]["videoId"]
                for item in playlist_response.get("items", [])
            ]

            if video_ids:
                video_details_list = get_video_details_batch(youtube_client, video_ids)
                for video_details in video_details_list:
                    video = YouTubeVideo.from_dict(video_details)
                    videos.append(video)

            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break

        return videos

    except googleapiclient.errors.HttpError as e:
        logger.error(f"Failed to fetch videos from playlist: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while fetching videos from playlist: {e}")
        raise


def save_videos_to_csv(videos: List[YouTubeVideo]) -> io.StringIO:
    """
    YouTubeVideo 객체 리스트를 CSV 파일로 저장합니다.
    메모리 내에서 StringIO 객체로 반환합니다.
    """
    if not videos:
        logger.info("저장할 동영상이 없습니다.")
        return None

    # CSV 헤더 정의
    headers = [
        "video_id",
        "published_at",
        "channel_id",
        "title",
        "description",
        "duration",
        "thumbnail_url",
        "thumbnail_width",
        "thumbnail_height",
        "channel_title",
        "category_id",
        "live_broadcast_content",
        "view_count",
        "like_count",
        "comment_count",
    ]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)  # 헤더 작성

    for video in videos:
        writer.writerow(
            [
                video.id,
                video.published_at,
                video.channel_id,
                video.title,
                video.description,
                video.duration,
                video.thumbnail_url,
                video.thumbnail_width,
                video.thumbnail_height,
                video.channel_title,
                video.category_id,
                video.live_broadcast_content,
                video.view_count,
                video.like_count,
                video.comment_count,
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
        file_location = f"videos/{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=file_location, Body=file_obj.getvalue()
        )
        s3_uri = f"s3://{S3_BUCKET}/{filename}"
        logger.info(f"CSV 파일이 S3에 업로드되었습니다: {s3_uri}")
        return s3_uri
    except Exception as e:
        logger.error(f"S3 업로드 중 오류 발생: {e}")
        return None


def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.

    Args:
        event (dict): 이벤트 데이터
        context: Lambda 실행 환경 정보

    Returns:
        dict: HTTP 응답
    """
    try:
        # 이벤트에서 토큰 추출
        access_token = event.get("access_token")
        refresh_token = event.get("refresh_token")
        handle = event.get("handle", None)

        if not access_token or not refresh_token:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing access_token or refresh_token."}),
            }

        # 환경 변수에서 Google API 자격 증명 가져오기
        client_id = os.environ.get("GOOGLE_CLIENT_ID")
        client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
        token_uri = os.environ.get("GOOGLE_TOKEN_URI")

        if not all([client_id, client_secret, token_uri]):
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {"error": "Google API credentials are not properly configured."}
                ),
            }

        # 필요한 스코프 정의
        scopes = [
            "https://www.googleapis.com/auth/youtube.force-ssl",
            "https://www.googleapis.com/auth/youtube",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email",
            "openid",
        ]

        # YouTube 클라이언트 빌드
        youtube_client = build_youtube_client(
            access_token=access_token,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri=token_uri,
            scopes=scopes,
        )

        # 채널 정보 가져오기
        channel = get_channel_details(youtube_client, handle)
        uploads_playlist_id = channel.uploads_playlist_id

        if not uploads_playlist_id:
            raise ValueError("Uploads playlist ID is not available for this channel.")

        # 업로드된 동영상 가져오기
        videos = get_videos_from_playlist(youtube_client, uploads_playlist_id)

        if not videos:
            raise ValueError("No videos found in the uploads playlist.")

        # 최근 10개 동영상과 나머지 동영상 분리
        recent_videos = videos[:10]

        if videos:
            csv_file = save_videos_to_csv(videos)
            if not csv_file:
                raise ValueError("Failed to save videos to CSV.")

            # CSV 파일을 S3에 업로드
            filename = f"videos_{channel.id}.csv"
            s3_uri = upload_to_s3(csv_file, filename)

            if not s3_uri:
                raise ValueError("Failed to upload CSV to S3.")
        else:
            s3_uri = None

        # 응답 데이터 구성 (최근 10개 동영상만 포함)
        response_data = {
            "channel": asdict(channel),
            "videos": [asdict(video) for video in recent_videos],
        }

        if s3_uri:
            response_data["additional_videos_csv_s3_uri"] = s3_uri

        return {
            "statusCode": 200,
            "body": json.dumps(response_data, ensure_ascii=False),
        }

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(ve)}),
        }
    except googleapiclient.errors.HttpError as he:
        logger.error(f"HTTPError: {he}")
        return {
            "statusCode": 502,
            "body": json.dumps({"error": "Bad Gateway. External API error."}),
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error."}),
        }
