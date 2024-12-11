import os
import json
import logging
from dataclasses import dataclass, asdict
from typing import List, Optional
import google.oauth2.credentials
import googleapiclient.discovery
import googleapiclient.errors

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
        print(data)
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

        # 응답 데이터 구성
        response_data = {"channel": asdict(channel)}

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
