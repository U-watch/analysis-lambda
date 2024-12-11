import json
import csv
import os
import time  # time 모듈 추가
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List

import google.oauth2.credentials
import google_auth_oauthlib.flow
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

CLIENT_SECRETS_FILE = "client_secret.json"  # 클라이언트 시크릿 파일 이름
TOKEN_FILE = "youtube_token.json"  # 토큰 파일 이름
CSV_FOLDER = "csv"  # CSV 파일을 저장할 폴더 이름

# CSV 폴더가 존재하지 않으면 생성
os.makedirs(CSV_FOLDER, exist_ok=True)


@dataclass
class YouTubeVideo:
    """YouTube 동영상 정보를 담는 클래스"""

    id: str  # 동영상의 고유 ID
    published_at: str  # 동영상 게시 일시 (ISO 8601 형식)
    channel_id: str  # 동영상 업로드자의 채널 ID
    title: str  # 동영상 제목
    description: str  # 동영상 설명
    thumbnail_url: str  # 기본 썸네일 URL
    thumbnail_width: int  # 기본 썸네일 너비
    thumbnail_height: int  # 기본 썸네일 높이
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

    value: str  # 채널 ID 값


@dataclass
class YouTubeComment:
    """YouTube 댓글 정보를 담는 클래스"""

    comment_id: str  # 댓글의 고유 ID
    channel_id: str  # 댓글 작성자의 채널 ID
    video_id: str  # 댓글이 달린 동영상의 ID
    text_display: str  # 댓글의 표시 텍스트
    text_original: str  # 댓글의 원본 텍스트
    author_display_name: str  # 댓글 작성자의 표시 이름
    author_profile_image_url: str  # 댓글 작성자의 프로필 사진 URL
    author_channel_url: str  # 댓글 작성자의 채널 URL
    author_channel_id: Optional[AuthorChannelId]  # 댓글 작성자의 채널 ID 객체
    like_count: int  # 댓글의 좋아요 수
    published_at: str  # 댓글이 게시된 날짜 및 시간 (ISO 8601 형식)
    updated_at: str  # 댓글이 업데이트된 날짜 및 시간 (ISO 8601 형식)
    reply: bool  # 댓글이 답글인지 여부

    @staticmethod
    def from_dict(data: dict, reply: bool = False) -> "YouTubeComment":
        """
        주어진 딕셔너리 데이터를 사용하여 YouTubeComment 객체를 생성합니다.
        """
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


def authenticate():
    """
    사용자를 인증하고 자격 증명을 반환합니다.
    """
    # 토큰 파일이 존재하는지 확인
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as token:
            token_data = json.load(token)
            credentials = (
                google.oauth2.credentials.Credentials.from_authorized_user_info(
                    token_data, SCOPES
                )
            )
    else:
        # OAuth 흐름 실행
        oauth_flow = (
            google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES
            )
        )
        credentials = oauth_flow.run_local_server(port=0)
        # 자격 증명을 저장하여 이후에 재사용
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())

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
            print(f"ID가 '{video_id}'인 동영상을 찾을 수 없습니다.\n")
            return None
        video_data = items[0]
        video_info = YouTubeVideo.from_dict(video_data)
        return video_info
    except googleapiclient.errors.HttpError as e:
        print(f"오류 발생: {e}\n")
        return None


def get_csv_path(filename: str) -> str:
    """
    주어진 파일 이름을 csv 폴더 안의 경로로 변환합니다.
    """
    return os.path.join(CSV_FOLDER, filename)


def save_video_info_to_csv(video: YouTubeVideo):
    """
    YouTubeVideo 객체를 하나의 CSV 파일로 저장합니다.
    모든 필드를 동적으로 포함합니다.
    """
    if not video:
        print("저장할 동영상 정보가 없습니다.\n")
        return

    filename = get_csv_path("all_video_info.csv")

    # YouTubeVideo 데이터 클래스를 딕셔너리로 변환
    video_dict = asdict(video)

    # CSV 헤더 정의 (필드 이름을 원하는 대로 매핑할 수 있음)
    headers = [
        "동영상 ID (video_id)",
        "게시 일시 (published_at)",
        "채널 ID (channel_id)",
        "제목 (title)",
        "설명 (description)",
        "기본 썸네일 URL (thumbnail_url)",
        "기본 썸네일 너비 (thumbnail_width)",
        "기본 썸네일 높이 (thumbnail_height)",
        "채널 이름 (channel_title)",
        "카테고리 ID (category_id)",
        "라이브 방송 여부 (live_broadcast_content)",
        "조회수 (view_count)",
        "좋아요 수 (like_count)",
        "댓글 수 (comment_count)",
    ]

    # 데이터 딕셔너리를 CSV에 맞게 매핑
    row = {
        "동영상 ID (video_id)": video_dict.get("id", ""),
        "게시 일시 (published_at)": video_dict.get("published_at", ""),
        "채널 ID (channel_id)": video_dict.get("channel_id", ""),
        "제목 (title)": video_dict.get("title", ""),
        "설명 (description)": video_dict.get("description", ""),
        "기본 썸네일 URL (thumbnail_url)": video_dict.get("thumbnail_url", ""),
        "기본 썸네일 너비 (thumbnail_width)": video_dict.get("thumbnail_width", 0),
        "기본 썸네일 높이 (thumbnail_height)": video_dict.get("thumbnail_height", 0),
        "채널 이름 (channel_title)": video_dict.get("channel_title", ""),
        "카테고리 ID (category_id)": video_dict.get("category_id", ""),
        "라이브 방송 여부 (live_broadcast_content)": video_dict.get(
            "live_broadcast_content", ""
        ),
        "조회수 (view_count)": video_dict.get("view_count", 0),
        "좋아요 수 (like_count)": video_dict.get("like_count", 0),
        "댓글 수 (comment_count)": video_dict.get("comment_count", 0),
    }

    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode="a", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()  # 헤더 작성
            writer.writerow(row)  # 동영상 정보 작성
        print(f"동영상 정보가 '{filename}' 파일에 저장되었습니다.\n")
    except IOError as e:
        print(f"파일 입출력 오류 발생: {e}\n")


def get_video_title(youtube_client, video_id):
    """
    주어진 동영상 ID의 제목을 가져옵니다.
    """
    try:
        response = youtube_client.videos().list(part="snippet", id=video_id).execute()
        items = response.get("items", [])
        if not items:
            print(f"ID가 '{video_id}'인 동영상을 찾을 수 없습니다.\n")
            return None
        return items[0]["snippet"]["title"]
    except googleapiclient.errors.HttpError as e:
        print(f"오류 발생: {e}\n")
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
            print(f"HTTP 오류 발생: {e}\n")
            break

        for thread in response.get("items", []):
            top_comment_snippet = thread["snippet"]["topLevelComment"]["snippet"]
            top_comment_data = {
                "comment_id": thread["id"],  # 댓글 ID
                "channelId": top_comment_snippet.get("channelId", ""),  # 채널 ID
                "videoId": video_id,  # 동영상 ID
                "textDisplay": top_comment_snippet.get("textDisplay", ""),  # 댓글 내용
                "textOriginal": top_comment_snippet.get(
                    "textOriginal", ""
                ),  # 원본 댓글
                "authorDisplayName": top_comment_snippet.get(
                    "authorDisplayName", ""
                ),  # 작성자 이름
                "authorProfileImageUrl": top_comment_snippet.get(
                    "authorProfileImageUrl", ""
                ),  # 프로필 사진 URL
                "authorChannelUrl": top_comment_snippet.get(
                    "authorChannelUrl", ""
                ),  # 채널 URL
                "authorChannelId": top_comment_snippet.get(
                    "authorChannelId", {}
                ),  # 채널 ID 객체
                "likeCount": top_comment_snippet.get("likeCount", 0),  # 좋아요 수
                "publishedAt": top_comment_snippet.get("publishedAt", ""),  # 게시 일시
                "updatedAt": top_comment_snippet.get("updatedAt", ""),  # 업데이트 일시
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
                        print(f"HTTP 오류 발생: {e}\n")
                        break

                    for reply in reply_response.get("items", []):
                        reply_snippet = reply["snippet"]
                        reply_data = {
                            "comment_id": reply["id"],  # 댓글 ID
                            "channelId": reply_snippet.get("channelId", ""),  # 채널 ID
                            "videoId": video_id,  # 동영상 ID
                            "textDisplay": reply_snippet.get(
                                "textDisplay", ""
                            ),  # 댓글 내용
                            "textOriginal": reply_snippet.get(
                                "textOriginal", ""
                            ),  # 원본 댓글
                            "authorDisplayName": reply_snippet.get(
                                "authorDisplayName", ""
                            ),  # 작성자 이름
                            "authorProfileImageUrl": reply_snippet.get(
                                "authorProfileImageUrl", ""
                            ),  # 프로필 사진 URL
                            "authorChannelUrl": reply_snippet.get(
                                "authorChannelUrl", ""
                            ),  # 채널 URL
                            "authorChannelId": reply_snippet.get(
                                "authorChannelId", {}
                            ),  # 채널 ID 객체
                            "likeCount": reply_snippet.get("likeCount", 0),  # 좋아요 수
                            "publishedAt": reply_snippet.get(
                                "publishedAt", ""
                            ),  # 게시 일시
                            "updatedAt": reply_snippet.get(
                                "updatedAt", ""
                            ),  # 업데이트 일시
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


def save_comments_to_csv(comments: List[YouTubeComment], video_id: str):
    """
    YouTubeComment 객체 리스트를 CSV 파일로 저장합니다.
    """
    if not comments:
        print("저장할 댓글이 없습니다.\n")
        return

    # CSV 파일 이름 정의
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"comments_{video_id}_{timestamp}.csv"
    filepath = get_csv_path(filename)

    # CSV 헤더 정의 (한글 및 영어 병기)
    headers = [
        "댓글 ID (comment_id)",  # comment_id
        "채널 ID (channel_id)",  # channel_id
        "동영상 ID (video_id)",  # video_id
        "댓글 내용 (text_display)",  # text_display
        "원본 댓글 (text_original)",  # text_original
        "작성자 이름 (author_display_name)",  # author_display_name
        "작성자 프로필 사진 URL (author_profile_image_url)",  # author_profile_image_url
        "작성자 채널 URL (author_channel_url)",  # author_channel_url
        "작성자 채널 ID (author_channel_id)",  # author_channel_id
        "좋아요 수 (like_count)",  # like_count
        "게시 일시 (published_at)",  # published_at
        "업데이트 일시 (updated_at)",  # updated_at
        "답글 여부 (reply)",  # reply
    ]

    try:
        with open(filepath, mode="w", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.writer(csvfile)
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
                        (
                            comment.author_channel_id.value
                            if comment.author_channel_id
                            else ""
                        ),
                        comment.like_count,
                        comment.published_at,
                        comment.updated_at,
                        "답글" if comment.reply else "댓글",
                    ]
                )
        print(f"댓글이 '{filepath}' 파일로 저장되었습니다.\n")
    except IOError as e:
        print(f"파일 입출력 오류 발생: {e}\n")


def print_video_info(video: YouTubeVideo):
    """
    YouTubeVideo 객체의 모든 정보를 한 번씩 출력합니다.
    """
    if not video:
        print("출력할 동영상 정보가 없습니다.\n")
        return

    print("\n동영상 정보:")
    print(f"동영상 ID: {video.id}")
    print(f"제목: {video.title}")
    print(f"설명: {video.description}")
    print(f"채널 ID: {video.channel_id}")
    print(f"채널 이름: {video.channel_title}")
    print(f"카테고리 ID: {video.category_id}")
    print(f"기본 썸네일 URL: {video.thumbnail_url}")
    print(f"기본 썸네일 너비: {video.thumbnail_width}")
    print(f"기본 썸네일 높이: {video.thumbnail_height}")
    print(f"라이브 방송 여부: {video.live_broadcast_content}")
    print(f"조회수: {video.view_count}")
    print(f"좋아요 수: {video.like_count}")
    print(f"댓글 수: {video.comment_count}\n")


def print_comment_sample(comments: List[YouTubeComment], sample_size: int = 5):
    """
    YouTubeComment 객체 리스트에서 샘플을 콘솔에 출력합니다.
    """
    if not comments:
        print("출력할 댓글이 없습니다.\n")
        return

    print(f"\n댓글 샘플 (최대 {sample_size}개):")
    for comment in comments[:sample_size]:
        reply_status = "답글" if comment.reply else "댓글"
        print(
            f"[{reply_status}] {comment.author_display_name}님이 작성: {comment.text_display}"
        )
        print(f"  - 댓글 ID: {comment.comment_id}")
        print(f"  - 프로필 사진: {comment.author_profile_image_url}")
        print(f"  - 평가 수: {comment.like_count}")
        print()


def main():
    # 인증 및 YouTube 클라이언트 생성
    credentials = authenticate()
    youtube_client = build_youtube_client(credentials)

    print("동영상 ID를 연속적으로 입력하세요. 종료하려면 '-1'을 입력하세요.\n")

    while True:
        # 입력: 동영상 ID
        video_id = input(
            "YouTube 동영상 ID를 입력하세요 (종료하려면 -1 입력): "
        ).strip()

        if video_id == "-1":
            print("프로그램을 종료합니다.")
            break

        if not video_id:
            print("동영상 ID가 입력되지 않았습니다. 다시 입력해주세요.\n")
            continue

        start_time = time.time()

        # 동영상 정보 가져오기
        video_info = get_video_info(youtube_client, video_id)
        if video_info:
            # 동영상 정보 CSV로 저장
            save_video_info_to_csv(video_info)
            # 동영상 정보 콘솔에 출력
            print_video_info(video_info)

            # 댓글 크롤링 여부 묻기
            while True:
                crawl_comments_choice = (
                    input("댓글을 크롤링하시겠습니까? (y/n): ").strip().lower()
                )
                if crawl_comments_choice not in ["y", "n"]:
                    print("유효하지 않은 입력입니다. 'y' 또는 'n'을 입력해주세요.\n")
                    continue
                break

            if crawl_comments_choice == "y":
                # 댓글 크롤링
                comments = crawl_comments(youtube_client, video_id)

                # 댓글을 CSV 파일로 저장
                save_comments_to_csv(comments, video_id)

                # 댓글 샘플 출력
                if comments:
                    print_comment_sample(comments, sample_size=5)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # 소요 시간 출력
        print(f"소요 시간: {elapsed_time:.2f}초\n")


if __name__ == "__main__":
    main()
