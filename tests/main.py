import json
import time
import warnings
import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

warnings.filterwarnings("ignore")

# OAuth2 인증 흐름 생성, 로컬 서버에서 실행. 추후 프론트에서 받아오는 방식으로 변경해야 함
scopes = [
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

client_secrets_path = "client_secret.json"

with open(client_secrets_path, "r") as file:
    secret_file = json.load(file)

oauth_flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    client_secrets_path, scopes
)
flow_result = oauth_flow.run_local_server()

oauth_access_token = flow_result.token
oauth_refresh_token = flow_result.refresh_token

####################################################################################################

client_id = secret_file["installed"]["client_id"]
client_secret = secret_file["installed"]["client_secret"]
token_uri = secret_file["installed"]["token_uri"]

credentials = google.oauth2.credentials.Credentials(
    token=oauth_access_token,
    refresh_token=oauth_refresh_token,
    token_uri=token_uri,
    client_id=client_id,
    client_secret=client_secret,
    scopes=scopes,
)

# OAuth2 객체 생성
oauth_client = googleapiclient.discovery.build("oauth2", "v2", credentials=credentials)

# 사용자 정보 가져오기
user_info = oauth_client.userinfo().v2().me().get().execute()

print(user_info)

# 결과 출력
print(f"Name: {user_info.get('name')}")
print(f"Email: {user_info.get('email')}")
print(f"Picture: {user_info.get('picture')}")

# 유튜브 본인 채널 정보 가져옴
youtube_client = googleapiclient.discovery.build(
    "youtube",
    "v3",
    credentials=credentials,
)

my_channel = (
    youtube_client.channels()
    .list(part="id,snippet,contentDetails,statistics", mine=True)
    .execute()
)

# 업로드된 동영상이 포함된 재생목록 ID 가져오기
uploads_playlist_id = my_channel["items"][0]["contentDetails"]["relatedPlaylists"][
    "uploads"
]

# 동영상 목록 가져오기
videos = []
next_page_token = None

while True:
    playlist_items_response = (
        youtube_client.playlistItems()
        .list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token,
        )
        .execute()
    )

    videos += playlist_items_response["items"]
    next_page_token = playlist_items_response.get("nextPageToken")

    if not next_page_token:
        break

# 동영상 정보 출력하기
for video in videos:
    video_title = video["snippet"]["title"]
    video_id = video["contentDetails"]["videoId"]
    published_at = video["contentDetails"]["videoPublishedAt"]
    print(f"Title: {video_title}, Video ID: {video_id}, Published At: {published_at}")


####################################################################################################
# 여기서부터 람다로 옮겨서 실행
def build_youtube_client(access_token, refresh_token):
    client_secrets_file = "client_secret.json"

    # SECRET JSON 파일 열기
    with open(client_secrets_file, "r") as file:
        secret_file = json.load(file)

    # 필요한 값 추출
    client_id = secret_file["installed"]["client_id"]
    client_secret = secret_file["installed"]["client_secret"]
    token_uri = secret_file["installed"]["token_uri"]

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


def crawl_comment(video_id, access_token, refresh_token):
    comments = []
    youtube_client = build_youtube_client(access_token, refresh_token)

    video_title = video["snippet"]["title"]
    print(f"동영상 '{video_title}'의 댓글을 가져오는 중...")

    # 첫 번째 페이지의 댓글 가져오기
    comment_response = (
        youtube_client.commentThreads()
        .list(part="id,snippet", videoId=video_id, maxResults=100)
        .execute()
    )

    while comment_response:
        # 각 댓글 스레드 처리
        for thread in comment_response["items"]:
            # 최상위 댓글 가져오기
            top_comment = thread["snippet"]["topLevelComment"]["snippet"]
            comments.append(
                {
                    "video_id": video_id,
                    "video_title": video_title,
                    "comment": top_comment["textOriginal"],
                    "author": top_comment["authorDisplayName"],
                    "published_at": top_comment["publishedAt"],
                    "like_count": top_comment["likeCount"],
                    "reply": False,  # 최상위 댓글 표시
                }
            )
            # total_comment_count += 1

            # 답글이 있을 경우 처리
            if thread["snippet"]["totalReplyCount"] > 0:
                # 답글 가져오기 (pagination 처리)
                reply_response = (
                    youtube_client.comments()
                    .list(part="snippet", parentId=thread["id"], maxResults=100)
                    .execute()
                )

                while reply_response:
                    for reply_item in reply_response["items"]:
                        reply = reply_item["snippet"]
                        comments.append(
                            {
                                "video_id": video_id,
                                "video_title": video_title,
                                "comment": reply["textOriginal"],
                                "author": reply["authorDisplayName"],
                                "published_at": reply["publishedAt"],
                                "like_count": reply["likeCount"],
                                "reply": True,  # 답글 표시
                            }
                        )
                        # total_reply_count += 1

                    # 다음 페이지의 답글이 있으면 가져오기
                    if "nextPageToken" in reply_response:
                        reply_response = (
                            youtube_client.comments()
                            .list(
                                part="snippet",
                                parentId=thread["id"],
                                maxResults=100,
                                pageToken=reply_response["nextPageToken"],
                            )
                            .execute()
                        )
                    else:
                        break

        # 다음 페이지의 댓글이 있으면 가져오기
        if "nextPageToken" in comment_response:
            comment_response = (
                youtube_client.commentThreads()
                .list(
                    part="id,snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=comment_response["nextPageToken"],
                )
                .execute()
            )
        else:
            break
    return comments


# 동영상 별로 댓글 가져오기
for video in videos:
    print("-" * 50)
    start_time = time.time()
    video_id = video["contentDetails"]["videoId"]
    comments = crawl_comment(video_id, oauth_access_token, oauth_refresh_token)
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"총 소요 시간: {elapsed_time:.2f}초")
    print(f"총 댓글 수: ", len(comments))

    print("\n댓글 예시 (5개):")
    for comment in comments[:5]:
        reply_status = "답글" if comment["reply"] else "댓글"
        print(f"[{reply_status}] {comment['author']}님이 작성: {comment['comment']}")


## 추가작업
# 1. 람다로 옮겨서 DB에 저장
