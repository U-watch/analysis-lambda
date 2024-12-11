import json
import google.oauth2.credentials
import google_auth_oauthlib.flow

# 필요한 OAuth 2.0 범위 정의
scopes = [
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

# client_secret.json 파일 경로
client_secrets_path = "client_secret.json"

# client_secret.json 파일 로드
with open(client_secrets_path, "r") as file:
    secret_file = json.load(file)

# OAuth 2.0 인증 흐름 생성
oauth_flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    client_secrets_path, scopes
)

# 로컬 서버를 통해 사용자 인증 실행
flow_result = oauth_flow.run_local_server(port=0, prompt="consent")

# Access Token과 Refresh Token 추출
access_token = flow_result.token
refresh_token = flow_result.refresh_token

# 토큰 출력
print("Access Token:", access_token)
print("Refresh Token:", refresh_token)
