# Copy this file to scripts\set_env.ps1 and replace the placeholders.
# Keep scripts\set_env.ps1 local and out of Git.
# Prefer *_PROFILE_ID when available. *_PROFILE_NAME is only a fallback.

$env:DEMAND_RADAR_WECOM_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
$env:DEMAND_RADAR_FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/YOUR_HOOK"

$env:DEMAND_RADAR_EMAIL_HOST = "smtp.qq.com"
$env:DEMAND_RADAR_EMAIL_PORT = "465"
$env:DEMAND_RADAR_EMAIL_USERNAME = "your@example.com"
$env:DEMAND_RADAR_EMAIL_PASSWORD = "your-app-password"
$env:DEMAND_RADAR_EMAIL_FROM = "your@example.com"
$env:DEMAND_RADAR_EMAIL_TO = "your@example.com"

$env:DEMAND_RADAR_CLOAK_API_BASE_URL = "http://127.0.0.1:54381"
$env:DEMAND_RADAR_ZBJ_PROFILE_ID = ""
$env:DEMAND_RADAR_ZBJ_PROFILE_NAME = "ZBJ Browser"
$env:DEMAND_RADAR_BILIBILI_PROFILE_ID = ""
$env:DEMAND_RADAR_BILIBILI_PROFILE_NAME = "Bilibili Browser"
$env:DEMAND_RADAR_DOUYIN_PROFILE_ID = ""
$env:DEMAND_RADAR_DOUYIN_PROFILE_NAME = "Douyin Browser"
$env:DEMAND_RADAR_XIAOHONGSHU_PROFILE_ID = ""
$env:DEMAND_RADAR_XIAOHONGSHU_PROFILE_NAME = "Xiaohongshu Browser"

Write-Host "Demand Radar environment variables loaded for the current PowerShell session."
Write-Host "Copy this file to scripts\\set_env.ps1, replace placeholders, and keep that file out of Git."
