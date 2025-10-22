import yt_dlp
import re

def safe_filename(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_-]+', '_', name).strip('_')

def ytdlp_extract_channel_video_ids(channel_url: str):
    ydl_opts = {'quiet': True, 'extract_flat': True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
    return [f"https://www.youtube.com/watch?v={e['id']}" for e in info.get('entries', [])]

def ytdlp_extract_video_details(video_url: str):
    ydl_opts = {'quiet': True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return {
        "title": info.get("title", ""),
        "views": info.get("view_count", 0),
        "date": info.get("upload_date", "")[:10],
        "link": f"https://www.youtube.com/watch?v={info.get('id', '')}",
    }

def ytdlp_extract_channel_title(channel_url: str):
    with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
        info = ydl.extract_info(channel_url, download=False)
    return info.get("title", "Unknown Channel")
