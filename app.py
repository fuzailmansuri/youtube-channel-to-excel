import re
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import streamlit as st

from scrape_youtube_channel import (
    ytdlp_extract_channel_video_ids,
    ytdlp_extract_video_details,
    ytdlp_extract_channel_title,
    safe_filename,
)


st.set_page_config(page_title="YouTube → Excel (yt-dlp)", page_icon="📊", layout="centered")
st.title("YouTube Channel → Excel")
st.write("Paste a YouTube channel link or @handle. The app will extract video Title, Views, Date, and Link and offer an Excel download.")


def normalize_channel_url(u: str) -> str:
    u = u.strip()
    if u.startswith("@"):
        return f"https://www.youtube.com/{u}/videos"
    if u.startswith("https://www.youtube.com/@") and "/videos" not in u:
        return u.rstrip("/") + "/videos"
    return u


def base_channel_url(u: str) -> str:
    b = u.strip()
    if b.startswith("@"):
        b = f"https://www.youtube.com/{b}"
    return b.rstrip("/")


def build_dataframe(channel_videos_url: str) -> pd.DataFrame:
    video_urls = ytdlp_extract_channel_video_ids(channel_videos_url)
    rows: List[Dict[str, Any]] = []
    total = len(video_urls)
    prog = st.progress(0, text=f"Fetching details... 0/{total}")
    for idx, url in enumerate(video_urls, start=1):
        try:
            details = ytdlp_extract_video_details(url)
            rows.append(details)
        except Exception as e:
            st.warning(f"Failed to fetch: {url} ({e})")
        if total:
            prog.progress(min(idx/total, 1.0), text=f"Fetching details... {idx}/{total}")
    if not rows:
        return pd.DataFrame(columns=["title", "views", "date", "link"])

    df = pd.DataFrame(rows, columns=["title", "views", "date", "link"]).copy()

    def date_key(x):
        try:
            return datetime.strptime(x, "%Y-%m-%d") if isinstance(x, str) else datetime.min
        except Exception:
            return datetime.min

    df = df.sort_values(by="date", key=lambda s: s.apply(date_key), ascending=False)
    return df


url_input = st.text_input("Channel link or @handle", placeholder="https://www.youtube.com/@example or @example")
run = st.button("Run and Prepare Excel")

if run:
    if not url_input.strip():
        st.error("Please enter a channel link or @handle")
    else:
        videos_url = normalize_channel_url(url_input)
        base_url = base_channel_url(url_input)
        with st.spinner("Collecting videos (may take a few minutes for large channels)..."):
            channel_title = ytdlp_extract_channel_title(base_url)
            clean_name = safe_filename(channel_title)
            df = build_dataframe(videos_url)

        if df.empty:
            st.error("No data extracted. Try the explicit /videos URL, e.g. https://www.youtube.com/@handle/videos")
        else:
            st.success(f"Found {len(df)} videos for '{channel_title}'.")
            st.dataframe(df.head(20), use_container_width=True)

            # Write Excel to memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            output.seek(0)

            st.download_button(
                label="Download Excel",
                data=output.read(),
                file_name=f"{clean_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.caption("Powered by yt-dlp + pandas + Streamlit")
