import re
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import streamlit as st
import google.generativeai as genai

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


def analyze_titles_gemini(titles: List[str], api_key: str, model_name: str) -> pd.DataFrame:
    """Return a DataFrame with columns: title, analysis (2-3 sentences)."""
    if not api_key:
        return pd.DataFrame(columns=["title", "analysis"])
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name or "gemini-1.5-flash")
    except Exception as e:
        st.error(f"Failed to initialize Gemini: {e}")
        return pd.DataFrame(columns=["title", "analysis"])

    rows = []
    for t in titles:
        prompt = (
            "Analyze this YouTube video title and write a concise 2-3 sentence summary focusing on: "
            "(1) the primary emotions it targets, and (2) any pattern or hook used. "
            "Be direct and on-point. Title: '" + t + "'"
        )
        try:
            resp = model.generate_content(prompt)
            text = (resp.text or "").strip()
        except Exception as e:
            text = f"Analysis unavailable: {e}"
        rows.append({"title": t, "analysis": text})
    return pd.DataFrame(rows, columns=["title", "analysis"]) 


def list_gemini_models(api_key: str) -> List[str]:
    """Return available Gemini model names supporting text generation for this key.
    Falls back to common options if listing fails."""
    default = [
        "gemini-1.5-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash-8b",
    ]
    if not api_key:
        return default
    try:
        genai.configure(api_key=api_key)
        names: List[str] = []
        for m in genai.list_models():
            # Different SDK versions expose different capabilities fields
            methods = getattr(m, "supported_generation_methods", None)
            if methods and ("generateContent" in methods or "generate_text" in methods):
                names.append(m.name)
        # Keep stable ordering: prefer common models first, then others
        prioritized = [n for n in default if n in names]
        others = [n for n in names if n not in prioritized]
        return prioritized + others
    except Exception:
        return default


c1, c2 = st.columns([3, 2])
with c1:
    url_input = st.text_input("Channel link or @handle", placeholder="https://www.youtube.com/@example or @example")
with c2:
    gemini_key = st.text_input("Gemini API Key (optional)", type="password", placeholder="AIza... or from HF secrets", help="Provide your own Google Gemini API key to add title analysis sheet.")

# Model selection for Gemini analysis (populated when key is provided)
if gemini_key:
    available_models = list_gemini_models(gemini_key)
else:
    available_models = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-1.5-flash-8b"]

model_name = st.selectbox(
    "Gemini model",
    options=available_models,
    index=0 if available_models else None,
    help="Choose the model for title analysis. Flash is fastest; Pro is higher quality but slower.",
)

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

            analysis_df = pd.DataFrame()
            if gemini_key:
                with st.spinner("Analyzing titles with Gemini..."):
                    analysis_df = analyze_titles_gemini(df["title"].astype(str).tolist(), gemini_key, model_name)
            else:
                st.info("No Gemini key provided. Skipping title analysis sheet.")

            # Write Excel to memory with two sheets if analysis present
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Videos")
                if not analysis_df.empty:
                    analysis_df.to_excel(writer, index=False, sheet_name="Title Analysis")
            output.seek(0)

            st.download_button(
                label="Download Excel",
                data=output.read(),
                file_name=f"{clean_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.caption("Powered by yt-dlp + pandas + Streamlit")

