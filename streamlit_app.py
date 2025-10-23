"""
YouTube Channel Scraper - Production Ready Version

This application scrapes YouTube channel video data and exports it to Excel.
It follows Qoder engineering rules for clean, maintainable, and production-ready code.
"""

import os
import re
import tempfile
import atexit
import pathlib
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

import pandas as pd
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set environment variable to use polling instead of inotify
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "poll"

# Try to import google.generativeai, but handle if it's not available
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logger.warning("Google Generative AI not available. AI analysis will be disabled.")

# Try to import Google API client for YouTube Data API
try:
    from googleapiclient.discovery import build
    YOUTUBE_API_AVAILABLE = True
except ImportError:
    YOUTUBE_API_AVAILABLE = False
    logger.warning("YouTube Data API client not installed.")

# Import local modules
from utils import normalize_channel_url, base_channel_url

try:
    from scrape_youtube_channel import (
        ytdlp_extract_channel_video_ids,
        ytdlp_extract_video_details,
        ytdlp_extract_channel_title,
        safe_filename,
        scrape_channel_to_excel_realtime,
    )
except ImportError as e:
    st.error(f"Missing required dependencies: {e}")
    st.info("Please ensure all requirements are installed by running: pip install -r requirements.txt")
    st.stop()

# Global variables for cleanup
temp_files = []


def cleanup_temp_files():
    """Clean up temporary files on exit to prevent resource leaks."""
    for temp_file in temp_files:
        try:
            pathlib.Path(temp_file).unlink(missing_ok=True)
            logger.info(f"Cleaned up temporary file: {temp_file}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {temp_file}: {e}")


# Register cleanup function
atexit.register(cleanup_temp_files)


def list_gemini_models(api_key: str) -> List[str]:
    """
    Lists available Gemini models.
    
    Args:
        api_key: Google Gemini API key
        
    Returns:
        List of available model names
    """
    if not GEMINI_AVAILABLE:
        return ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-1.5-flash-8b"]
    
    try:
        genai.configure(api_key=api_key)
        models = [m.name for m in genai.list_models() if "generateContent" in m.supported_generation_methods]
        return [model.replace("models/", "") for model in models]
    except Exception as e:
        logger.error(f"Error listing Gemini models: {e}")
        st.error(f"Error listing Gemini models: {e}")
        return ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-1.5-flash-8b"]


def validate_youtube_url(url: str) -> bool:
    """
    Validate if the provided URL is a valid YouTube channel URL.
    
    Args:
        url: YouTube channel URL or handle
        
    Returns:
        True if valid, False otherwise
    """
    if not url:
        return False
    
    # Regex pattern for YouTube URLs and handles
    youtube_pattern = re.compile(
        r'^(https?://)?(www\.)?(youtube\.com/@|youtube\.com/channel/|youtube\.com/user/|@)[\w\-]+(/videos)?/?$'
    )
    
    return bool(youtube_pattern.match(url.strip()))


def create_temp_file(suffix: str = ".xlsx") -> str:
    """
    Create a temporary file and track it for cleanup.
    
    Args:
        suffix: File suffix/extension
        
    Returns:
        Path to the temporary file
    """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp_file.close()
    temp_files.append(temp_file.name)
    return temp_file.name


def format_time(seconds: float) -> str:
    """
    Format seconds into HH:MM:SS format.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string
    """
    return time.strftime("%H:%M:%S", time.gmtime(seconds))


def main_app():
    """Main application function."""
    # Configure Streamlit page
    st.set_page_config(
        page_title="YouTube → Excel (yt-dlp)", 
        page_icon="📊", 
        layout="centered"
    )
    
    st.title("YouTube Channel → Excel")
    st.write(
        "Paste a YouTube channel link or @handle. "
        "The app will extract video Title, Views, Date, Link, and Analysis "
        "and offer an Excel download."
    )

    # Create tabs for different sections
    tab1, tab2 = st.tabs(["Scraper", "API Configuration"])

    with tab1:
        render_scraper_tab()
        
    with tab2:
        render_api_configuration_tab()
        
    st.caption("Powered by yt-dlp + openpyxl + Streamlit")


def render_scraper_tab():
    """Render the scraper tab UI."""
    # Input section
    c1, c2 = st.columns([3, 2])
    with c1:
        url_input = st.text_input(
            "Channel link or @handle", 
            placeholder="https://www.youtube.com/@example or @example"
        )
    with c2:
        default_key = os.getenv("GEMINI_API_KEY", "")
        gemini_key = st.text_input(
            "Gemini API Key (optional)", 
            type="password", 
            placeholder="AIza... or from HF secrets", 
            help="Provide your own Google Gemini API key to add title analysis sheet.", 
            value=default_key
        )

    # Add pagination controls
    st.subheader("Scraping Options")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        max_videos = st.number_input(
            "Maximum videos to extract (0 for unlimited)", 
            min_value=0, 
            max_value=50000, 
            value=1000,
            help="Limit for large channels to prevent timeouts. Set to 0 for no limit (not recommended for large channels)."
        )
    with col2:
        enable_pagination = st.checkbox(
            "Enable Pagination", 
            value=False, 
            help="Break large channels into smaller chunks for processing"
        )
    with col3:
        videos_per_page = st.number_input(
            "Videos per page", 
            min_value=10, 
            max_value=5000, 
            value=100,
            help="Number of videos to process in each chunk",
            disabled=not enable_pagination
        )
    
    # Initialize pagination variables
    start_page = 1
    end_page = 1
    
    # Show pagination controls only when enabled
    if enable_pagination:
        st.info(
            "When pagination is enabled, the scraper will process videos in chunks. "
            "You can specify which chunks to process below."
        )
        col1, col2 = st.columns(2)
        with col1:
            start_page = st.number_input(
                "Start Page", 
                min_value=1, 
                value=1, 
                help="First page to process (1 = first set of videos)"
            )
        with col2:
            end_page = st.number_input(
                "End Page", 
                min_value=start_page, 
                value=start_page, 
                help="Last page to process"
            )
    
    # Add advanced options
    with st.expander("Advanced Options"):
        col1, col2, col3 = st.columns(3)
        with col1:
            max_workers = st.slider(
                "Concurrent Workers", 
                min_value=1, 
                max_value=30, 
                value=15, 
                help="Number of concurrent requests (higher = faster but more resource intensive)"
            )
        with col2:
            retry_attempts = st.slider(
                "Retry Attempts", 
                min_value=1, 
                max_value=20, 
                value=7,
                help="Number of times to retry failed requests"
            )
        with col3:
            timeout_seconds = st.slider(
                "Request Timeout (seconds)", 
                min_value=30, 
                max_value=300, 
                value=90,
                help="Timeout for each request"
            )
        
        use_api_fallback = st.checkbox(
            "Use YouTube API with yt-dlp fallback", 
            value=False,
            help="Try YouTube Data API first, fallback to yt-dlp if API fails"
        )
    
    # Model selection
    if gemini_key and GEMINI_AVAILABLE:
        available_models = list_gemini_models(gemini_key)
    else:
        available_models = ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-1.5-flash-8b"]

    model_name = st.selectbox(
        "Gemini model",
        options=available_models,
        index=0 if available_models else None,
        help="Choose the model for title analysis. Flash is fastest; Pro is higher quality but slower.",
        disabled=not GEMINI_AVAILABLE
    )

    run = st.button("Run and Prepare Excel", type="primary")

    if run:
        handle_scraping_process(
            url_input, 
            gemini_key, 
            model_name,
            max_videos,
            enable_pagination,
            videos_per_page,
            start_page,
            end_page,
            max_workers,
            retry_attempts,
            timeout_seconds,
            use_api_fallback
        )


def handle_scraping_process(
    url_input: str,
    gemini_key: str,
    model_name: str,
    max_videos: int,
    enable_pagination: bool,
    videos_per_page: int,
    start_page: int,
    end_page: int,
    max_workers: int,
    retry_attempts: int,
    timeout_seconds: int,
    use_api_fallback: bool
):
    """
    Handle the scraping process.
    
    Args:
        url_input: YouTube channel URL or handle
        gemini_key: Gemini API key
        model_name: Selected Gemini model
        max_videos: Maximum number of videos to extract
        enable_pagination: Whether pagination is enabled
        videos_per_page: Number of videos per page
        start_page: Start page number
        end_page: End page number
        max_workers: Number of concurrent workers
        retry_attempts: Number of retry attempts
        timeout_seconds: Request timeout in seconds
        use_api_fallback: Whether to use YouTube API fallback
    """
    # Validate input
    if not url_input.strip():
        st.error("Please enter a channel link or @handle")
        return
    
    if not validate_youtube_url(url_input):
        st.error("Please enter a valid YouTube channel URL or @handle")
        return
    
    # Process URLs
    videos_url = normalize_channel_url(url_input)
    base_url = base_channel_url(url_input)
    
    # Initialize variables that might be used later
    channel_title = "Unknown Channel"
    clean_name = "channel"
    temp_excel_path = None
    
    # Show configuration summary
    with st.expander("Scraping Configuration", expanded=False):
        st.write(f"**Channel URL:** {videos_url}")
        st.write(f"**Maximum Videos:** {max_videos if max_videos > 0 else 'Unlimited'}")
        st.write(f"**Pagination:** {'Enabled' if enable_pagination else 'Disabled'}")
        if enable_pagination:
            st.write(f"**Videos per Page:** {videos_per_page}")
            st.write(f"**Pages:** {start_page} to {end_page}")
        st.write(f"**Concurrent Workers:** {max_workers}")
        st.write(f"**Retry Attempts:** {retry_attempts}")
        st.write(f"**Timeout:** {timeout_seconds} seconds")
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    start_time = time.time()
    
    def update_progress(current: int, total: int):
        """Update progress bar and status text."""
        progress_percent = int((current / total) * 100) if total > 0 else 0
        progress_bar.progress(progress_percent)
        
        if total > 1000:
            elapsed_time = time.time() - start_time
            if current > 0:
                eta_seconds = (elapsed_time / current) * (total - current)
                eta_formatted = format_time(eta_seconds)
                status_text.text(f"Processing video {current} of {total} ({progress_percent}%) - ETA: {eta_formatted}")
            else:
                status_text.text(f"Processing video {current} of {total} ({progress_percent}%)")
        else:
            status_text.text(f"Processing video {current} of {total} ({progress_percent}%)")

    # Main scraping process
    with st.spinner("Collecting videos (may take a few minutes for large channels)..."):
        try:
            channel_title = ytdlp_extract_channel_title(base_url)
            clean_name = safe_filename(channel_title)
            
            # Create a temporary file for real-time Excel writing
            temp_excel_path = create_temp_file(".xlsx")
            
            # Determine max videos based on pagination settings
            actual_max_videos = max_videos if max_videos > 0 else None
            
            # If pagination is enabled, calculate the range
            if enable_pagination:
                start_video = (start_page - 1) * videos_per_page
                end_video = end_page * videos_per_page
                if actual_max_videos:
                    end_video = min(end_video, actual_max_videos)
                actual_max_videos = end_video
                
                st.info(f"Processing videos {start_video+1} to {end_video}")

            scrape_channel_to_excel_realtime(
                videos_url,
                temp_excel_path,
                gemini_key=gemini_key if GEMINI_AVAILABLE else "",
                model_name=model_name if GEMINI_AVAILABLE else "",
                max_workers=max_workers,
                progress_callback=update_progress,
                max_videos=actual_max_videos
            )

        except Exception as e:
            logger.error(f"Error collecting videos: {str(e)}", exc_info=True)
            st.error(f"Error collecting videos: {str(e)}")
            st.info("Try using the explicit /videos URL, e.g. https://www.youtube.com/@handle/videos")
            
            # Try alternative approach
            try:
                alt_url = videos_url + "/videos" if "/videos" not in videos_url else videos_url
                st.info(f"Trying alternative approach with URL: {alt_url}")
                
                temp_excel_path = create_temp_file(".xlsx")

                scrape_channel_to_excel_realtime(
                    alt_url,
                    temp_excel_path,
                    gemini_key=gemini_key if GEMINI_AVAILABLE else "",
                    model_name=model_name if GEMINI_AVAILABLE else "",
                    max_workers=max_workers,
                    progress_callback=update_progress,
                    max_videos=actual_max_videos
                )

                channel_title = ytdlp_extract_channel_title(base_url)
                clean_name = safe_filename(channel_title)
            except Exception as e2:
                logger.error(f"Alternative approach also failed: {str(e2)}", exc_info=True)
                st.error(f"Alternative approach also failed: {str(e2)}")
                st.stop()
            finally:
                progress_bar.empty()
                status_text.empty()
        else:
            progress_bar.empty()
            status_text.empty()

    # Display results
    if not temp_excel_path or not os.path.exists(temp_excel_path) or os.path.getsize(temp_excel_path) == 0:
        st.error("No data extracted. Try the explicit /videos URL, e.g. https://www.youtube.com/@handle/videos")
        st.info("Note: Some channels may have restrictions that prevent data extraction.")
    else:
        # Read the generated Excel file to display data and statistics
        try:
            df_display = pd.read_excel(temp_excel_path)
        except Exception as e:
            logger.error(f"Error reading generated Excel file: {e}", exc_info=True)
            st.error(f"Error reading generated Excel file: {e}")
            st.stop()

        st.success(f"Found {len(df_display)} videos for '{channel_title}'.")
        
        st.subheader("Preview of Extracted Data")
        st.dataframe(df_display.head(20), use_container_width=True)

        # Provide download button
        try:
            with open(temp_excel_path, "rb") as file:
                st.download_button(
                    label="Download Excel File",
                    data=file,
                    file_name=f"{clean_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        except Exception as e:
            logger.error(f"Error preparing download: {e}", exc_info=True)
            st.error(f"Error preparing download: {e}")


def render_api_configuration_tab():
    """Render the API configuration tab UI."""
    st.header("API Configuration")
    
    st.subheader("Gemini API Key")
    st.write("Configure your Google Gemini API key for AI-powered video title analysis.")
    
    gemini_api_key = st.text_input(
        "Gemini API Key", 
        value=os.getenv("GEMINI_API_KEY", ""),
        type="password",
        help="Your Google Gemini API key for title analysis",
        key="gemini_config"
    )
    
    if st.button("Save Gemini API Key", key="save_gemini"):
        if gemini_api_key:
            os.environ["GEMINI_API_KEY"] = gemini_api_key
            st.success("Gemini API key saved to environment variables!")
        else:
            st.warning("Please enter a valid API key")
    
    st.divider()
    
    st.subheader("YouTube Data API Key")
    st.write("Configure your YouTube Data API key for enhanced reliability and faster scraping.")
    
    if not YOUTUBE_API_AVAILABLE:
        st.warning("YouTube Data API client not installed. Install with: `pip install google-api-python-client`")
    
    youtube_api_key = st.text_input(
        "YouTube Data API Key", 
        value=os.getenv("YOUTUBE_API_KEY", ""),
        type="password",
        help="Your YouTube Data API key for enhanced reliability",
        key="youtube_config"
    )
    
    if st.button("Save YouTube API Key", key="save_youtube"):
        if youtube_api_key:
            os.environ["YOUTUBE_API_KEY"] = youtube_api_key
            st.success("YouTube API key saved to environment variables!")
            st.info("Note: YouTube Data API support needs to be implemented in the scraping functions to take effect.")
        else:
            st.warning("Please enter a valid API key")
    
    st.divider()
    
    st.subheader("API Key Management")
    st.write("API keys are stored in environment variables for security.")
    
    if st.button("Clear All API Keys"):
        keys_cleared = []
        if "GEMINI_API_KEY" in os.environ:
            del os.environ["GEMINI_API_KEY"]
            keys_cleared.append("Gemini")
        if "YOUTUBE_API_KEY" in os.environ:
            del os.environ["YOUTUBE_API_KEY"]
            keys_cleared.append("YouTube")
        
        if keys_cleared:
            st.success(f"Cleared API keys: {', '.join(keys_cleared)}")
        else:
            st.info("No API keys to clear")


if __name__ == "__main__":
    main_app()
