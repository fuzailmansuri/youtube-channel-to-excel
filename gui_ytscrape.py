import threading
import os
import traceback
from tkinter import Tk, Label, Entry, Button, StringVar, messagebox

from scrape_youtube_channel import (
    ytdlp_extract_channel_title,
    scrape_channel_to_excel,
    safe_filename,
)


def normalize_channel_url(user_input: str) -> str:
    u = user_input.strip()
    if not u:
        return u
    if u.startswith("@"):
        return f"https://www.youtube.com/{u}/videos"
    if u.startswith("https://www.youtube.com/@") and "/videos" not in u:
        return u.rstrip("/") + "/videos"
    return u


def base_channel_url(user_input: str) -> str:
    b = user_input.strip()
    if b.startswith("@"):
        b = f"https://www.youtube.com/{b}"
    return b.rstrip("/")


class App:
    def __init__(self, root: Tk):
        self.root = root
        root.title("YouTube → Excel (yt-dlp)")
        root.geometry("520x160")
        root.resizable(False, False)

        self.url_var = StringVar()
        self.status_var = StringVar(value="Enter channel link (e.g., https://www.youtube.com/@handle)")

        Label(root, text="Channel link:").place(x=16, y=18)
        self.url_entry = Entry(root, textvariable=self.url_var, width=62)
        self.url_entry.place(x=16, y=40)
        self.url_entry.focus_set()

        self.run_btn = Button(root, text="Run and Save", width=16, command=self.on_run)
        self.run_btn.place(x=16, y=78)

        Label(root, textvariable=self.status_var, anchor="w", fg="#444").place(x=16, y=120)

    def on_run(self):
        url_input = self.url_var.get().strip()
        if not url_input:
            messagebox.showwarning("Missing", "Please enter a YouTube channel link or @handle")
            return

        self.run_btn.config(state="disabled")
        self.status_var.set("Starting…")

        t = threading.Thread(target=self._run_task, args=(url_input,), daemon=True)
        t.start()

    def _run_task(self, url_input: str):
        try:
            url = normalize_channel_url(url_input)
            base = base_channel_url(url_input)
            title = ytdlp_extract_channel_title(base)
            filename = f"{safe_filename(title)}.xlsx"
            out_path = os.path.abspath(filename)

            self._set_status("Collecting videos… This may take a few minutes on large channels…")
            scrape_channel_to_excel(url, out_path)

            self._set_status(f"Done: {out_path}")
            messagebox.showinfo("Saved", f"Saved to:\n{out_path}")
        except Exception:
            err = traceback.format_exc()
            self._set_status("Error. See details.")
            messagebox.showerror("Error", err)
        finally:
            self.run_btn.config(state="normal")

    def _set_status(self, text: str):
        # Tk operations must run on main thread
        self.root.after(0, self.status_var.set, text)


def main():
    root = Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
