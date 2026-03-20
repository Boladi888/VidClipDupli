# VidClipDupli

An audio-based duplicate and clip finder designed specifically for large NAS media libraries. 

Standard deduplication tools look at file hashes, which means they fail if a video is a different resolution, a different format (`.mkv` vs `.mp4`), or slightly compressed. **This tool listens to the audio track instead.** By extracting a Chromaprint audio fingerprint from every file, it can find exact duplicates across different formats, AND it can find short video clips that were extracted from longer parent videos.

**⚠️ Safe by Design: This script NEVER deletes files automatically.** It generates an interactive HTML report and customized deletion scripts for you to review first.

## 🚀 How It Works

It detects two specific kinds of relationships:
1. **Exact Duplicates:** Two files with nearly identical audio across their full length (e.g., a 1080p and a 4K version of the same movie). 
2. **Clips:** A shorter file whose audio appears perfectly inside a longer file (e.g., a 2-minute trailer sitting next to the full 2-hour movie, or a downloaded clip from a compilation of family videos).

Results are cached in a local SQLite database (`.audio_cache_v1a.db`). The first run takes time to extract the fingerprints, but subsequent runs are near-instant, even if you change the mathematical thresholds.

## ✨ Key Features (v27)
* **NAS-Optimized:** Threaded CPU-only extraction designed not to thrash mechanical hard drives over Gigabit ethernet.
* **Interactive HTML Report:** Visually review matches in your browser, choose which versions to keep/rename, and export a custom deletion script.
* **Unicode/ANSI Invincible:** Uses Windows 8.3 DOS short paths to safely pass Chinese, Japanese, emoji, and heavily bracketed filenames to legacy C-binaries.
* **Massive Scale:** Capable of processing 4M+ pair comparisons in minutes using Multi-Core Shared Memory (Zero-RAM duplication).
* **Graceful Shutdown:** Hit `Ctrl+C` at any time; progress is safely saved to the database.

## 🛠️ Requirements

**1. Python 3.8+** with the following packages:
```bash
pip install numpy tqdm