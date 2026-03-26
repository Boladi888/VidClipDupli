# VidClipDuplis

An audio-based duplicate and clip finder for large NAS media libraries.

Standard deduplication tools compare file hashes — they fail the moment a video is re-encoded, repackaged as a different container, or slightly trimmed. **VidClipDuplis listens to the audio track instead.** It extracts a [Chromaprint](https://acoustid.org/chromaprint) audio fingerprint from every file and compares them all, finding matches regardless of resolution, codec, bitrate, or container format.

It finds two things:
1. **Exact Duplicates** — The same content in different formats (e.g., a 1080p `.mkv` and a 4K `.mp4` of the same movie).
2. **Clips** — A shorter file whose audio lives inside a longer file (e.g., a trailer sitting next to the full movie, or a scene rip from a longer compilation).

> **⚠️ Safe by design.** This script **never deletes files**. It generates an interactive HTML report where you review every match and choose what to keep, delete, rename, or move — then exports a custom PowerShell script with your exact choices.

---

## Quick Start

```bash
pip install numpy tqdm
```

Download [`fpcalc.exe`](https://acoustid.org/chromaprint) and place it next to the script. Then just run it:

```
python VidClipDupli.py
```

No arguments needed — the interactive setup walks you through everything: folder selection, worker counts, sensitivity, cache management, and CLI tips. Or go fully headless:

```
python VidClipDupli.py "Z:\Movies" "Z:\TV Shows" "Z:\Clips" --no-prompt
```

### Optional: ffmpeg for better container support

Place `ffmpeg.exe` next to the script (or ensure it's on your PATH). When fpcalc fails on any file, VCD will automatically extract audio with ffmpeg and retry. Download from [ffmpeg.org](https://ffmpeg.org/download.html).

---

## How It Works

```
Phase 0: Scan        Find all media files in target directories
Phase 0.5: Hash      Compute content keys (quick_hash) for cache lookup
Phase 1: Extract     fpcalc decodes the full audio stream → uint32 fingerprint array
                     (ffmpeg fallback for any fpcalc failure if available)
Phase 2: Pre-filter  Skip pairs that can never pass the intro filter, then cache lookup
Phase 3: Compare     Sliding-window XOR + popcount across remaining pairs (multi-core)
Phase 4: Group       Union-Find for duplicates, directed edges for clips
Output:              HTML report + .bat + .ps1 + .json
```

**Fingerprints** are arrays of 32-bit integers (~6 per second of audio). Comparing two files means sliding the shorter array across the longer one, XOR-ing chunks, and counting differing bits. A low hamming distance = matching audio.

**Content-Based Caching** stores fingerprints and comparisons keyed by a hash of the file's content (first + middle + last 64KB + size), not by path. This means you can rename or move files and the cache still applies.

**Byte-for-byte detection** — Files with identical content keys are grouped as exact duplicates instantly, without needing fingerprint comparison. This catches copies across different folders in milliseconds.

**Threshold Re-evaluation** stores raw comparison metrics (match_ratio, length_ratio, matched_seconds) — not boolean results. You can change `--clip-ratio` or `--dup-ratio` between runs and cached metrics are re-evaluated against the new thresholds instantly, without re-extracting or re-comparing.

**Pre-filtering** — Before any comparisons run, pairs where the shorter file's audio is too short to ever pass the intro filter are skipped entirely. This can eliminate millions of unnecessary comparisons in libraries with many short clips alongside long movies.

**Graceful interruption** — Press Ctrl+C during any phase to stop gracefully. VCD saves all progress to cache and generates a partial HTML report with whatever matches were found so far. Run again to continue where you left off.

---

## Interactive HTML Report

The star feature. Open `review_results.html` in any browser:

- **All groups default to Skip** — nothing happens unless you explicitly decide. This prevents accidental deletions.
- **Collapsible cards** — each group shows a one-line summary (group number, file count, recommended keep filename). Click to expand and see all files + action buttons. Use "Expand All" / "Collapse All" buttons for quick navigation.
- **Summary line updates live** — when you make a choice, the collapsed header shows your decision: "Delete 2 · Keep: movie.mkv → T:\Movies\Action"
- **★ Recommended keep** badge on each group header shows which file the script recommends keeping (largest file by size)
- **Clickable filenames** open the file directly; **clickable folder paths** open the containing directory
- **Open** button for quick preview of any file
- **Keep** / **Delete** / **Use Name** buttons on every file
- "Use Name" shows an **editable text field** — type any custom filename
- **Folder selector** appears when files are in different directories — choose where the kept file goes (with or without a rename)
- **Skip** button to leave a group untouched
- Live counters for actions, deletions, renames/moves, and skips
- Redundant clips shown as parent/child pairs with the same interactive controls (including folder selection)
- **Previously skipped groups** are hidden by default. Toggle "Show previously skipped (N)" to reveal them. These appear in a separate section with muted styling.
- **Download Custom PowerShell Script** button shows action count. Generates `.ps1` with only your chosen actions.
- **Save Dismissed Groups** button exports `vcd_dismissed.json` — save it next to VidClipDupli.py and skipped groups will be hidden on the next run.
- **Partial results banner** if the scan was interrupted — shows what was found so far

The generated `.ps1` contains only the actions you chose — `Remove-Item` for deletions, `Rename-Item` for same-directory renames, `Move-Item` for cross-directory moves. No surprises.

> **Note:** The static `delete_duplicates.ps1` in the results folder uses default recommendations. For your custom choices, download the script from the HTML report, save it into the results folder, and use `RUN_CUSTOM_ACTIONS.bat` to launch it.

---

## What It Outputs

```
📂 results_Movies_TV_20260320_143022/
├── review_results.html       ← Interactive — open in browser
├── duplicate_report.json      Full structured data
├── delete_duplicates.bat      cmd.exe script (basic)
├── delete_duplicates.ps1      PowerShell script (handles long paths + Unicode)
├── RUN_CUSTOM_ACTIONS.bat      Launches your custom .ps1 with ExecutionPolicy bypass
└── fpcalc_debug.log           Anonymized failure log (if errors occurred)
```

**Which script should I use?**
- **review_results.html** — Recommended. Review interactively, then download a custom PowerShell script with your exact choices.
- **RUN_CUSTOM_ACTIONS.bat** — After downloading `custom_actions.ps1` from the HTML report, save it into the results folder and double-click this to run it. Handles ExecutionPolicy automatically.
- **delete_duplicates.ps1** — The default deletion script. Use directly if you've already enabled PowerShell scripts.
- **delete_duplicates.bat** — Fallback for cmd.exe. Has 260-character path limit on older Windows.

---

## Command-Line Options

| Option | Default | Description |
|---|---|---|
| `directories` | interactive | Paths to scan. Comma/semicolon separated, or one at a time. |
| `-w`, `--workers` | `6` | fpcalc extraction workers. Keep low for NAS (4-6 for Gigabit, 8-10 for 2.5GbE). |
| `-c`, `--compare-workers` | 75% CPUs | Comparison workers. These pin your CPU hard. |
| `--clip-ratio` | `0.75` | Min match ratio for clips (0.0–1.0). Lower = more lenient. |
| `--dup-ratio` | `0.95` | Min match ratio for exact duplicates (0.0–1.0). |
| `--intro-filter` | `30` | Ignore matches shorter than N seconds of audio. Filters shared studio logos. Also controls the pre-filter that skips impossible pairs before comparison. |
| `--timeout` | `600` | Per-file timeout in seconds. Increase for large files on NAS. |
| `--clear-cache` | — | Wipe all cached data and start fresh. |
| `--clear-comparisons` | — | Wipe comparisons only (keeps fingerprints). Use when changing thresholds. |
| `--clear-failed` | — | Retry previously failed files. |
| `--cleanup-cache` | — | Remove cached data for files no longer in scanned directories and shrink DB. |
| `--clear-dismissed` | — | Re-show previously skipped groups in the HTML report. |
| `--no-prompt` | — | Skip interactive setup, use all defaults. |

---

## Examples

```bash
# Scan multiple NAS shares
python VidClipDupli.py "\\NAS\Movies" "\\NAS\TV" "\\NAS\Clips"

# Scan local drive + NAS together (cross-drive is supported)
python VidClipDupli.py "C:\Clips" "Z:\Movies"

# Retry failed files with a higher timeout
python VidClipDupli.py "Z:\Videos" --clear-failed --timeout 900

# Find heavily edited clips (50% match threshold)
python VidClipDupli.py "D:\Videos" --clip-ratio 0.50

# Re-compare with new thresholds without re-extracting
python VidClipDupli.py "D:\Videos" --clear-comparisons --clip-ratio 0.60

# Find very short clips (disable the 30-second intro filter)
python VidClipDupli.py "D:\Videos" --intro-filter 0

# Clean up cached data for files you've deleted from the NAS
python VidClipDupli.py "Z:\Movies" --cleanup-cache
```

---

## Requirements

- **Python 3.8+**
- **numpy**, **tqdm** (`pip install numpy tqdm`)
- **fpcalc.exe** from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) — place in the script directory
- **Windows 10/11** (uses Windows-specific subprocess flags and short path API)
- **ffmpeg** (optional) — enables fallback for any file that fpcalc can't handle
- Works on any CPU (Intel, AMD, ARM). No GPU needed.

---

## Cache Details

The cache uses content-based keys (`quick_hash`) instead of file paths:

```
quick_hash = MD5(first_64KB + middle_64KB + last_64KB + file_size)[:16]
```

The three-point sampling (start, middle, end) prevents collisions between same-size CBR files from dashcams, GoPros, and security cameras, while keeping reads to just 192KB per file.

**Benefits:**
- Move files → cache still works
- Rename files → cache still works
- Reorganize library → no re-extraction needed

**Cache file:** `.audio_cache.db` in the script directory

**Schema:**
- `fingerprints` — content_key, current_path, file_size, fingerprint, duration
- `comparisons` — key1, key2, match_ratio, length_ratio, matched_seconds
- `failed_files` — content_key, last_path, reason
- `cache_params` — tracks algorithm version and comparison params; auto-clears stale data on upgrades
- `dismissed_pairs` — content_key pairs the user explicitly skipped in the HTML report

**Cache cleanup:** Use `--cleanup-cache` or option 5 in the interactive menu to remove cached data for files that have been deleted from the scanned directories. This is scoped — only data for files under the directories you're currently scanning is affected. Data from other directories (scanned in previous runs) is left untouched. Cleanup is automatically skipped if any files were inaccessible during scanning to prevent false deletion of valid cache entries.

---

## Dismissed Groups

When you skip groups in the HTML report, you can save your dismissals:

1. Click **Save Dismissed Groups (vcd_dismissed.json)** in the HTML report
2. Save the downloaded file next to `VidClipDupli.py`
3. On the next run, VCD automatically imports it and those groups are hidden by default

**How it works:** Dismissed groups are stored as content_key pairs in the SQLite cache. Since content_keys are based on file content (not paths), dismissed groups survive file moves and renames. Groups are only hidden when ALL pairs within the group have been dismissed — if a new file joins a previously-dismissed group, it will reappear.

**Manage dismissed groups:**
- In the HTML report, toggle "Show previously skipped" to reveal and optionally un-skip them
- Use `--clear-dismissed` or option 6 in the interactive menu to reset all dismissals
- Dismissed pairs for deleted files are cleaned up automatically with `--cleanup-cache`

---

## Ctrl+C and Partial Results

You can press Ctrl+C at any point during scanning, extraction, or comparison. VCD will:

1. **Save all progress** — Fingerprints and comparisons computed so far are committed to the SQLite cache. Nothing is lost.
2. **Generate a partial report** — The HTML report, JSON, and scripts are generated with whatever matches were found up to that point. The HTML shows a prominent warning banner indicating results are incomplete.
3. **Resume on next run** — Cached fingerprints and comparisons are reused automatically. Only the remaining uncached pairs need computing.

Press Ctrl+C once for graceful shutdown, twice to force quit (kills all worker processes immediately).

---

## Unicode & Special Characters

VCD handles filenames with Chinese, Japanese, Korean, emoji, brackets, `#`, `%`, `&`, and other special characters through a multi-step fallback chain:

1. **8.3 short path** — Converts to DOS-safe `FILENA~1.MKV` via Windows API
2. **Hardlink** — Creates a safe-named hardlink in the same directory (zero-copy)
3. **Symlink (same dir)** — If hardlink fails, tries a symlink (requires Developer Mode)
4. **Symlink (temp dir)** — If NAS is read-only, creates symlink in `%TEMP%` pointing to the NAS file (zero network I/O)
5. **Raw path** — If all else fail, passes the original path to fpcalc

No files are ever copied. The entire chain is zero-copy.

Filenames with `#`, `%`, and `?` are URL-encoded in the HTML report so clickable file links work correctly in all browsers.

---

## Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| **Audio-only matching** | Different videos with the same background music will match | Groups with >3x file size difference are auto-flagged |
| **Full audio decode** | First run on 3000+ files over Gigabit NAS takes 15-30 hours | Cached — subsequent runs are near-instant |
| **Content-based cache** | Editing a file (even just metadata) may trigger re-extraction | Only if first/middle/last 64KB or size changes |
| **Time-agnostic matching** | Compilations with scattered fragments may hit clip threshold | Review clip results before deleting |
| **Some containers** | fpcalc may fail on certain files (unusual codecs, corrupt headers) | Install ffmpeg for automatic fallback |
| **Pre-filter and intro filter** | Pairs where the shorter file is too short to pass the intro filter are skipped without caching | Running later with `--intro-filter 0` will compute these fresh |

Best for: movies, TV episodes, music, lectures, podcasts. Use caution with TikTok/meme folders or stock footage libraries where many videos share the same background music.

---

## Hardware Tested On

| Component | Spec | Notes |
|---|---|---|
| CPU | Intel 13700K (24 threads) | 18 comparison workers (75%) |
| RAM | 64 GB DDR5 | ~500 MB fingerprint data for 3000 files |
| Storage | 5.04 TB NAS via Gigabit Ethernet | 4-6 extraction workers saturates link |
| GPU | Intel Arc A770 | Not used — audio fingerprinting is CPU-only |

Minimum: 2-core CPU, 8 GB RAM. Worker counts auto-scale to your hardware.

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `fpcalc.exe not found` | Missing binary | Download from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) |
| Mass `exit code 1` with empty stderr | Timeout on large files over NAS | `--clear-failed --timeout 900` |
| Files failing with fpcalc errors | Container or codec not supported | Install ffmpeg for automatic fallback |
| Files with `#`, `[]`, `{}`, `%` in name | Special chars confuse fpcalc | Safe-path fallback handles this automatically |
| `Another instance is already running` | Multiple VCD processes | Close other instance, or wait for it to finish |
| `database is locked` | Stale lock file after crash | Delete `.vcd_instance.lock` in script directory |
| PowerShell "scripts disabled" error | Execution policy blocking | Use `RUN_CUSTOM_ACTIONS.bat` instead |
| False positives (shared music) | Audio-only limitation | Size-warning flag auto-comments these in scripts |
| Stuck at Phase 3 start | Serializing arrays to temp file | Normal for large libraries, wait ~10s |
| Stuck at "Computing content keys" | NAS latency on many files | Normal — reads 192KB per file |
| Long paths failing (>260 chars) | Deep NAS folder structure | Handled automatically via extended-length paths |
| UNC paths not opening from HTML | Browser security restriction | Works in Chrome/Edge; some browsers block `file://` URLs |
| Cache DB very large | Orphaned data from deleted files | Use `--cleanup-cache` or option 5 in interactive menu |
| Phase 3 ETA jumps around | Normal for batch-based processing | ETA is smoothed — let it stabilize over a few minutes |
| Phase 3 shows CLIP not DUP | Files match 100% but differ in length | Correct — shorter audio is fully inside the longer file. DUP requires same length (±10%). |

---

## License

AGPL-3.0 — See [LICENSE](LICENSE) for details.
