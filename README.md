# VidClipDuplis

An audio-based duplicate and clip finder for large NAS media libraries.

Standard deduplication tools compare file hashes — they fail the moment a video is re-encoded, repackaged as a different container, or slightly trimmed. **VidClipDuplis listens to the audio track instead.** It extracts a [Chromaprint](https://acoustid.org/chromaprint) audio fingerprint from every file and compares them all, finding matches regardless of resolution, codec, bitrate, or container format.

It finds two things:
1. **Exact Duplicates** — The same content in different formats (e.g., a 1080p `.mkv` and a 4K `.mp4` of the same movie).
2. **Clips** — A shorter file whose audio lives inside a longer file (e.g., a trailer sitting next to the full movie, or a scene rip from a longer compilation).

> **⚠️ Safe by design.** This script **never deletes files**. It generates an interactive HTML report where you review every match and choose what to keep, delete, or rename — then exports a custom PowerShell script with your exact choices.

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

---

## How It Works

```
Phase 0: Scan        Find all media files in target directories
Phase 0.5: Hash      Compute content keys (quick_hash) for cache lookup
Phase 1: Extract     fpcalc decodes the full audio stream of every file → uint32 fingerprint array
Phase 2: Cache       Look up previously computed comparisons in SQLite
Phase 3: Compare     Sliding-window XOR + popcount across all pairs (multi-core)
Phase 4: Group       Union-Find for duplicates, directed edges for clips
Output:              HTML report + .bat + .ps1 + .json
```

**Fingerprints** are arrays of 32-bit integers (~6 per second of audio). Comparing two files means sliding the shorter array across the longer one, XOR-ing chunks, and counting differing bits. A low hamming distance = matching audio.

**Content-Based Caching** stores fingerprints and comparisons keyed by a hash of the file's content (first+last 64KB + size), not by path. This means you can rename or move files and the cache still applies.

**Threshold Re-evaluation** stores raw comparison metrics (match_ratio, length_ratio, matched_seconds) — not boolean results. This means you can change `--clip-ratio` or `--dup-ratio` between runs and cached metrics are re-evaluated against the new thresholds instantly, without re-extracting or re-comparing.

---

## Interactive HTML Report

The star feature. Open `review_results.html` in any browser:

- Each duplicate group is a card showing every file with size, duration, and full path
- **Keep** / **Delete** / **Use Name** buttons on every file
- "Use Name" keeps the higher-quality file but renames it to the other file's name
- **Skip** button to leave a group untouched
- Live counters for deletions, renames, and skips
- Big **Download Custom PowerShell Script** button at the bottom

The generated `.ps1` contains only the actions you chose — `Remove-Item` for deletions, `Rename-Item` for renames. No surprises.

---

## What It Outputs

```
📂 results_Movies_TV_20260320_143022/
├── review_results.html       ← Interactive — open in browser
├── duplicate_report.json      Full structured data
├── delete_duplicates.bat      cmd.exe script (basic)
├── delete_duplicates.ps1      PowerShell script (handles long paths + Unicode)
├── RUN_DELETIONS.bat          Launches PowerShell with ExecutionPolicy bypass
└── fpcalc_debug.log           Anonymized failure log (if errors occurred)
```

**Which script should I use?**
- **RUN_DELETIONS.bat** — Recommended. Double-click this to run the PowerShell script without needing to change system settings.
- **delete_duplicates.ps1** — The actual deletion script. Use directly if you've already enabled PowerShell scripts.
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
| `--intro-filter` | `30` | Ignore matches shorter than N seconds of audio. Filters shared studio logos. |
| `--timeout` | `600` | Per-file timeout in seconds. Increase for large files on NAS. |
| `--clear-cache` | — | Wipe all cached data and start fresh. |
| `--clear-comparisons` | — | Wipe comparisons only (keeps fingerprints). Use when changing thresholds. |
| `--clear-failed` | — | Retry previously failed files. |
| `--no-prompt` | — | Skip interactive setup, use all defaults. |

---

## Examples

```bash
# Scan multiple NAS shares
python VidClipDupli.py "\\NAS\Movies" "\\NAS\TV" "\\NAS\Clips"

# Retry failed files with a higher timeout
python VidClipDupli.py "Z:\Videos" --clear-failed --timeout 900

# Find heavily edited clips (50% match threshold)
python VidClipDupli.py "D:\Videos" --clip-ratio 0.50

# Re-compare with new thresholds without re-extracting
python VidClipDupli.py "D:\Videos" --clear-comparisons --clip-ratio 0.60

# Find very short clips (disable the 30-second intro filter)
python VidClipDupli.py "D:\Videos" --intro-filter 0
```

---

## Requirements

- **Python 3.8+**
- **numpy**, **tqdm** (`pip install numpy tqdm`)
- **fpcalc.exe** from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) — place in the script directory
- **Windows 10/11** (uses Windows-specific subprocess flags and short path API)
- Works on any CPU (Intel, AMD, ARM). No GPU needed.

---

## Cache Details

The cache uses content-based keys (`quick_hash`) instead of file paths:

```
quick_hash = MD5(first_64KB + last_64KB + file_size)[:16]
```

**Benefits:**
- Move files → cache still works
- Rename files → cache still works
- Reorganize library → no re-extraction needed

**Cache file:** `.audio_cache.db` in the script directory

**Schema:**
- `fingerprints` — content_key (PRIMARY KEY), current_path, file_size, fingerprint, duration
- `comparisons` — key1, key2 (PRIMARY KEY), match_ratio, length_ratio, matched_seconds
- `failed_files` — content_key (PRIMARY KEY), last_path, reason

---

## Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| **Audio-only matching** | Different videos with the same background music will match | Groups with >3x file size difference are auto-flagged |
| **Full audio decode** | First run on 3000+ files over Gigabit NAS takes 15-30 hours | Cached — subsequent runs are near-instant |
| **Content-based cache** | Editing a file (even just metadata) may trigger re-extraction | Only if first/last 64KB or size changes |
| **Time-agnostic matching** | Compilations with scattered fragments may hit clip threshold | Review clip results before deleting |

Best for: movies, TV episodes, music, lectures, podcasts. Use caution with TikTok/meme folders or stock footage libraries where many videos share the same background music.

---

## Hardware Tested On

| Component | Spec | Notes |
|---|---|---|
| CPU | Intel 13700K (24 threads) | 18 comparison workers (75%) |
| RAM | 64 GB DDR5 | ~500 MB fingerprint data for 3000 files |
| Storage | 4.44 TB NAS via Gigabit Ethernet | 4-6 extraction workers saturates link |
| GPU | Intel Arc A770 | Not used — audio fingerprinting is CPU-only |

Minimum: 4-core CPU, 8 GB RAM. Reduce `-w` and `-c` for slower hardware.

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `fpcalc.exe not found` | Missing binary | Download from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) |
| Mass `exit code 1` with empty stderr | Timeout on large files over NAS | `--clear-failed --timeout 900` |
| `.ts` files failing | Brackets `[]` in filename | Safe-path fallback handles this automatically |
| `Another instance is already running` | Multiple VCD processes | Close other instance, or wait for it to finish |
| `database is locked` | Stale lock file after crash | Delete `.vcd_instance.lock` in script directory |
| PowerShell "scripts disabled" error | Execution policy blocking | Use `RUN_DELETIONS.bat` instead |
| False positives (shared music) | Audio-only limitation | Size-warning flag auto-comments these in scripts |
| Stuck at Phase 3 start | Serializing arrays to temp file | Normal for large libraries, wait ~10s |
| Stuck at "Computing content keys" | NAS latency on many files | Normal — reads 128KB per file |
| Long paths failing (>260 chars) | Deep NAS folder structure | Handled automatically via Windows extended-length paths |

---

## License

AGPL-3.0 — See [LICENSE](LICENSE) for details.
