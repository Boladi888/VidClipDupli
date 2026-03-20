#!/usr/bin/env python3
r"""
VidClipDuplis (VCD) v27
=======================
Audio Duplicate & Clip Finder using Chromaprint (fpcalc).
NAS-safe, file-based IPC, CPU-only.

THIS SCRIPT NEVER DELETES FILES. It generates a report, a .bat file,
and a .ps1 (PowerShell) script for you to review and run if you choose.

AUDIO-ONLY MATCHING: This compares audio tracks, not video frames.
Different videos with the same background music will match.
Best for: movies, TV, music, lectures. Review results for meme/TikTok folders.

Features:
- Multi-folder support (unlimited directories, comma/semicolon separated)
- Interactive setup with cache management and CLI tips
- Anonymous debug log for failures (no filenames logged)
- Ctrl+C graceful shutdown with progress saving
- Windows 8.3 short path + hardlink fallback for unsafe filenames
  (Chinese, Japanese, emoji, brackets, special characters)
- Cache: .audio_cache_v1a.db

Requirements: fpcalc.exe in script directory (from acoustid.org/chromaprint)
              pip install numpy tqdm
"""

import os
import sys
import json
import subprocess
import argparse
import time
import threading
import signal
import sqlite3
import tempfile
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
import multiprocessing

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from tqdm import tqdm

# ============================================================================
# GLOBALS
# ============================================================================

if getattr(sys, 'frozen', False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FPCALC_PATH = os.path.join(_BASE_DIR, 'fpcalc.exe')
GLOBAL_ARRAYS: Dict[str, np.ndarray] = {}
FPCALC_MAX_STDOUT_BYTES = 50 * 1024 * 1024
_ARRAYS_TEMP_PATH: Optional[str] = None
_debug_logger: Optional[logging.Logger] = None

def setup_debug_logger(results_dir: str) -> logging.Logger:
    """Create anonymized debug log for fpcalc failures."""
    logger = logging.getLogger('fpcalc_debug')
    logger.setLevel(logging.DEBUG)
    log_path = os.path.join(results_dir, 'fpcalc_debug.log')
    handler = logging.FileHandler(log_path, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(handler)
    return logger

def init_worker(arrays_file: str):
    """Load hash_arrays from temp file (bypasses pickle IPC pipe)."""
    global GLOBAL_ARRAYS
    import pickle
    import time
    for attempt in range(10):
        try:
            with open(arrays_file, 'rb') as f:
                GLOBAL_ARRAYS = pickle.load(f)
            return
        except PermissionError:
            if attempt < 9:
                time.sleep(0.5)
            else:
                raise RuntimeError("Failed to load hash arrays: file locked (antivirus?)")

def save_arrays_for_workers(hash_arrays: Dict[str, np.ndarray]) -> str:
    """Serialize hash_arrays to a temp file. Returns the filepath."""
    import pickle
    fd, path = tempfile.mkstemp(suffix='.pkl', prefix='audiocache_arrays_')
    try:
        with os.fdopen(fd, 'wb') as f:
            pickle.dump(hash_arrays, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        os.close(fd)
        raise
    return path

def cleanup_arrays_file():
    """Remove the temp arrays file."""
    global _ARRAYS_TEMP_PATH
    if _ARRAYS_TEMP_PATH and os.path.exists(_ARRAYS_TEMP_PATH):
        try:
            os.remove(_ARRAYS_TEMP_PATH)
        except OSError:
            pass
        _ARRAYS_TEMP_PATH = None

def fast_popcount_uint32(a: np.ndarray) -> np.ndarray:
    """Bare-metal bitwise popcount fallback for NumPy < 2.0."""
    a = a - ((a >> 1) & np.uint32(0x55555555))
    a = (a & np.uint32(0x33333333)) + ((a >> np.uint32(2)) & np.uint32(0x33333333))
    a = (a + (a >> np.uint32(4))) & np.uint32(0x0F0F0F0F)
    a = a + (a >> np.uint32(8))
    a = a + (a >> np.uint32(16))
    return a & np.uint32(0x0000003F)

HAS_BITWISE_COUNT = hasattr(np, 'bitwise_count')

SHUTDOWN_REQUESTED = False
CTRL_C_COUNT = 0
ACTIVE_PROCESSES: List[subprocess.Popen] = []
PROCESS_LOCK = threading.Lock()

def signal_handler(signum, frame):
    global SHUTDOWN_REQUESTED, CTRL_C_COUNT
    CTRL_C_COUNT += 1
    if CTRL_C_COUNT >= 2:
        print("\n\n🛑 Force quitting — killing active processes...")
        with PROCESS_LOCK:
            for proc in ACTIVE_PROCESSES:
                _kill_proc(proc)
        os._exit(1)
    SHUTDOWN_REQUESTED = True
    print("\n\n⚠️ Shutdown requested... Press Ctrl+C again to force quit.")

# ============================================================================
# CONFIGURATION & UTILS
# ============================================================================

MEDIA_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.webm', '.m4v',
    '.mpg', '.mpeg', '.3gp', '.ts', '.mts', '.m2ts', '.flv',
    '.mp3', '.wav', '.flac', '.ogg', '.aac', '.m4a', '.wma',
    '.vob', '.ogv', '.divx',
}

@dataclass
class Config:
    max_workers: int = 6
    comparison_workers: int = 18       # Updated at runtime to 75% of CPUs
    comparison_batch_size: int = 50
    chunk_size: int = 15
    match_threshold: int = 60
    clip_match_ratio: float = 0.75
    duplicate_match_ratio: float = 0.95
    intro_filter_seconds: float = 30.0
    video_timeout: int = 600

def _normalize_pair(p1: str, p2: str) -> Tuple[str, str]:
    return (p1, p2) if p1 <= p2 else (p2, p1)

def _bat_safe_path(path: str) -> str:
    """Absolute path with % escaped to %% for cmd.exe."""
    return os.path.abspath(path).replace('%', '%%')

def get_results_folder_path(script_dir: str, root_dirs: List[str]) -> str:
    clean = lambda s: "".join(c if c.isalnum() or c in "-_" else "_" for c in s)[:20]
    ts = time.strftime('%Y%m%d_%H%M%S')
    if len(root_dirs) == 1:
        p = Path(root_dirs[0])
        parent = p.parent.name if p.parent.name else "root"
        return os.path.join(script_dir, f"results_{clean(parent)}_{clean(p.name)}_{ts}")
    else:
        names = "_".join(clean(Path(d).name) for d in root_dirs[:3])
        if len(root_dirs) > 3:
            names += f"_+{len(root_dirs) - 3}"
        return os.path.join(script_dir, f"results_{names}_{ts}")

def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def format_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h > 0 else f"{m}m {s}s"

class UnionFind:
    """Used ONLY for exact duplicates. Clips are tracked separately."""
    def __init__(self):
        self.parent = {}
        self.rank = {}
    def find(self, i):
        if self.parent.setdefault(i, i) != i:
            self.parent[i] = self.find(self.parent[i])
        return self.parent[i]
    def union(self, i, j):
        ri, rj = self.find(i), self.find(j)
        if ri != rj:
            rki, rkj = self.rank.get(ri, 0), self.rank.get(rj, 0)
            if rki < rkj: ri, rj = rj, ri
            self.parent[rj] = ri
            if rki == rkj: self.rank[ri] = rki + 1

# ============================================================================
# INTERACTIVE SETUP
# ============================================================================

def prompt_with_default(prompt: str, default: str) -> str:
    try:
        user_input = input(f"   {prompt} [{default}]: ").strip()
        return user_input if user_input else default
    except (EOFError, KeyboardInterrupt):
        return default

def interactive_setup(cpu_count: int, cache: 'UnifiedCache') -> Config:
    """Interactive configuration with settings preview, cache management, and CLI tips."""
    comp_default = max(4, int(cpu_count * 0.75))
    default_scale = 5
    dup_ratio = 0.99 - (default_scale - 1) * 0.005
    clip_ratio = 0.90 - (default_scale - 1) * 0.02
    intro_filter = max(5.0, 40.0 - (default_scale - 1) * 3)
    
    print("\n" + "─" * 60)
    print("  SETUP")
    print("─" * 60)
    print(f"  Detected {cpu_count} CPU cores")
    
    print(f"\n  Recommended settings:")
    print(f"    Extraction workers:  6")
    print(f"      These are parallel fpcalc processes reading files from")
    print(f"      your disk/NAS. Each one streams an entire file's audio.")
    print(f"      Too many = disk thrashing and timeouts on mechanical drives.")
    print(f"        Gigabit Ethernet NAS:  4-6  (saturates at ~940 Mbps)")
    print(f"        2.5GbE NAS:            8-10")
    print(f"        Local SSD:             8-12")
    print(f"    Comparison workers:  {comp_default}  (75% of {cpu_count} cores)")
    print(f"      These are CPU processes doing heavy math (XOR + popcount")
    print(f"      on every chunk pair). This WILL pin your CPU at near 100%%.")
    print(f"      Leave some cores free for your OS and other apps.")
    print(f"    Sensitivity:         {default_scale}/10  (balanced)")
    print(f"      1  = Very strict. Only near-identical audio. Almost no")
    print(f"           false positives, but may miss re-encoded duplicates.")
    print(f"      5  = Balanced. Catches re-encodes, different bitrates,")
    print(f"           and most clips. Good starting point.")
    print(f"      10 = Very loose. Catches heavily edited clips and partial")
    print(f"           matches. More false positives from shared music.")
    print(f"    Intro filter:        {intro_filter:.0f}s")
    print(f"      Ignores matches shorter than this many seconds of audio.")
    print(f"      Filters out shared studio logos (Netflix ta-dum, HBO static).")
    print(f"      Set to 0 if you want to find very short clips (<30s).")
    print(f"    Timeout:             600s  (per file)")
    print(f"      fpcalc must decode the entire audio stream over the network.")
    print(f"      Bigger files need more time:")
    print(f"        1-2 GB files:   300s should be plenty")
    print(f"        3-5 GB files:   600s (default)")
    print(f"        5-10 GB files:  900s recommended")
    print(f"        10+ GB files:   1200s or more")

    print(f"\n  Cache status:")
    fp_count = cache.get_fingerprint_count()
    cmp_count = cache.get_comparison_count()
    failed_count = cache.get_failed_count()
    print(f"    Fingerprints cached: {fp_count:,}")
    print(f"    Comparisons cached:  {cmp_count:,}")
    print(f"    Failed files:        {failed_count:,}")
    
    print(f"\n  Cache management:")
    print(f"    1. Clear failed list     (retry files that previously failed)")
    print(f"    2. Clear comparisons     (re-compare with new thresholds)")
    print(f"    3. Clear everything      (start completely fresh)")
    print(f"    4. No changes            (keep cache as-is)")
    
    cache_choice = input("\n  Cache action? [4]: ").strip()
    if cache_choice == '1':
        cache.clear_failed()
        print("  🗑️  Failed list cleared")
    elif cache_choice == '2':
        cache.clear_comparisons()
        print("  🗑️  Comparisons cleared")
    elif cache_choice == '3':
        cache.clear_fingerprints()
        cache.clear_comparisons()
        cache.clear_failed()
        print("  🗑️  All caches cleared")

    print(f"\n  CLI tip: you can skip this menu with command-line flags:")
    print(f"    --no-prompt              Use defaults, skip interactive setup")
    print(f"    --clear-failed           Retry previously failed files")
    print(f"    --clear-comparisons      Re-compare with different thresholds")
    print(f"    --clear-cache            Wipe everything and start fresh")
    print(f"    -w 4                     Set extraction workers")
    print(f"    -c 16                    Set comparison workers")
    print(f"    --clip-ratio 0.50        Set clip match threshold")
    print(f"    --timeout 900            Timeout per file in seconds")
    print(f"    --intro-filter 0         Find very short clips")

    response = input(f"\n  Use recommended settings? [Y/n]: ").strip().lower()
    if response in ('n', 'no'):
        print()
        workers_str = prompt_with_default("Extraction workers (fpcalc instances)", "6")
        workers = int(workers_str) if workers_str.isdigit() else 6

        comp_str = prompt_with_default("Comparison workers (CPU cores)", str(comp_default))
        comp_workers = int(comp_str) if comp_str.isdigit() else comp_default

        print()
        print("   Sensitivity: 1-10  (see descriptions above)")
        scale_str = prompt_with_default("Sensitivity", str(default_scale))
        scale = max(1, min(10, int(scale_str) if scale_str.isdigit() else default_scale))

        dup_ratio = 0.99 - (scale - 1) * 0.005
        clip_ratio = 0.90 - (scale - 1) * 0.02
        intro_filter = max(5.0, 40.0 - (scale - 1) * 3)

        print()
        intro_str = prompt_with_default("Intro filter seconds (0 = find short clips)", f"{intro_filter:.0f}")
        try:
            intro_filter = max(0.0, float(intro_str))
        except ValueError:
            pass

        print()
        timeout_str = prompt_with_default("Timeout per file (seconds)", "600")
        timeout = int(timeout_str) if timeout_str.isdigit() else 600

        return Config(
            max_workers=workers,
            comparison_workers=comp_workers,
            clip_match_ratio=round(clip_ratio, 3),
            duplicate_match_ratio=round(dup_ratio, 3),
            intro_filter_seconds=intro_filter,
            video_timeout=timeout,
        )
    else:
        return Config(comparison_workers=comp_default, video_timeout=600)

# ============================================================================
# UNIFIED SQLITE CACHE
# ============================================================================

class UnifiedCache:
    """SQLite cache for fingerprints, comparisons, and failed files."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            self.local.conn = sqlite3.connect(self.db_path, timeout=30.0)
            self.local.conn.execute("PRAGMA journal_mode=WAL")
            self.local.conn.execute("PRAGMA synchronous=NORMAL")
            self.local.conn.execute("PRAGMA temp_store = MEMORY")
            self.local.conn.execute("PRAGMA mmap_size = 2147483648")
            self.local.conn.execute("PRAGMA cache_size = -500000")
        return self.local.conn

    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS fingerprints (
            path TEXT PRIMARY KEY, file_size INTEGER, mtime REAL,
            fingerprint BLOB, duration REAL, processed_at TEXT)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS comparisons (
            path1 TEXT NOT NULL, path2 TEXT NOT NULL,
            match_ratio REAL NOT NULL, length_ratio REAL NOT NULL,
            matched_seconds REAL NOT NULL, PRIMARY KEY (path1, path2))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cmp_p1 ON comparisons(path1)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cmp_p2 ON comparisons(path2)")
        conn.execute("CREATE TABLE IF NOT EXISTS failed_files (path TEXT PRIMARY KEY, reason TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS cache_params (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

    def validate_params(self, config: Config) -> bool:
        conn = self._get_conn()
        params = {'chunk_size': str(config.chunk_size), 'match_threshold': str(config.match_threshold)}
        try:
            stored = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM cache_params")}
        except sqlite3.Error:
            stored = {}
        if not stored:
            conn.executemany("INSERT OR REPLACE INTO cache_params (key, value) VALUES (?, ?)", list(params.items()))
            conn.commit()
            return True
        mismatched = [f"{k}: {stored.get(k,'?')} -> {v}" for k, v in params.items() if stored.get(k) != v]
        if mismatched:
            print(f"   Warning: Algorithm params changed: {', '.join(mismatched)}")
            print(f"   Auto-clearing comparison cache")
            conn.execute("DELETE FROM comparisons")
            conn.executemany("INSERT OR REPLACE INTO cache_params (key, value) VALUES (?, ?)", list(params.items()))
            conn.commit()
            return False
        return True

    def get_all_fingerprints(self) -> Dict[str, Dict]:
        fps = {}
        try:
            for row in self._get_conn().execute("SELECT path, file_size, mtime, fingerprint, duration FROM fingerprints"):
                arr = np.frombuffer(row[3], dtype=np.uint32).copy() if row[3] else None
                fps[row[0]] = {'file_size': row[1], 'mtime': row[2], 'duration': row[4], 'fingerprint': arr}
        except sqlite3.Error as e:
            print(f"Warning: Could not load fingerprints: {e}")
        return fps

    def set_fingerprint(self, path, arr, size, mtime, duration):
        blob = arr.tobytes() if arr is not None else None
        try:
            conn = self._get_conn()
            conn.execute("INSERT OR REPLACE INTO fingerprints (path,file_size,mtime,fingerprint,duration,processed_at) VALUES (?,?,?,?,?,datetime('now'))",
                         (path, size, mtime, blob, duration))
            conn.commit()
        except sqlite3.Error as e:
            print(f"Warning: Could not save fingerprint: {e}")

    def get_many_comparisons(self, pairs):
        results = {}
        if not pairs: return results
        conn = self._get_conn()
        try:
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_pairs (path1 TEXT, path2 TEXT)")
            for i in range(0, len(pairs), 100000):
                conn.execute("DELETE FROM temp_pairs")
                conn.executemany("INSERT INTO temp_pairs VALUES (?,?)", pairs[i:i+100000])
                for row in conn.execute("SELECT c.path1,c.path2,c.match_ratio,c.length_ratio,c.matched_seconds FROM temp_pairs t INNER JOIN comparisons c ON t.path1=c.path1 AND t.path2=c.path2"):
                    results[(row[0], row[1])] = (row[2], row[3], row[4])
            conn.execute("DELETE FROM temp_pairs")
        except sqlite3.Error as e:
            print(f"Warning: Batch lookup error: {e}")
        return results

    def batch_set_comparisons(self, results):
        if not results: return
        try:
            conn = self._get_conn()
            conn.executemany("INSERT OR REPLACE INTO comparisons (path1,path2,match_ratio,length_ratio,matched_seconds) VALUES (?,?,?,?,?)", results)
            conn.commit()
        except sqlite3.Error as e:
            print(f"Warning: Batch insert error: {e}")

    def wal_checkpoint(self):
        try: self._get_conn().execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error: pass

    def is_failed(self, path):
        try: return self._get_conn().execute("SELECT 1 FROM failed_files WHERE path=?", (path,)).fetchone() is not None
        except sqlite3.Error: return False

    def mark_failed(self, path, reason):
        try:
            conn = self._get_conn()
            conn.execute("INSERT OR REPLACE INTO failed_files (path,reason) VALUES (?,?)", (path, reason))
            conn.commit()
        except sqlite3.Error: pass

    def clear_fingerprints(self):
        try: c = self._get_conn(); c.execute("DELETE FROM fingerprints"); c.commit()
        except sqlite3.Error: pass
    def clear_comparisons(self):
        try: c = self._get_conn(); c.execute("DELETE FROM comparisons"); c.commit()
        except sqlite3.Error: pass
    def clear_failed(self):
        try: c = self._get_conn(); c.execute("DELETE FROM failed_files"); c.commit()
        except sqlite3.Error: pass
    def get_failed_count(self):
        try: return self._get_conn().execute("SELECT COUNT(*) FROM failed_files").fetchone()[0]
        except sqlite3.Error: return 0
    def get_comparison_count(self):
        try: return self._get_conn().execute("SELECT COUNT(*) FROM comparisons").fetchone()[0]
        except sqlite3.Error: return 0
    def get_fingerprint_count(self):
        try: return self._get_conn().execute("SELECT COUNT(*) FROM fingerprints").fetchone()[0]
        except sqlite3.Error: return 0

# ============================================================================
# EXTRACTION
# ============================================================================

def _drain_stderr(pipe, result_holder):
    try:
        result_holder[0] = pipe.read(4096)
        pipe.read()
    except Exception: pass

def _kill_proc(proc):
    if proc is None: return
    try: proc.kill(); proc.wait(timeout=5)
    except Exception: pass

def _get_short_path(long_path: str) -> str:
    """
    Convert a path to a DOS 8.3 short path for legacy C binaries.
    Returns the original path if not on Windows or API fails.
    """
    if os.name != 'nt':
        return long_path
    try:
        import ctypes
        from ctypes import wintypes
        GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        GetShortPathNameW.restype = wintypes.DWORD
        
        abs_path = os.path.abspath(long_path)
        buf_size = GetShortPathNameW(abs_path, None, 0)
        if buf_size == 0:
            return long_path
        buffer = ctypes.create_unicode_buffer(buf_size)
        GetShortPathNameW(abs_path, buffer, buf_size)
        return buffer.value
    except Exception:
        return long_path

# Characters that cause fpcalc/FFmpeg to choke:
# [ ] are FFmpeg glob/sequence patterns
# Non-ASCII triggers ANSI code page corruption in legacy C binaries
_FPCALC_UNSAFE_CHARS = set('[]{}')

def _is_fpcalc_safe(path: str) -> bool:
    """Check if a path is safe to pass directly to fpcalc."""
    if not path.isascii():
        return False
    return not any(c in _FPCALC_UNSAFE_CHARS for c in path)

def _make_safe_path(original_path: str) -> Tuple[str, Optional[str]]:
    """
    Get an fpcalc-safe path. Tries 8.3 short path first.
    If that still has unsafe chars, creates a temp hardlink (same volume, instant)
    or temp copy (cross-volume) with a safe ASCII name.
    
    Returns (safe_path, cleanup_path_or_None).
    If cleanup_path is not None, caller must delete it after use.
    """
    # Try 8.3 short path first (instant, no copy)
    short = _get_short_path(original_path)
    if _is_fpcalc_safe(short):
        return short, None
    
    # 8.3 didn't help — create a temp file with a safe name
    # Use the same directory to try hardlink first (instant, zero-copy)
    ext = os.path.splitext(original_path)[1].lower()
    if not ext or not ext.isascii():
        ext = '.tmp'
    safe_ext = ''.join(c for c in ext if c.isalnum() or c == '.')
    
    try:
        # Try hardlink in temp dir (works if same volume, instant, no disk space)
        fd, temp_path = tempfile.mkstemp(suffix=safe_ext, prefix='vcd_safe_')
        os.close(fd)
        os.remove(temp_path)  # Remove the empty file so hardlink can take its place
        os.link(original_path, temp_path)
        return temp_path, temp_path
    except OSError:
        pass
    
    try:
        # Cross-volume: copy to temp (costs time + disk space but always works)
        fd, temp_path = tempfile.mkstemp(suffix=safe_ext, prefix='vcd_safe_')
        os.close(fd)
        import shutil
        shutil.copy2(original_path, temp_path)
        return temp_path, temp_path
    except OSError:
        # Give up — try the original path and let fpcalc fail with a clear error
        return original_path, None

def extract_audio_fingerprint(args):
    """Extract Chromaprint fingerprint. Returns 7-tuple including raw stderr for debug."""
    path, size, mtime, video_timeout = args
    process = None
    cleanup_path = None
    try:
        safe_path, cleanup_path = _make_safe_path(path)
        cmd = [FPCALC_PATH, '-raw', '-length', '0', '-json', safe_path]
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW)
        with PROCESS_LOCK:
            ACTIVE_PROCESSES.append(process)

        stderr_result = [b'']
        stderr_thread = threading.Thread(target=_drain_stderr, args=(process.stderr, stderr_result), daemon=True)
        stderr_thread.start()

        timer = threading.Timer(video_timeout, lambda: _kill_proc(process))
        timer.start()
        try:
            stdout_bytes = process.stdout.read(FPCALC_MAX_STDOUT_BYTES + 1)
            process.wait()
            stderr_thread.join(timeout=2.0)
        finally:
            timer.cancel()
            with PROCESS_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)

        stderr_bytes = stderr_result[0]

        if len(stdout_bytes) > FPCALC_MAX_STDOUT_BYTES:
            _kill_proc(process)
            return path, size, mtime, None, 0.0, "fpcalc stdout exceeded 50MB cap", b''

        if process.returncode != 0:
            err = stderr_bytes.decode('utf-8', errors='ignore').strip()[:200]
            return path, size, mtime, None, 0.0, err or f"fpcalc exit code {process.returncode}", stderr_bytes

        data = json.loads(stdout_bytes.decode('utf-8'))
        fp = data.get('fingerprint', [])
        if not fp:
            return path, size, mtime, None, 0.0, "Empty fingerprint (no audio stream?)", b''
        return path, size, mtime, np.array(fp, dtype=np.uint32), data.get('duration', 0.0), "", b''

    except subprocess.TimeoutExpired:
        _kill_proc(process)
        return path, size, mtime, None, 0.0, f"Timeout ({video_timeout}s)", b''
    except json.JSONDecodeError as e:
        return path, size, mtime, None, 0.0, f"Invalid fpcalc JSON: {e}", b''
    except Exception as e:
        _kill_proc(process)
        return path, size, mtime, None, 0.0, str(e)[:200], b''
    finally:
        # Always clean up temp hardlink/copy
        if cleanup_path:
            try:
                os.remove(cleanup_path)
            except OSError:
                pass

# ============================================================================
# COMPARISON
# ============================================================================

def compare_audio_pair(path1, path2, config):
    """Compare two fingerprints. Returns raw metrics (no early exit)."""
    global GLOBAL_ARRAYS
    arr1, arr2 = GLOBAL_ARRAYS.get(path1), GLOBAL_ARRAYS.get(path2)
    if arr1 is None or arr2 is None:
        return path1, path2, 0.0, 0.0, 0.0
    arr_short, arr_long = (arr1, arr2) if len(arr1) <= len(arr2) else (arr2, arr1)
    if len(arr_short) < config.chunk_size:
        return path1, path2, 0.0, 0.0, 0.0

    length_ratio = abs(len(arr_short) - len(arr_long)) / max(len(arr_short), len(arr_long))
    num_chunks = len(arr_short) // config.chunk_size
    chunks_short = arr_short[:num_chunks * config.chunk_size].reshape(num_chunks, config.chunk_size)
    window_long = sliding_window_view(arr_long, config.chunk_size)

    matched_chunks = 0
    for chunk in chunks_short:
        xor_result = window_long ^ chunk
        if HAS_BITWISE_COUNT:
            distances = np.sum(np.bitwise_count(xor_result), axis=1)
        else:
            distances = np.sum(fast_popcount_uint32(xor_result), axis=1)
        if np.min(distances) < config.match_threshold:
            matched_chunks += 1

    match_ratio = matched_chunks / num_chunks if num_chunks else 0.0
    matched_seconds = matched_chunks * (config.chunk_size / 6.0)
    return path1, path2, match_ratio, length_ratio, matched_seconds

def classify_comparison(match_ratio, length_ratio, matched_seconds, config):
    """Apply thresholds to raw metrics. Returns (is_dup, is_clip)."""
    if matched_seconds < config.intro_filter_seconds and match_ratio > 0:
        return False, False
    is_dup = match_ratio >= config.duplicate_match_ratio and length_ratio < 0.1
    is_clip = not is_dup and match_ratio >= config.clip_match_ratio
    return is_dup, is_clip

def compare_batch(batch, config):
    results = []
    for p1, p2 in batch:
        _, _, mr, lr, ms = compare_audio_pair(p1, p2, config)
        results.append((*_normalize_pair(p1, p2), mr, lr, ms))
    return results

# ============================================================================
# HTML INTERACTIVE REPORT
# ============================================================================

def _html_escape(s: str) -> str:
    """Escape HTML special characters."""
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#39;')

def _js_escape(s: str) -> str:
    """Escape string for JavaScript."""
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"').replace('\n', '\\n')

def _generate_html_report(results_dir, dup_groups, clip_deletions, fingerprints, display_root, total_delete, total_savings):
    """Generate an interactive HTML report for reviewing duplicates."""
    html_path = os.path.join(results_dir, 'review_results.html')

    # Build data for JS
    groups_js = []
    for gi, g in enumerate(dup_groups):
        files_js = []
        for v in g['videos']:
            abs_path = os.path.abspath(v['path'])
            files_js.append({
                'path': abs_path,
                'relpath': os.path.relpath(v['path'], display_root),
                'filename': os.path.basename(v['path']),
                'dir': os.path.dirname(abs_path),
                'size': v['size'],
                'size_fmt': format_size(v['size']),
                'duration': v.get('duration', 0),
                'dur_fmt': format_duration(v['duration']) if v.get('duration') else '?',
                'is_keep': v['path'] == g['recommend_keep'],
            })
        groups_js.append({
            'id': gi,
            'files': files_js,
            'savings_fmt': format_size(g['potential_savings']),
            'size_warning': g.get('size_warning', False),
        })

    clips_js = []
    for ci, (child, parents, ratio) in enumerate(clip_deletions):
        fp = fingerprints.get(child, {})
        clips_js.append({
            'id': ci,
            'child_path': os.path.abspath(child),
            'child_rel': os.path.relpath(child, display_root),
            'child_name': os.path.basename(child),
            'child_size': format_size(fp.get('file_size', 0)),
            'child_dur': format_duration(fp.get('duration', 0)),
            'ratio': f"{ratio*100:.0f}%",
            'parents': [os.path.basename(p) for p in parents[:3]],
            'delete': True,
        })

    data_json = json.dumps({'groups': groups_js, 'clips': clips_js}, ensure_ascii=False).replace('</', '<\\/')

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>VidClipDuplis — Review Results</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 20px; line-height: 1.5; }}
h1 {{ color: #e94560; margin-bottom: 5px; }}
.subtitle {{ color: #888; margin-bottom: 20px; }}
.summary {{ background: #16213e; padding: 15px 20px; border-radius: 8px; margin-bottom: 25px; display: flex; gap: 30px; flex-wrap: wrap; }}
.summary .stat {{ text-align: center; }}
.summary .stat .num {{ font-size: 24px; font-weight: bold; color: #e94560; }}
.summary .stat .label {{ font-size: 12px; color: #888; }}
.card {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 15px; border-left: 4px solid #0f3460; }}
.card.warning {{ border-left-color: #e9a045; }}
.card-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }}
.card-title {{ font-weight: bold; font-size: 16px; }}
.badge {{ padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
.badge-warn {{ background: #e9a045; color: #000; }}
.badge-savings {{ background: #0f3460; color: #e0e0e0; }}
.badge-clip {{ background: #533483; color: #e0e0e0; }}
.file-row {{ background: #0f3460; border-radius: 6px; padding: 12px 15px; margin-bottom: 8px; display: flex; align-items: center; gap: 12px; cursor: pointer; transition: background 0.15s; }}
.file-row:hover {{ background: #1a4a8a; }}
.file-row.selected-keep {{ border: 2px solid #4ecca3; background: #0f3460; }}
.file-row.selected-delete {{ border: 2px solid #e94560; opacity: 0.6; }}
.file-row.selected-skip {{ border: 2px solid #888; opacity: 0.5; }}
.file-info {{ flex: 1; min-width: 0; }}
.file-path {{ font-size: 13px; color: #ccc; word-break: break-all; }}
.file-name {{ font-weight: bold; color: #fff; }}
.file-meta {{ font-size: 12px; color: #888; margin-top: 3px; }}
.actions {{ display: flex; gap: 6px; flex-shrink: 0; flex-wrap: wrap; }}
.btn {{ padding: 5px 12px; border: none; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: bold; transition: all 0.15s; }}
.btn-keep {{ background: #4ecca3; color: #000; }}
.btn-delete {{ background: #e94560; color: #fff; }}
.btn-rename {{ background: #533483; color: #fff; }}
.btn-skip {{ background: #555; color: #fff; }}
.btn.active {{ box-shadow: 0 0 0 2px #fff; }}
.clip-row {{ background: #0f3460; border-radius: 6px; padding: 12px 15px; margin-bottom: 8px; display: flex; align-items: center; gap: 12px; }}
.clip-toggle {{ display: flex; gap: 6px; }}
.generate {{ position: sticky; bottom: 20px; background: #e94560; color: #fff; border: none; padding: 15px 30px; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; width: 100%; max-width: 500px; margin: 30px auto; display: block; box-shadow: 0 4px 15px rgba(233,69,96,0.4); }}
.generate:hover {{ background: #d63851; }}
.section-title {{ font-size: 20px; font-weight: bold; margin: 25px 0 15px; color: #4ecca3; }}
.rename-info {{ font-size: 11px; color: #b388ff; margin-top: 4px; font-style: italic; }}
.counter {{ position: fixed; top: 10px; right: 20px; background: #16213e; padding: 10px 15px; border-radius: 8px; font-size: 13px; z-index: 100; border: 1px solid #333; }}
</style>
</head>
<body>
<h1>VidClipDuplis — Review Results</h1>
<p class="subtitle">Click files to set actions. Then generate a custom PowerShell script.</p>

<div class="summary">
 <div class="stat"><div class="num" id="del-count">0</div><div class="label">Files to Delete</div></div>
 <div class="stat"><div class="num" id="rename-count">0</div><div class="label">Files to Rename</div></div>
 <div class="stat"><div class="num" id="skip-count">0</div><div class="label">Groups Skipped</div></div>
 <div class="stat"><div class="num">{format_size(total_savings)}</div><div class="label">Max Potential Savings</div></div>
</div>

<div class="counter">
 <span id="reviewed">0</span> / <span id="total-groups">0</span> groups reviewed
</div>

<div id="groups-container"></div>
<div id="clips-container"></div>

<button class="generate" onclick="generateScript()">Download Custom PowerShell Script</button>

<script>
const DATA = {data_json};

// State: for each group, which file index is "keep", and optional rename source
// state[groupId] = {{ keep: fileIdx, rename_from: fileIdx|null, action: 'decided'|'skipped' }}
const state = {{}};
const clipState = {{}};

function init() {{
  const gc = document.getElementById('groups-container');
  const cc = document.getElementById('clips-container');
  document.getElementById('total-groups').textContent = DATA.groups.length;

  if (DATA.groups.length > 0) {{
    gc.innerHTML = '<div class="section-title">Duplicate Groups (' + DATA.groups.length + ')</div>';
  }}

  DATA.groups.forEach((g, gi) => {{
    // Default: largest file is keep
    const keepIdx = g.files.findIndex(f => f.is_keep);
    state[gi] = {{ keep: keepIdx >= 0 ? keepIdx : 0, rename_from: null, action: 'decided' }};

    let warn = g.size_warning ? ' warning' : '';
    let warnBadge = g.size_warning ? ' <span class="badge badge-warn">SIZE MISMATCH</span>' : '';
    let html = '<div class="card' + warn + '" id="group-' + gi + '">';
    html += '<div class="card-header"><span class="card-title">Group ' + (gi+1) + ' (' + g.files.length + ' files)' + warnBadge + '</span>';
    html += '<span class="badge badge-savings">Save ' + g.savings_fmt + '</span></div>';

    g.files.forEach((f, fi) => {{
      html += '<div class="file-row" id="row-' + gi + '-' + fi + '">';
      html += '<div class="file-info">';
      html += '<div class="file-path"><span class="file-name">' + escHtml(f.filename) + '</span></div>';
      html += '<div class="file-path" style="font-size:11px;color:#666">' + escHtml(f.dir) + '</div>';
      html += '<div class="file-meta">' + f.size_fmt + ' &middot; ' + f.dur_fmt + '</div>';
      html += '<div class="rename-info" id="rename-info-' + gi + '-' + fi + '" style="display:none"></div>';
      html += '</div>';
      html += '<div class="actions">';
      html += '<button class="btn btn-keep" onclick="setKeep(' + gi + ',' + fi + ')">Keep</button>';
      html += '<button class="btn btn-delete" onclick="setDelete(' + gi + ',' + fi + ')">Delete</button>';
      html += '<button class="btn btn-rename" onclick="setRename(' + gi + ',' + fi + ')" title="Keep the largest file but rename it to this file\'s name">Use Name</button>';
      html += '</div></div>';
    }});

    html += '<div style="text-align:right;margin-top:8px"><button class="btn btn-skip" onclick="setSkip(' + gi + ')">Skip Group</button></div>';
    html += '</div>';
    gc.innerHTML += html;
  }});

  if (DATA.clips.length > 0) {{
    cc.innerHTML = '<div class="section-title">Redundant Clips (' + DATA.clips.length + ')</div>';
    DATA.clips.forEach((c, ci) => {{
      clipState[ci] = c.delete;
      let html = '<div class="clip-row" id="clip-' + ci + '">';
      html += '<div class="file-info">';
      html += '<div class="file-path"><span class="file-name">' + escHtml(c.child_name) + '</span> <span class="badge badge-clip">' + c.ratio + ' match</span></div>';
      html += '<div class="file-meta">' + c.child_size + ' &middot; ' + c.child_dur + ' &middot; Contained in: ' + c.parents.join(', ') + '</div>';
      html += '</div>';
      html += '<div class="clip-toggle">';
      html += '<button class="btn btn-delete active" id="clip-del-' + ci + '" onclick="toggleClip(' + ci + ',true)">Delete</button>';
      html += '<button class="btn btn-skip" id="clip-skip-' + ci + '" onclick="toggleClip(' + ci + ',false)">Skip</button>';
      html += '</div></div>';
      cc.innerHTML += html;
    }});
  }}

  updateAllRows();
  updateCounters();
}}

function escHtml(s) {{
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}}

function setKeep(gi, fi) {{
  state[gi].keep = fi;
  state[gi].rename_from = null;
  state[gi].action = 'decided';
  updateGroupRows(gi);
  updateCounters();
}}

function setDelete(gi, fi) {{
  // Set the OTHER file as keep
  const otherIdx = DATA.groups[gi].files.findIndex((_, i) => i !== fi);
  if (otherIdx >= 0) {{ state[gi].keep = otherIdx; }}
  state[gi].rename_from = null;
  state[gi].action = 'decided';
  updateGroupRows(gi);
  updateCounters();
}}

function setRename(gi, fi) {{
  // Keep the current "keep" file, but rename it to this file's name
  if (state[gi].keep === fi) {{
    // Can't rename to yourself — pick another keep first
    return;
  }}
  state[gi].rename_from = fi;
  state[gi].action = 'decided';
  updateGroupRows(gi);
  updateCounters();
}}

function setSkip(gi) {{
  state[gi].action = 'skipped';
  updateGroupRows(gi);
  updateCounters();
}}

function toggleClip(ci, del) {{
  clipState[ci] = del;
  document.getElementById('clip-del-' + ci).className = 'btn btn-delete' + (del ? ' active' : '');
  document.getElementById('clip-skip-' + ci).className = 'btn btn-skip' + (!del ? ' active' : '');
  updateCounters();
}}

function updateGroupRows(gi) {{
  const g = DATA.groups[gi];
  g.files.forEach((f, fi) => {{
    const row = document.getElementById('row-' + gi + '-' + fi);
    const renameInfo = document.getElementById('rename-info-' + gi + '-' + fi);
    row.className = 'file-row';
    renameInfo.style.display = 'none';

    if (state[gi].action === 'skipped') {{
      row.className = 'file-row selected-skip';
    }} else if (fi === state[gi].keep) {{
      row.className = 'file-row selected-keep';
      if (state[gi].rename_from !== null) {{
        const srcName = g.files[state[gi].rename_from].filename;
        renameInfo.textContent = 'Will be renamed to: ' + srcName;
        renameInfo.style.display = 'block';
      }}
    }} else {{
      row.className = 'file-row selected-delete';
    }}
  }});
}}

function updateAllRows() {{
  DATA.groups.forEach((_, gi) => updateGroupRows(gi));
}}

function updateCounters() {{
  let dels = 0, renames = 0, skips = 0;
  DATA.groups.forEach((g, gi) => {{
    if (state[gi].action === 'skipped') {{ skips++; return; }}
    dels += g.files.length - 1; // all except keep
    if (state[gi].rename_from !== null) renames++;
  }});
  DATA.clips.forEach((_, ci) => {{ if (clipState[ci]) dels++; }});
  document.getElementById('del-count').textContent = dels;
  document.getElementById('rename-count').textContent = renames;
  document.getElementById('skip-count').textContent = skips;
  document.getElementById('reviewed').textContent = Object.values(state).filter(s => s.action !== undefined).length;
}}

function generateScript() {{
  let lines = [];
  lines.push('# VidClipDuplis — Custom deletion/rename script');
  lines.push('# Generated from interactive review');
  lines.push('# Run: powershell -ExecutionPolicy Bypass -File custom_actions.ps1');
  lines.push('');
  lines.push('Write-Host "This script will delete and rename files as you chose." -ForegroundColor Yellow');
  lines.push('$null = Read-Host "Press Enter to continue or Ctrl+C to cancel"');
  lines.push('');

  DATA.groups.forEach((g, gi) => {{
    if (state[gi].action === 'skipped') return;
    const keepIdx = state[gi].keep;
    const keepFile = g.files[keepIdx];

    lines.push('# Group ' + (gi+1));

    // Delete all non-keep files
    g.files.forEach((f, fi) => {{
      if (fi !== keepIdx) {{
        const p = f.path.replace(/'/g, "''");
        lines.push("Remove-Item -LiteralPath '" + p + "' -Force");
      }}
    }});

    // Rename if requested
    if (state[gi].rename_from !== null) {{
      const srcName = g.files[state[gi].rename_from].filename;
      const keepDir = keepFile.dir;
      const newPath = keepDir + '\\\\' + srcName;
      const oldP = keepFile.path.replace(/'/g, "''");
      const newP = newPath.replace(/'/g, "''");
      lines.push("Rename-Item -LiteralPath '" + oldP + "' -NewName '" + srcName.replace(/'/g, "''") + "'");
    }}
    lines.push('');
  }});

  // Clips
  let hasClips = false;
  DATA.clips.forEach((c, ci) => {{
    if (clipState[ci]) {{
      if (!hasClips) {{ lines.push('# Redundant clips'); hasClips = true; }}
      const p = c.child_path.replace(/'/g, "''");
      lines.push("Remove-Item -LiteralPath '" + p + "' -Force");
    }}
  }});

  lines.push('');
  lines.push('Write-Host "Done!" -ForegroundColor Green');

  const blob = new Blob([lines.join('\\r\\n')], {{ type: 'text/plain' }});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'custom_actions.ps1';
  a.click();
  URL.revokeObjectURL(a.href);
}}

init();
</script>
</body>
</html>'''

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)


# ============================================================================
# FILE SCANNING
# ============================================================================

def find_media_files(root_dir):
    files, seen = [], set()
    for p in Path(root_dir).rglob('*'):
        if not p.is_file() or p.suffix.lower() not in MEDIA_EXTENSIONS: continue
        key = str(p).lower()
        if key in seen: continue
        seen.add(key)
        try:
            st = p.stat()
            files.append((str(p), st.st_size, st.st_mtime))
        except OSError: pass
    return files

# ============================================================================
# MAIN
# ============================================================================

def main():
    global SHUTDOWN_REQUESTED, _ARRAYS_TEMP_PATH, _debug_logger
    signal.signal(signal.SIGINT, signal_handler)

    if not os.path.exists(FPCALC_PATH):
        print(f"❌ CRITICAL: {FPCALC_PATH} not found.")
        print("   Download from: https://acoustid.org/chromaprint")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='VidClipDuplis (VCD) — Audio Duplicate & Clip Finder',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('directories', nargs='*', help='Directories to scan (space-separated, or use interactive prompt)')
    parser.add_argument('-w', '--workers', type=int, default=0, help='Extraction workers (default: 6)')
    parser.add_argument('-c', '--compare-workers', type=int, default=0, help='Comparison workers (default: 75%% of CPUs)')
    parser.add_argument('--clip-ratio', type=float, default=0, help='Clip threshold 0.0-1.0 (default: 0.75)')
    parser.add_argument('--dup-ratio', type=float, default=0, help='Duplicate threshold 0.0-1.0 (default: 0.95)')
    parser.add_argument('--intro-filter', type=float, default=-1, help='Min matched seconds (default: 30)')
    parser.add_argument('--clear-cache', action='store_true', help='Clear all cached data')
    parser.add_argument('--clear-comparisons', action='store_true', help='Clear comparisons only')
    parser.add_argument('--clear-failed', action='store_true', help='Clear failed file list')
    parser.add_argument('--timeout', type=int, default=600, help='Timeout per file (default: 600)')
    parser.add_argument('--no-prompt', action='store_true', help='Skip interactive setup')
    args = parser.parse_args()

    print("=" * 60)
    print("  VIDCLIPDUPLIS — Audio Duplicate & Clip Finder")
    print("  Chromaprint fingerprinting + NAS-safe + CPU-only")
    print("=" * 60)
    print()
    print("  How it works:")
    print("  This tool uses Chromaprint (fpcalc) to extract an audio")
    print("  fingerprint from every media file, then compares all")
    print("  fingerprints against each other to find duplicates and")
    print("  clips (shorter files whose audio is inside a longer file).")
    print("  It works regardless of video resolution, codec, or bitrate")
    print("  because it only looks at the audio track.")
    print()
    print("  Works on any CPU (Intel, AMD, ARM). No GPU needed.")

    # Resolve directories — support multiple via CLI args or interactive prompt
    if args.directories:
        root_dirs = [os.path.abspath(d) for d in args.directories]
    else:
        print("\n  Enter folder paths to scan.")
        print("  You can enter multiple paths separated by ; or , (comma/semicolon).")
        print("  Or enter them one at a time (press Enter on empty line when done).\n")
        
        root_dirs = []
        while True:
            prompt = "  Folder path" if not root_dirs else "  Another folder (or Enter to finish)"
            dir_input = input(f"{prompt}: ").strip().strip('"').strip("'")
            if not dir_input:
                if root_dirs:
                    break
                else:
                    print("  ❌ At least one folder is required.")
                    continue
            # Split by ; or , for multi-path entry
            for part in dir_input.replace(';', ',').split(','):
                part = part.strip().strip('"').strip("'")
                if part:
                    root_dirs.append(os.path.abspath(part))

    for d in root_dirs:
        if not os.path.isdir(d):
            print(f"❌ Invalid directory: {d}")
            sys.exit(1)

    # Configuration
    try: cpu_count = multiprocessing.cpu_count()
    except Exception: cpu_count = 4

    script_dir = _BASE_DIR
    cache_path = os.path.join(script_dir, '.audio_cache_v1a.db')
    cache = UnifiedCache(cache_path)

    # Handle CLI cache clearing first
    if args.clear_cache:
        cache.clear_fingerprints(); cache.clear_comparisons(); cache.clear_failed()
        print("🗑️  All caches cleared")
    if args.clear_comparisons:
        cache.clear_comparisons(); print("🗑️  Comparison cache cleared")
    if args.clear_failed:
        cache.clear_failed(); print("🗑️  Failed list cleared")

    has_cli_config = (args.workers > 0 or args.compare_workers > 0 or args.clip_ratio > 0
                      or args.dup_ratio > 0 or args.intro_filter >= 0 or args.no_prompt)

    if has_cli_config:
        config = Config(video_timeout=args.timeout)
        if args.workers > 0: config.max_workers = args.workers
        if args.compare_workers > 0: config.comparison_workers = args.compare_workers
        else: config.comparison_workers = max(4, int(cpu_count * 0.75))
        if args.clip_ratio > 0: config.clip_match_ratio = max(0.01, min(1.0, args.clip_ratio))
        if args.dup_ratio > 0: config.duplicate_match_ratio = max(0.01, min(1.0, args.dup_ratio))
        if args.intro_filter >= 0: config.intro_filter_seconds = args.intro_filter
    else:
        config = interactive_setup(cpu_count, cache)

    cache.validate_params(config)

    print(f"\n⚙️  Settings:")
    print(f"   Extraction workers:  {config.max_workers}")
    print(f"   Comparison workers:  {config.comparison_workers}")
    print(f"   Clip threshold:      {config.clip_match_ratio:.0%}")
    print(f"   Duplicate threshold: {config.duplicate_match_ratio:.0%}")
    print(f"   Intro filter:        {config.intro_filter_seconds}s")
    print(f"   Cache: {cache_path}")

    # Phase 0: Scan
    for d in root_dirs:
        print(f"\n📂 Scanning: {d}")
    media_files = []
    seen_paths = set()
    for d in root_dirs:
        for item in find_media_files(d):
            if item[0].lower() not in seen_paths:
                seen_paths.add(item[0].lower())
                media_files.append(item)
    if not media_files:
        print("   ❌ No media files found"); sys.exit(0)

    total_size = sum(s for _, s, _ in media_files)
    print(f"   Found {len(media_files)} files ({format_size(total_size)})")

    all_cached = cache.get_all_fingerprints()
    to_process, fingerprints, skipped_failed = [], {}, 0
    for path, size, mtime in media_files:
        if cache.is_failed(path): skipped_failed += 1; continue
        cached = all_cached.get(path)
        if cached and cached['file_size'] == size and abs(cached['mtime'] - mtime) < 1:
            fingerprints[path] = cached
        else:
            to_process.append((path, size, mtime))

    print(f"\n📦 Status:")
    print(f"   Cached: {len(fingerprints)}")
    print(f"   To process: {len(to_process)}")
    if skipped_failed: print(f"   Skipped failed: {skipped_failed}")

    # Set up results dir and debug log early
    results_dir = get_results_folder_path(script_dir, root_dirs)
    if to_process:
        os.makedirs(results_dir, exist_ok=True)
        _debug_logger = setup_debug_logger(results_dir)

    # Phase 1: Extraction
    if to_process and not SHUTDOWN_REQUESTED:
        print(f"\n🎵 Phase 1: Extracting audio fingerprints ({len(to_process)} files)...")
        print(f"   ⌨️  Press Ctrl+C once to stop gracefully, twice to force quit.")
        extraction_args = [(p, s, m, config.video_timeout) for p, s, m in to_process]
        pbar = tqdm(total=len(to_process), unit="file")
        failed_count = 0
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(extract_audio_fingerprint, item): item for item in extraction_args}
            for future in as_completed(futures):
                if SHUTDOWN_REQUESTED:
                    executor.shutdown(wait=False, cancel_futures=True); break
                try:
                    path, size, mtime, arr, duration, err, stderr_raw = future.result()
                    if arr is not None:
                        fingerprints[path] = {'file_size': size, 'mtime': mtime, 'duration': duration, 'fingerprint': arr}
                        cache.set_fingerprint(path, arr, size, mtime, duration)
                    else:
                        cache.mark_failed(path, err)
                        failed_count += 1
                        tqdm.write(f"   ❌ {err} — {os.path.basename(path)}")
                        if _debug_logger:
                            ext = os.path.splitext(path)[1].lower()
                            # Scrub any occurrence of the file path from error/stderr
                            # to keep the log truly anonymous (fpcalc and Python exceptions
                            # both embed the full path in their error strings)
                            safe_err = err
                            safe_stderr = stderr_raw.decode('utf-8', errors='ignore').strip()[:500] if stderr_raw else ''
                            for variant in (path, os.path.abspath(path), os.path.basename(path)):
                                safe_err = safe_err.replace(variant, '[PATH]')
                                safe_stderr = safe_stderr.replace(variant, '[PATH]')
                            _debug_logger.debug(f"FAIL | ext={ext} | size={size} | error={safe_err} | stderr={safe_stderr}")
                except Exception as e:
                    tqdm.write(f"   ❌ Worker error: {e}")
                pbar.update(1)
        pbar.close()
        print(f"   ✓ Extracted: {len(to_process) - failed_count}, Failed: {failed_count}")
        if failed_count > 0 and _debug_logger:
            print(f"   📋 Debug log: {os.path.join(results_dir, 'fpcalc_debug.log')}")

    if SHUTDOWN_REQUESTED:
        print("\n⚠️  Progress saved. Run again to continue."); return

    # Phase 2: Cache lookup
    hash_arrays = {p: fp['fingerprint'] for p, fp in fingerprints.items()
                   if fp.get('fingerprint') is not None and len(fp['fingerprint']) > 0}
    paths = sorted(hash_arrays.keys())
    total_pairs = len(paths) * (len(paths) - 1) // 2
    print(f"\n📊 Total fingerprints: {len(hash_arrays)}")
    print(f"   Total pairs: {total_pairs:,}")
    if len(paths) < 2:
        print("\nNot enough files to compare."); return

    all_pairs = [_normalize_pair(paths[i], paths[j]) for i in range(len(paths)) for j in range(i+1, len(paths))]

    print(f"\n🔍 Phase 2: Cache lookup...")
    t0 = time.time()
    cached_comparisons = cache.get_many_comparisons(all_pairs)
    print(f"   Found {len(cached_comparisons):,} cached ({time.time()-t0:.1f}s)")

    pairs_to_compute = [p for p in all_pairs if p not in cached_comparisons]
    cached_count = len(cached_comparisons)

    dup_matches, clip_matches = [], []
    for (p1, p2), (mr, lr, ms) in cached_comparisons.items():
        is_dup, is_clip = classify_comparison(mr, lr, ms, config)
        if is_dup: dup_matches.append((p1, p2, mr))
        elif is_clip: clip_matches.append((p1, p2, mr))

    print(f"   Cached: {cached_count:,}, To compute: {len(pairs_to_compute):,}")
    if dup_matches or clip_matches:
        print(f"   From cache: {len(dup_matches)} duplicates, {len(clip_matches)} clips")

    # Phase 3: Comparison
    new_comparisons = 0
    if pairs_to_compute and not SHUTDOWN_REQUESTED:
        print(f"\n⚡ Phase 3: Computing {len(pairs_to_compute):,} comparisons ({config.comparison_workers} workers)...")
        print(f"   ⌨️  Press Ctrl+C once to stop gracefully, twice to force quit.")
        _ARRAYS_TEMP_PATH = save_arrays_for_workers(hash_arrays)
        print(f"   Arrays temp file: {os.path.getsize(_ARRAYS_TEMP_PATH)/(1024*1024):.0f} MB")

        batches = [pairs_to_compute[i:i+config.comparison_batch_size] for i in range(0, len(pairs_to_compute), config.comparison_batch_size)]
        pbar = tqdm(total=len(pairs_to_compute), unit="pair")
        try:
            with ProcessPoolExecutor(max_workers=config.comparison_workers, initializer=init_worker, initargs=(_ARRAYS_TEMP_PATH,)) as executor:
                future_to_size = {}
                for batch in batches:
                    if SHUTDOWN_REQUESTED: break
                    future_to_size[executor.submit(compare_batch, batch, config)] = len(batch)

                batch_buf = []
                for future in as_completed(future_to_size):
                    if SHUTDOWN_REQUESTED:
                        for f in future_to_size: f.cancel()
                        executor.shutdown(wait=False, cancel_futures=True); break
                    try:
                        res = future.result()
                        batch_buf.extend(res)
                        for np1, np2, mr, lr, ms in res:
                            new_comparisons += 1
                            is_dup, is_clip = classify_comparison(mr, lr, ms, config)
                            if is_dup:
                                dup_matches.append((np1, np2, mr))
                                tqdm.write(f"   ✅ DUP ({mr*100:.1f}%): {os.path.basename(np1)} ↔ {os.path.basename(np2)}")
                            elif is_clip:
                                clip_matches.append((np1, np2, mr))
                                tqdm.write(f"   📎 CLIP ({mr*100:.1f}%): {os.path.basename(np1)} ↔ {os.path.basename(np2)}")
                        pbar.update(future_to_size[future])
                        if len(batch_buf) >= 5000:
                            cache.batch_set_comparisons(batch_buf); batch_buf = []
                    except Exception as e:
                        tqdm.write(f"   ⚠️  Batch error: {type(e).__name__}: {str(e)[:80]}")
                        pbar.update(future_to_size[future])
                if batch_buf: cache.batch_set_comparisons(batch_buf)
            pbar.close()
            cache.wal_checkpoint()
        finally:
            cleanup_arrays_file()

    if SHUTDOWN_REQUESTED:
        print("\n⚠️  Progress saved. Run again to continue."); return

    print(f"\n✓ {len(dup_matches)+len(clip_matches)} matches ({len(dup_matches)} dups, {len(clip_matches)} clips)")
    print(f"   ({cached_count:,} cached, {new_comparisons:,} computed)")

    # Phase 4: Safe grouping
    print("\n📋 Phase 4: Grouping results...")
    uf = UnionFind()
    for p1, p2, _ in dup_matches: uf.union(p1, p2)

    dup_groups_raw = defaultdict(list)
    for path in paths: uf.find(path); dup_groups_raw[uf.find(path)].append(path)

    dup_groups, keep_set, dup_delete_set = [], set(), set()
    for gp in dup_groups_raw.values():
        if len(gp) < 2: keep_set.add(gp[0]); continue
        sg = sorted(gp, key=lambda p: (fingerprints[p]['file_size'], fingerprints[p].get('duration',0)), reverse=True)
        keep_set.add(sg[0]); dup_delete_set.update(sg[1:])
        sizes = [fingerprints[p]['file_size'] for p in sg]
        dup_groups.append({
            'recommend_keep': sg[0], 'recommend_delete': sg[1:],
            'potential_savings': sum(fingerprints[p]['file_size'] for p in sg[1:]),
            'size_warning': (max(sizes) / max(min(sizes), 1)) > 3.0,
            'videos': [{'path': p, 'size': fingerprints[p]['file_size'], 'duration': fingerprints[p].get('duration',0)} for p in sg],
        })
    dup_groups.sort(key=lambda g: g['potential_savings'], reverse=True)

    clip_children = defaultdict(list)
    for p1, p2, mr in clip_matches:
        l1 = len(hash_arrays[p1]) if p1 in hash_arrays else 0
        l2 = len(hash_arrays[p2]) if p2 in hash_arrays else 0
        child, parent = (p1, p2) if l1 <= l2 else (p2, p1)
        clip_children[child].append((parent, mr))

    clip_deletions = []
    for child, parents in clip_children.items():
        if child in dup_delete_set: continue
        if child in keep_set and any(child == g['recommend_keep'] for g in dup_groups): continue
        kept = [(par, r) for par, r in parents if par in keep_set]
        if kept:
            clip_deletions.append((child, [p for p,_ in kept], max(r for _,r in kept)))
    clip_deletions.sort(key=lambda x: fingerprints[x[0]]['file_size'], reverse=True)

    # Output
    display_root = root_dirs[0]
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print("\n   ℹ️  Audio-only matching. Videos with the same background")
    print("   music will match. Review results before deleting.")

    if not dup_groups and not clip_deletions:
        print("\n✨ No duplicates or clips found!"); return

    if dup_groups:
        print(f"\n📋 EXACT DUPLICATES: {len(dup_groups)} groups")
        print(f"   Delete: {sum(len(g['recommend_delete']) for g in dup_groups)} files")
        print(f"   Savings: {format_size(sum(g['potential_savings'] for g in dup_groups))}")
        for i, g in enumerate(dup_groups[:15], 1):
            warn = " ⚠️ SIZE MISMATCH" if g.get('size_warning') else ""
            print(f"\n{'─'*50}")
            print(f"Dup Group {i} ({len(g['videos'])} files, save {format_size(g['potential_savings'])}){warn}")
            if g.get('size_warning'):
                print(f"   ⚠️  Files differ >3x in size — likely shared audio!")
            for v in g['videos']:
                marker = "✓ KEEP  " if v['path'] == g['recommend_keep'] else "✗ DELETE"
                print(f"   {marker} {os.path.relpath(v['path'], display_root)}")
                print(f"            {format_size(v['size'])}, {format_duration(v['duration']) if v['duration'] else '?'}")
        if len(dup_groups) > 15: print(f"\n   ... and {len(dup_groups)-15} more groups")

    if clip_deletions:
        clip_savings = sum(fingerprints[c]['file_size'] for c,_,_ in clip_deletions)
        print(f"\n📎 REDUNDANT CLIPS: {len(clip_deletions)} files, {format_size(clip_savings)}")
        for i, (child, parents, ratio) in enumerate(clip_deletions[:15], 1):
            pnames = ", ".join(os.path.basename(p) for p in parents[:3])
            if len(parents) > 3: pnames += f" (+{len(parents)-3})"
            print(f"\n{'─'*50}")
            print(f"Clip {i} ({ratio*100:.0f}% match)")
            print(f"   ✗ DELETE  {os.path.relpath(child, display_root)}")
            print(f"             {format_size(fingerprints[child]['file_size'])}, {format_duration(fingerprints[child].get('duration',0))}")
            print(f"   ↳ Contained in: {pnames}")
        if len(clip_deletions) > 15: print(f"\n   ... and {len(clip_deletions)-15} more clips")

    # Save results
    os.makedirs(results_dir, exist_ok=True)
    total_delete = sum(len(g['recommend_delete']) for g in dup_groups) + len(clip_deletions)
    total_savings = sum(g['potential_savings'] for g in dup_groups) + sum(fingerprints[c]['file_size'] for c,_,_ in clip_deletions)

    # JSON
    report = {
        'scan_info': {'directories': root_dirs, 'total_files': len(media_files), 'fingerprinted': len(fingerprints),
                      'failed': cache.get_failed_count(), 'scan_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                      'config': {k: getattr(config, k) for k in ['clip_match_ratio','duplicate_match_ratio','intro_filter_seconds','match_threshold','chunk_size']}},
        'summary': {'duplicate_groups': len(dup_groups), 'redundant_clips': len(clip_deletions),
                    'files_to_delete': total_delete, 'potential_savings': format_size(total_savings)},
        'duplicate_groups': [{'group_id': i, 'file_count': len(g['videos']), 'potential_savings': format_size(g['potential_savings']),
            'size_warning': g.get('size_warning', False), 'recommend_keep': os.path.relpath(g['recommend_keep'], display_root),
            'recommend_delete': [os.path.relpath(p, display_root) for p in g['recommend_delete']],
            'files': [{'path': os.path.relpath(v['path'], display_root), 'size': format_size(v['size']),
                       'duration': format_duration(v['duration']) if v['duration'] else '?',
                       'action': 'KEEP' if v['path']==g['recommend_keep'] else 'DELETE'} for v in g['videos']]}
            for i, g in enumerate(dup_groups, 1)],
        'redundant_clips': [{'clip': os.path.relpath(c, display_root), 'clip_size': format_size(fingerprints[c]['file_size']),
            'clip_duration': format_duration(fingerprints[c].get('duration',0)), 'match_ratio': f"{r*100:.1f}%",
            'contained_in': [os.path.relpath(p, display_root) for p in ps]} for c, ps, r in clip_deletions],
    }
    with open(os.path.join(results_dir, 'duplicate_report.json'), 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # .bat
    with open(os.path.join(results_dir, 'delete_duplicates.bat'), 'w', encoding='utf-8') as f:
        f.write('@echo off\nchcp 65001 > nul\n')
        f.write(f'REM {total_delete} files, {format_size(total_savings)} potential savings\n')
        f.write('echo WARNING: This will PERMANENTLY delete files!\npause\n\n')
        for g in dup_groups:
            f.write(f'REM Keep: {os.path.relpath(g["recommend_keep"], display_root)}\n')
            if g.get('size_warning'):
                f.write('REM SIZE MISMATCH — uncomment only if verified:\n')
                for p in g['recommend_delete']: f.write(f'REM del "{_bat_safe_path(p)}"\n')
            else:
                for p in g['recommend_delete']: f.write(f'del "{_bat_safe_path(p)}"\n')
            f.write('\n')
        for child, parents, ratio in clip_deletions:
            f.write(f'REM Clip ({ratio*100:.0f}%% match) of {os.path.basename(parents[0])}\n')
            f.write(f'del "{_bat_safe_path(child)}"\n\n')
        f.write('echo Done!\npause\n')

    # .ps1
    with open(os.path.join(results_dir, 'delete_duplicates.ps1'), 'w', encoding='utf-8') as f:
        f.write(f'# {total_delete} files, {format_size(total_savings)} savings\n')
        f.write('# Run: powershell -ExecutionPolicy Bypass -File delete_duplicates.ps1\n')
        f.write('Write-Host "WARNING: This will PERMANENTLY delete files!" -ForegroundColor Red\n')
        f.write('$null = Read-Host "Press Enter or Ctrl+C to cancel"\n\n')
        for g in dup_groups:
            if g.get('size_warning'):
                f.write('# SIZE MISMATCH — uncomment only if verified:\n')
                for p in g['recommend_delete']:
                    f.write(f"# Remove-Item -LiteralPath '{os.path.abspath(p).replace(chr(39), chr(39)+chr(39))}' -Force\n")
            else:
                for p in g['recommend_delete']:
                    f.write(f"Remove-Item -LiteralPath '{os.path.abspath(p).replace(chr(39), chr(39)+chr(39))}' -Force\n")
            f.write('\n')
        for child, parents, ratio in clip_deletions:
            f.write(f"# Clip ({ratio*100:.0f}% match) of {os.path.basename(parents[0])}\n")
            f.write(f"Remove-Item -LiteralPath '{os.path.abspath(child).replace(chr(39), chr(39)+chr(39))}' -Force\n\n")
        f.write('Write-Host "Done!" -ForegroundColor Green\n')

    # Interactive HTML report
    _generate_html_report(results_dir, dup_groups, clip_deletions, fingerprints, display_root, total_delete, total_savings)

    print(f"\n📄 Results saved to: {results_dir}")
    print("   - review_results.html     (interactive — open in browser)")
    print("   - duplicate_report.json")
    print("   - delete_duplicates.bat   (cmd.exe)")
    print("   - delete_duplicates.ps1   (PowerShell — handles long paths)")
    if _debug_logger and _debug_logger.handlers:
        print("   - fpcalc_debug.log        (anonymized failure details)")
    print(f"\n   Total: {total_delete} files, {format_size(total_savings)} savings")
    print("\n✨ Done! Open review_results.html in your browser to review.")

if __name__ == '__main__':
    main()
