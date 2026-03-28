#!/usr/bin/env python3
r"""
VidClipDuplis (VCD)
===================
Audio Duplicate & Clip Finder using Chromaprint (fpcalc).
NAS-safe, file-based IPC, CPU-only.

THIS SCRIPT NEVER DELETES FILES. It generates a report, a .bat file,
and a .ps1 (PowerShell) script for you to review and run if you choose.

AUDIO-ONLY MATCHING: This compares audio tracks, not video frames.
Different videos with the same background music will match.
Best for: movies, TV, music, lectures. Review results for meme/TikTok folders.

Features:
- Content-based cache keys (quick_hash) that survive file moves/renames
- Byte-for-byte identical files detected instantly (no fingerprint comparison needed)
- Multi-folder support (unlimited directories, comma/semicolon separated)
- Interactive setup with cache management and CLI tips
- Anonymous debug log for failures (no filenames logged)
- Ctrl+C graceful shutdown with progress saving
- Windows 8.3 short path + hardlink fallback for unsafe filenames
  (Chinese, Japanese, emoji, brackets, special characters)

Requirements: fpcalc.exe in script directory (from acoustid.org/chromaprint)
              pip install numpy tqdm
"""

import os
import sys
import json
import hashlib
import subprocess
import argparse
import time
import threading
import signal
import sqlite3
import tempfile
import logging
import atexit
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
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
FFMPEG_PATH: Optional[str] = None  # Detected at startup if available
GLOBAL_ARRAYS: Dict[str, np.ndarray] = {}
FPCALC_MAX_STDOUT_BYTES = 50 * 1024 * 1024
_ARRAYS_TEMP_PATH: Optional[str] = None
_debug_logger: Optional[logging.Logger] = None
_INSTANCE_LOCK_FD: Optional[int] = None

def _find_ffmpeg() -> Optional[str]:
    """Find ffmpeg.exe — check script directory first, then PATH."""
    local = os.path.join(_BASE_DIR, 'ffmpeg.exe')
    if os.path.exists(local):
        return local
    import shutil
    found = shutil.which('ffmpeg')
    return found

def acquire_instance_lock() -> bool:
    """
    Acquire an exclusive lock to prevent multiple instances from running.
    Returns True if lock acquired, False if another instance is running.
    """
    global _INSTANCE_LOCK_FD
    lock_path = os.path.join(_BASE_DIR, '.vcd_instance.lock')
    
    try:
        # Open/create lock file
        _INSTANCE_LOCK_FD = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        
        if os.name == 'nt':
            # Windows: use msvcrt for exclusive lock
            import msvcrt
            msvcrt.locking(_INSTANCE_LOCK_FD, msvcrt.LK_NBLCK, 1)
        else:
            # Unix: use fcntl
            import fcntl
            fcntl.flock(_INSTANCE_LOCK_FD, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        return True
    except (OSError, IOError):
        if _INSTANCE_LOCK_FD is not None:
            try:
                os.close(_INSTANCE_LOCK_FD)
            except OSError:
                pass
            _INSTANCE_LOCK_FD = None
        return False

def release_instance_lock():
    """Release the instance lock so another run can start cleanly.
    Explicitly unlocks before closing to avoid undefined msvcrt behavior."""
    global _INSTANCE_LOCK_FD
    if _INSTANCE_LOCK_FD is not None:
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(_INSTANCE_LOCK_FD, msvcrt.LK_UNLCK, 1)
            os.close(_INSTANCE_LOCK_FD)
        except OSError:
            pass
        _INSTANCE_LOCK_FD = None
        # Clean up the physical lockfile
        try:
            lock_path = os.path.join(_BASE_DIR, '.vcd_instance.lock')
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except OSError:
            pass

atexit.register(release_instance_lock)

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
    import signal
    
    # Workers must ignore Ctrl+C — only main process handles graceful shutdown
    # Without this, Windows broadcasts SIGINT to all processes and workers crash
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    for attempt in range(10):
        try:
            with open(arrays_file, 'rb') as f:
                GLOBAL_ARRAYS = pickle.load(f)
            return
        except (PermissionError, FileNotFoundError):
            if attempt < 9:
                time.sleep(0.5)
            else:
                raise RuntimeError("Failed to load hash arrays: file locked or not found")

def save_arrays_for_workers(hash_arrays: Dict[str, np.ndarray]) -> str:
    """Serialize hash_arrays to a temp file. Returns the filepath."""
    global _ARRAYS_TEMP_PATH
    import pickle
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.pkl', prefix='audiocache_arrays_', delete=False) as f:
        pickle.dump(hash_arrays, f, protocol=pickle.HIGHEST_PROTOCOL)
        _ARRAYS_TEMP_PATH = f.name
        return f.name

def cleanup_arrays_file():
    """Remove the temp arrays file. Registered with atexit for crash safety.
    
    Guard: Only the main process may delete the temp file. On Windows,
    multiprocessing uses 'spawn', so every child worker re-imports this
    module and re-registers atexit hooks. Without this guard, the first
    worker to exit could delete the file while others still need it.
    """
    global _ARRAYS_TEMP_PATH
    if multiprocessing.current_process().name != 'MainProcess':
        return
    if _ARRAYS_TEMP_PATH and os.path.exists(_ARRAYS_TEMP_PATH):
        try:
            os.remove(_ARRAYS_TEMP_PATH)
        except OSError:
            pass
        _ARRAYS_TEMP_PATH = None

# Register cleanup for crash/kill scenarios
atexit.register(cleanup_arrays_file)

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
    
    # Only main process handles UI/cleanup; workers die silently
    if multiprocessing.current_process().name != 'MainProcess':
        return
    
    CTRL_C_COUNT += 1
    if CTRL_C_COUNT >= 2:
        print("\n\n🛑 Force quitting — killing active processes...")
        with PROCESS_LOCK:
            for proc in ACTIVE_PROCESSES:
                _kill_proc(proc)
        
        # Kill comparison workers so they don't linger in Task Manager
        for child in multiprocessing.active_children():
            child.kill()
        
        # Manually trigger cleanup that os._exit would skip
        cleanup_arrays_file()
        release_instance_lock()
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

def _bat_safe_path(path: str) -> str:
    """Absolute path with % escaped to %% for cmd.exe."""
    return os.path.abspath(path).replace('%', '%%')

def _safe_relpath(path: str, start: str) -> str:
    """os.path.relpath that won't crash on different drives (Windows ValueError).
    Falls back to absolute path when drives differ."""
    try:
        return os.path.relpath(path, start)
    except ValueError:
        return os.path.abspath(path)

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
        # Iterative find with path compression (avoids recursion limit)
        root = i
        while self.parent.get(root, root) != root:
            root = self.parent[root]
        # Path compression — flatten chain so future lookups are O(1)
        curr = i
        while curr != root:
            nxt = self.parent.get(curr)
            if nxt is None:
                break
            self.parent[curr] = root
            curr = nxt
        return root
    def union(self, i, j):
        ri, rj = self.find(i), self.find(j)
        if ri != rj:
            rki, rkj = self.rank.get(ri, 0), self.rank.get(rj, 0)
            if rki < rkj: ri, rj = rj, ri
            self.parent[rj] = ri
            if rki == rkj: self.rank[ri] = rki + 1

# ============================================================================
# CONTENT-BASED HASHING
# ============================================================================

def _long_path_safe(path: str) -> str:
    """Add Windows extended-length path prefix to handle paths exceeding MAX_PATH (260 chars)."""
    if os.name == 'nt' and not path.startswith('\\\\?\\'):
        abs_path = os.path.abspath(path)
        if abs_path.startswith('\\\\'):
            # Network share (UNC path): \\server\share → \\?\UNC\server\share
            return '\\\\?\\UNC\\' + abs_path[2:]
        else:
            # Local drive letter: C:\path → \\?\C:\path
            return '\\\\?\\' + abs_path
    return path

def get_quick_hash(filepath: str, chunk_size: int = 65536) -> Tuple[str, str]:
    """
    Compute a content-based hash that survives file moves and renames.
    
    Reads first 64KB + middle 64KB + last 64KB + file_size → MD5 truncated to 16 chars.
    The middle chunk defends against CBR video files (dashcams, GoPros, security cameras)
    that may share identical headers, file size, and even trailing data.
    Only 192KB max read — fast even on Gigabit NAS.
    
    NOTE: Changing this algorithm invalidates all previously cached fingerprints.
    They will be re-extracted on next run automatically (old keys become orphaned).
    
    Returns (hash, "") on success, ("", error_reason) on failure.
    """
    try:
        # Use extended-length path on Windows to handle >260 char paths
        safe_path = _long_path_safe(filepath)
        file_size = os.path.getsize(safe_path)
        hasher = hashlib.md5()
        with open(safe_path, 'rb') as f:
            # Read first chunk
            first_chunk = f.read(chunk_size)
            hasher.update(first_chunk)
            
            # Read middle chunk if file is large enough (3+ chunks)
            if file_size >= chunk_size * 3:
                f.seek(file_size // 2)
                mid_chunk = f.read(chunk_size)
                hasher.update(mid_chunk)
            
            # Read last chunk if file is large enough
            if file_size >= chunk_size * 2:
                f.seek(-chunk_size, 2)  # 2 = SEEK_END
                last_chunk = f.read(chunk_size)
                hasher.update(last_chunk)
            elif file_size > chunk_size:
                # File is between 64KB and 128KB — read whatever's left
                f.seek(chunk_size)
                remaining = f.read()
                hasher.update(remaining)
            
            # Include file size in hash to differentiate truncated versions
            hasher.update(str(file_size).encode('utf-8'))
        
        return hasher.hexdigest()[:16], ""
    except (OSError, IOError) as e:
        # File might be locked, deleted, or on disconnected network share
        return "", str(e)

# ============================================================================
# INTERACTIVE SETUP
# ============================================================================

def prompt_with_default(prompt: str, default: str) -> str:
    try:
        user_input = input(f"   {prompt} [{default}]: ").strip()
        return user_input if user_input else default
    except (EOFError, KeyboardInterrupt):
        return default

def interactive_setup(cpu_count: int, cache: 'UnifiedCache') -> Tuple[Config, bool]:
    """Interactive configuration with settings preview, cache management, and CLI tips.
    Returns (Config, cleanup_requested)."""
    comp_default = max(1, min(cpu_count - 1, int(cpu_count * 0.75)))
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
    print(f"      on every chunk pair). This WILL pin your CPU at near 100%.")
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

    print(f"\n  Cache status (content-based keys — survives moves/renames):")
    fp_count = cache.get_fingerprint_count()
    cmp_count = cache.get_comparison_count()
    failed_count = cache.get_failed_count()
    dismissed_count = cache.get_dismissed_count()
    print(f"    Fingerprints cached: {fp_count:,}")
    print(f"    Comparisons cached:  {cmp_count:,}")
    print(f"    Failed files:        {failed_count:,}")
    print(f"    Dismissed groups:    {dismissed_count:,} pairs")
    
    print(f"\n  Cache management:")
    print(f"    1. Clear failed list     (retry files that previously failed)")
    print(f"    2. Clear comparisons     (re-compare with new thresholds)")
    print(f"    3. Clear everything      (start completely fresh)")
    print(f"    4. No changes            (keep cache as-is)")
    print(f"    5. Cleanup orphans       (remove data for deleted/moved files, shrink DB)")
    print(f"       DB size: {format_size(cache.get_db_size())}")
    print(f"    6. Clear dismissed       (re-show previously skipped groups)")
    
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
        cache.clear_dismissed()
        print("  🗑️  All caches cleared")
    elif cache_choice == '5':
        print("  🔍 Cleanup runs after scanning (needs file list to know what's orphaned)")
    elif cache_choice == '6':
        cache.clear_dismissed()
        print("  🗑️  Dismissed groups cleared — all groups will appear in next report")

    _cleanup_requested = cache_choice == '5'

    print(f"\n  CLI tip: you can skip this menu with command-line flags:")
    print(f"    --no-prompt              Use defaults, skip interactive setup")
    print(f"    --clear-failed           Retry previously failed files")
    print(f"    --clear-comparisons      Re-compare with different thresholds")
    print(f"    --clear-cache            Wipe everything and start fresh")
    print(f"    --cleanup-cache          Remove data for deleted/moved files, shrink DB")
    print(f"    --clear-dismissed        Re-show previously skipped groups")
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
        ), _cleanup_requested
    else:
        return Config(comparison_workers=comp_default, video_timeout=600), _cleanup_requested

# ============================================================================
# UNIFIED SQLITE CACHE
# ============================================================================

class UnifiedCache:
    """
    SQLite cache for fingerprints, comparisons, and failed files.
    Uses content-based keys (quick_hash) that survive file moves/renames.
    
    IMPORTANT: The disk scanner (os.walk) is the source of truth for which
    files exist. The database only caches fingerprints and comparisons.
    Multiple files with the same content_key are handled by the caller.
    """

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
        
        # Fingerprints: keyed by content_key (quick_hash)
        # current_path is just for display; the disk scanner is the source of truth
        conn.execute("""CREATE TABLE IF NOT EXISTS fingerprints (
            content_key TEXT PRIMARY KEY,
            current_path TEXT,
            file_size INTEGER,
            fingerprint BLOB,
            duration REAL,
            processed_at TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fp_path ON fingerprints(current_path)")
        
        # Comparisons: keyed by (key1, key2) - both are content_keys
        conn.execute("""CREATE TABLE IF NOT EXISTS comparisons (
            key1 TEXT NOT NULL,
            key2 TEXT NOT NULL,
            match_ratio REAL NOT NULL,
            length_ratio REAL NOT NULL,
            matched_seconds REAL NOT NULL,
            PRIMARY KEY (key1, key2))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cmp_k1 ON comparisons(key1)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cmp_k2 ON comparisons(key2)")
        
        # Failed files: keyed by content_key
        conn.execute("""CREATE TABLE IF NOT EXISTS failed_files (
            content_key TEXT PRIMARY KEY,
            last_path TEXT,
            reason TEXT)""")
        
        # Cache parameters for auto-invalidation on algorithm changes
        conn.execute("CREATE TABLE IF NOT EXISTS cache_params (key TEXT PRIMARY KEY, value TEXT)")
        
        # Dismissed pairs: content_key pairs the user explicitly skipped in the HTML report
        # Persists across runs so previously-reviewed groups stay hidden
        conn.execute("""CREATE TABLE IF NOT EXISTS dismissed_pairs (
            key1 TEXT NOT NULL,
            key2 TEXT NOT NULL,
            PRIMARY KEY (key1, key2))""")
        conn.commit()

    def validate_params(self, config: Config) -> bool:
        conn = self._get_conn()
        # hash_version tracks quick_hash algorithm changes — if it changes, ALL
        # cached fingerprints are invalid (different keys for the same file).
        # comparison params track the comparison algorithm — if they change, only
        # comparisons need clearing (fingerprints are still valid).
        HASH_VERSION = '2'  # Bump when quick_hash algorithm changes (e.g., added middle chunk)
        
        compare_params = {'chunk_size': str(config.chunk_size), 'match_threshold': str(config.match_threshold)}
        all_params = {**compare_params, 'hash_version': HASH_VERSION}
        
        try:
            stored = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM cache_params")}
        except sqlite3.Error:
            stored = {}
        if not stored:
            conn.executemany("INSERT OR REPLACE INTO cache_params (key, value) VALUES (?, ?)", list(all_params.items()))
            conn.commit()
            return True
        
        # Check if hash algorithm changed — must clear EVERYTHING
        if stored.get('hash_version') != HASH_VERSION:
            old_ver = stored.get('hash_version', '1')
            print(f"   Warning: Hash algorithm changed (v{old_ver} → v{HASH_VERSION})")
            print(f"   Auto-clearing all caches (fingerprints + comparisons)")
            conn.execute("DELETE FROM fingerprints")
            conn.execute("DROP TABLE IF EXISTS comparisons")
            conn.execute("""CREATE TABLE comparisons (
                key1 TEXT NOT NULL, key2 TEXT NOT NULL,
                match_ratio REAL NOT NULL, length_ratio REAL NOT NULL,
                matched_seconds REAL NOT NULL, PRIMARY KEY (key1, key2))""")
            conn.execute("CREATE INDEX idx_cmp_k1 ON comparisons(key1)")
            conn.execute("CREATE INDEX idx_cmp_k2 ON comparisons(key2)")
            conn.execute("DELETE FROM failed_files")
            conn.executemany("INSERT OR REPLACE INTO cache_params (key, value) VALUES (?, ?)", list(all_params.items()))
            conn.commit()
            return False
        
        # Check if comparison params changed — only clear comparisons
        mismatched = [f"{k}: {stored.get(k,'?')} -> {v}" for k, v in compare_params.items() if stored.get(k) != v]
        if mismatched:
            print(f"   Warning: Algorithm params changed: {', '.join(mismatched)}")
            print(f"   Auto-clearing comparison cache")
            conn.execute("DROP TABLE IF EXISTS comparisons")
            conn.execute("""CREATE TABLE comparisons (
                key1 TEXT NOT NULL, key2 TEXT NOT NULL,
                match_ratio REAL NOT NULL, length_ratio REAL NOT NULL,
                matched_seconds REAL NOT NULL, PRIMARY KEY (key1, key2))""")
            conn.execute("CREATE INDEX idx_cmp_k1 ON comparisons(key1)")
            conn.execute("CREATE INDEX idx_cmp_k2 ON comparisons(key2)")
            conn.executemany("INSERT OR REPLACE INTO cache_params (key, value) VALUES (?, ?)", list(all_params.items()))
            conn.commit()
            return False
        return True

    # ==================== FINGERPRINT METHODS ====================
    
    def get_fingerprint(self, content_key: str) -> Optional[Dict]:
        """Look up fingerprint by content_key."""
        if not content_key:
            return None
        try:
            row = self._get_conn().execute(
                "SELECT content_key, current_path, file_size, fingerprint, duration FROM fingerprints WHERE content_key=?",
                (content_key,)
            ).fetchone()
            if row:
                arr = np.frombuffer(row[3], dtype=np.uint32).copy() if row[3] else None
                return {
                    'content_key': row[0],
                    'current_path': row[1],
                    'file_size': row[2],
                    'fingerprint': arr,
                    'duration': row[4]
                }
        except sqlite3.Error as e:
            print(f"Warning: Database error reading fingerprint: {e}")
        return None

    def get_all_fingerprints(self) -> Dict[str, Dict]:
        """Load all fingerprints keyed by content_key."""
        fps = {}
        try:
            for row in self._get_conn().execute(
                "SELECT content_key, current_path, file_size, fingerprint, duration FROM fingerprints"
            ):
                arr = np.frombuffer(row[3], dtype=np.uint32).copy() if row[3] else None
                fps[row[0]] = {
                    'content_key': row[0],
                    'current_path': row[1],
                    'file_size': row[2],
                    'fingerprint': arr,
                    'duration': row[4]
                }
        except sqlite3.Error as e:
            print(f"Warning: Could not load fingerprints: {e}")
        return fps

    def set_fingerprint(self, content_key: str, path: str, arr: Optional[np.ndarray], 
                        size: int, duration: float):
        """Store or update a fingerprint."""
        blob = arr.tobytes() if arr is not None else None
        try:
            conn = self._get_conn()
            conn.execute(
                """INSERT OR REPLACE INTO fingerprints 
                   (content_key, current_path, file_size, fingerprint, duration, processed_at) 
                   VALUES (?,?,?,?,?,datetime('now'))""",
                (content_key, path, size, blob, duration)
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"Warning: Could not save fingerprint: {e}")

    def update_path(self, content_key: str, new_path: str):
        """Update the current_path for an existing fingerprint (file was moved/renamed)."""
        try:
            conn = self._get_conn()
            conn.execute(
                "UPDATE fingerprints SET current_path=? WHERE content_key=?",
                (new_path, content_key)
            )
            conn.commit()
        except sqlite3.Error:
            pass

    # ==================== COMPARISON METHODS ====================
    
    def _normalize_key_pair(self, k1: str, k2: str) -> Tuple[str, str]:
        """Ensure consistent ordering for comparison lookup."""
        return (k1, k2) if k1 <= k2 else (k2, k1)

    def get_many_comparisons(self, pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], Tuple[float, float, float]]:
        """
        Batch lookup comparisons by content_key pairs.
        Returns dict mapping (key1, key2) -> (match_ratio, length_ratio, matched_seconds)
        """
        results = {}
        if not pairs:
            return results
        
        conn = self._get_conn()
        try:
            # Process in chunks to avoid memory issues
            for i in range(0, len(pairs), 100000):
                # DROP + CREATE is faster than DELETE FROM for temp tables
                conn.execute("DROP TABLE IF EXISTS temp_pairs")
                conn.execute("CREATE TEMP TABLE temp_pairs (key1 TEXT, key2 TEXT)")
                
                # Normalize all pairs before lookup
                normalized = [self._normalize_key_pair(k1, k2) for k1, k2 in pairs[i:i+100000]]
                conn.executemany("INSERT INTO temp_pairs VALUES (?,?)", normalized)
                
                for row in conn.execute("""
                    SELECT c.key1, c.key2, c.match_ratio, c.length_ratio, c.matched_seconds 
                    FROM temp_pairs t 
                    INNER JOIN comparisons c ON t.key1=c.key1 AND t.key2=c.key2
                """):
                    results[(row[0], row[1])] = (row[2], row[3], row[4])
            
            conn.execute("DROP TABLE IF EXISTS temp_pairs")
        except sqlite3.Error as e:
            print(f"Warning: Batch lookup error: {e}")
        
        return results

    def batch_set_comparisons(self, results: List[Tuple[str, str, float, float, float]]):
        """
        Batch insert comparison results.
        Each tuple is (key1, key2, match_ratio, length_ratio, matched_seconds).
        Keys will be normalized internally.
        """
        if not results:
            return
        try:
            conn = self._get_conn()
            # Normalize pairs before insert
            normalized = [
                (*self._normalize_key_pair(k1, k2), mr, lr, ms) 
                for k1, k2, mr, lr, ms in results
            ]
            conn.executemany(
                "INSERT OR REPLACE INTO comparisons (key1, key2, match_ratio, length_ratio, matched_seconds) VALUES (?,?,?,?,?)",
                normalized
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"Warning: Batch insert error: {e}")

    # ==================== FAILED FILE METHODS ====================
    
    def is_failed(self, content_key: str) -> bool:
        """Check if a content_key is in the failed list."""
        if not content_key:
            return False
        try:
            return self._get_conn().execute(
                "SELECT 1 FROM failed_files WHERE content_key=?", (content_key,)
            ).fetchone() is not None
        except sqlite3.Error:
            return False

    def mark_failed(self, content_key: str, path: str, reason: str):
        """Mark a file as failed (by content_key)."""
        if not content_key:
            return
        try:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO failed_files (content_key, last_path, reason) VALUES (?,?,?)",
                (content_key, path, reason)
            )
            conn.commit()
        except sqlite3.Error:
            pass

    # ==================== DISMISSED PAIRS ====================
    
    def _normalize_dismissed_pair(self, k1: str, k2: str) -> Tuple[str, str]:
        return (k1, k2) if k1 <= k2 else (k2, k1)

    def get_all_dismissed(self) -> set:
        """Return set of (key1, key2) tuples for all dismissed pairs."""
        result = set()
        try:
            for row in self._get_conn().execute("SELECT key1, key2 FROM dismissed_pairs"):
                result.add((row[0], row[1]))
        except sqlite3.Error:
            pass
        return result

    def batch_add_dismissed(self, pairs: List[Tuple[str, str]]):
        """Add dismissed content_key pairs (normalized)."""
        if not pairs:
            return
        try:
            conn = self._get_conn()
            normalized = [self._normalize_dismissed_pair(k1, k2) for k1, k2 in pairs]
            conn.executemany("INSERT OR IGNORE INTO dismissed_pairs (key1, key2) VALUES (?,?)", normalized)
            conn.commit()
        except sqlite3.Error:
            pass

    def clear_dismissed(self):
        try:
            c = self._get_conn()
            c.execute("DELETE FROM dismissed_pairs")
            c.commit()
        except sqlite3.Error:
            pass

    def get_dismissed_count(self) -> int:
        try:
            return self._get_conn().execute("SELECT COUNT(*) FROM dismissed_pairs").fetchone()[0]
        except sqlite3.Error:
            return 0

    # ==================== CACHE MANAGEMENT ====================
    
    def clear_fingerprints(self):
        try:
            c = self._get_conn()
            c.execute("DELETE FROM fingerprints")
            c.commit()
        except sqlite3.Error:
            pass

    def clear_comparisons(self):
        try:
            c = self._get_conn()
            # DROP + CREATE is O(1) and avoids WAL bloat vs DELETE FROM on large tables
            c.execute("DROP TABLE IF EXISTS comparisons")
            c.execute("""CREATE TABLE comparisons (
                key1 TEXT NOT NULL, key2 TEXT NOT NULL,
                match_ratio REAL NOT NULL, length_ratio REAL NOT NULL,
                matched_seconds REAL NOT NULL, PRIMARY KEY (key1, key2))""")
            c.execute("CREATE INDEX idx_cmp_k1 ON comparisons(key1)")
            c.execute("CREATE INDEX idx_cmp_k2 ON comparisons(key2)")
            c.commit()
        except sqlite3.Error:
            pass

    def clear_failed(self):
        try:
            c = self._get_conn()
            c.execute("DELETE FROM failed_files")
            c.commit()
        except sqlite3.Error:
            pass

    def get_failed_count(self) -> int:
        try:
            return self._get_conn().execute("SELECT COUNT(*) FROM failed_files").fetchone()[0]
        except sqlite3.Error:
            return 0

    def get_comparison_count(self) -> int:
        try:
            return self._get_conn().execute("SELECT COUNT(*) FROM comparisons").fetchone()[0]
        except sqlite3.Error:
            return 0

    def get_fingerprint_count(self) -> int:
        try:
            return self._get_conn().execute("SELECT COUNT(*) FROM fingerprints").fetchone()[0]
        except sqlite3.Error:
            return 0

    def wal_checkpoint(self):
        """Force WAL merge to prevent journal bloat."""
        try:
            self._get_conn().execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass

    def cleanup_orphans(self, active_keys: set, root_dirs: List[str]) -> Dict[str, int]:
        """
        Remove cached data for files that no longer exist in the scanned directories.
        
        SCOPED TO ROOT_DIRS ONLY: Only deletes fingerprints whose current_path falls
        under one of the scanned directories. Fingerprints from other directories
        (e.g., a different NAS share scanned last week) are left untouched.
        
        Comparisons are only deleted if BOTH keys are orphaned within scope.
        Then VACUUMs to reclaim disk space.
        
        Returns dict with counts of removed entries.
        """
        removed = {'fingerprints': 0, 'comparisons': 0, 'failed': 0, 'dismissed': 0}
        try:
            conn = self._get_conn()
            
            # Step 1: Find all content_keys in the DB whose current_path falls under
            # one of the scanned root_dirs — these are "in scope" for cleanup
            # os.path.join(rd, '') ensures trailing separator so Z:\Movies doesn't match Z:\Movies_Docs
            safe_roots = [os.path.join(rd, '').lower() for rd in root_dirs]
            in_scope_keys = set()
            for row in conn.execute("SELECT content_key, current_path FROM fingerprints"):
                ck, stored_path = row[0], (row[1] or '').lower()
                for sr in safe_roots:
                    if stored_path.startswith(sr):
                        in_scope_keys.add(ck)
                        break
            
            # Orphans = in-scope keys that aren't in the current scan's active_keys
            orphan_keys = in_scope_keys - active_keys
            
            if not orphan_keys:
                return removed
            
            # Create temp table of orphan keys for efficient deletes
            conn.execute("DROP TABLE IF EXISTS temp_orphan_keys")
            conn.execute("CREATE TEMP TABLE temp_orphan_keys (key TEXT PRIMARY KEY)")
            keys_list = list(orphan_keys)
            for i in range(0, len(keys_list), 50000):
                conn.executemany("INSERT OR IGNORE INTO temp_orphan_keys VALUES (?)",
                                 [(k,) for k in keys_list[i:i+50000]])
            
            # Count and delete orphaned fingerprints
            removed['fingerprints'] = len(orphan_keys)
            conn.execute("DELETE FROM fingerprints WHERE content_key IN (SELECT key FROM temp_orphan_keys)")
            
            # Delete comparisons where EITHER key is orphaned
            removed['comparisons'] = conn.execute(
                "SELECT COUNT(*) FROM comparisons WHERE key1 IN (SELECT key FROM temp_orphan_keys) OR key2 IN (SELECT key FROM temp_orphan_keys)"
            ).fetchone()[0]
            if removed['comparisons'] > 0:
                conn.execute("DELETE FROM comparisons WHERE key1 IN (SELECT key FROM temp_orphan_keys) OR key2 IN (SELECT key FROM temp_orphan_keys)")
            
            # Delete orphaned failed entries
            removed['failed'] = conn.execute(
                "SELECT COUNT(*) FROM failed_files WHERE content_key IN (SELECT key FROM temp_orphan_keys)"
            ).fetchone()[0]
            if removed['failed'] > 0:
                conn.execute("DELETE FROM failed_files WHERE content_key IN (SELECT key FROM temp_orphan_keys)")
            
            # Delete dismissed pairs where EITHER key is orphaned
            removed['dismissed'] = conn.execute(
                "SELECT COUNT(*) FROM dismissed_pairs WHERE key1 IN (SELECT key FROM temp_orphan_keys) OR key2 IN (SELECT key FROM temp_orphan_keys)"
            ).fetchone()[0]
            if removed['dismissed'] > 0:
                conn.execute("DELETE FROM dismissed_pairs WHERE key1 IN (SELECT key FROM temp_orphan_keys) OR key2 IN (SELECT key FROM temp_orphan_keys)")
            
            conn.execute("DROP TABLE IF EXISTS temp_orphan_keys")
            conn.commit()
            
            # VACUUM reclaims disk space (rewrites the entire DB file)
            if any(v > 0 for v in removed.values()):
                print("   Reclaiming disk space (VACUUM)...")
                conn.execute("VACUUM")
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            
        except sqlite3.Error as e:
            print(f"Warning: Cleanup error: {e}")
        
        return removed

    def get_db_size(self) -> int:
        """Get total size of DB file + WAL + SHM in bytes."""
        total = 0
        for suffix in ('', '-wal', '-shm'):
            path = self.db_path + suffix
            try:
                total += os.path.getsize(path)
            except OSError:
                pass
        return total

# ============================================================================
# EXTRACTION
# ============================================================================

def _drain_stderr(pipe, result_holder):
    try:
        result_holder[0] = pipe.read(4096)
        pipe.read()
    except (OSError, ValueError):
        pass

def _kill_proc(proc):
    if proc is None:
        return
    try:
        proc.kill()
        proc.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass

def _get_short_path(long_path: str) -> str:
    """Convert a path to a DOS 8.3 short path for legacy C binaries."""
    if os.name != 'nt':
        return long_path
    try:
        import ctypes
        from ctypes import wintypes
        GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        GetShortPathNameW.restype = wintypes.DWORD
        
        # Use extended-length path to handle >260 char paths
        abs_path = _long_path_safe(long_path)
        buf_size = GetShortPathNameW(abs_path, None, 0)
        if buf_size == 0:
            return long_path
        buffer = ctypes.create_unicode_buffer(buf_size)
        GetShortPathNameW(abs_path, buffer, buf_size)
        result = buffer.value
        
        # Strip \\?\ or \\?\UNC\ prefix — Windows cwd (SetCurrentDirectoryW) doesn't accept them
        if result.startswith('\\\\?\\UNC\\'):
            result = '\\\\' + result[8:]  # \\?\UNC\server\share → \\server\share
        elif result.startswith('\\\\?\\'):
            result = result[4:]  # \\?\C:\path → C:\path
        
        return result
    except (OSError, AttributeError, ValueError):
        return long_path

# Characters that cause fpcalc/FFmpeg to choke
# [] {} — treated as pattern/sequence characters by FFmpeg demuxer
# #     — treated as URL fragment separator (everything after # is ignored)
# %     — treated as URL percent-encoding prefix
# &     — treated as URL query separator
# （）【】 — fullwidth CJK punctuation that survives 8.3 conversion on some NAS
_FPCALC_UNSAFE_CHARS = set('[]{}#%&（）【】')
def _is_fpcalc_safe(path: str) -> bool:
    """Check if a path is safe to pass directly to fpcalc."""
    if not path.isascii():
        return False
    return not any(c in _FPCALC_UNSAFE_CHARS for c in path)

def _make_safe_path(original_path: str) -> Tuple[str, Optional[str]]:
    """
    Get an fpcalc-safe path. Tries 8.3 short path first.
    If that still has unsafe chars, creates a temp hardlink or symlink.
    
    CRITICAL: Never copies video files to avoid network/SSD thrashing.
    If all zero-copy methods fail, returns original path and lets fpcalc try.
    
    Returns (safe_path, cleanup_path_or_None).
    """
    import uuid
    
    short = _get_short_path(original_path)
    if _is_fpcalc_safe(short):
        return short, None
    
    ext = os.path.splitext(original_path)[1].lower()
    if not ext or not ext.isascii():
        ext = '.tmp'
    safe_ext = ''.join(c for c in ext if c.isalnum() or c == '.')
    
    # Get the directory of the original file
    original_dir = os.path.dirname(os.path.abspath(original_path))
    
    try:
        # Try hardlink in SAME directory (instant, zero-copy, same volume guaranteed)
        temp_path = os.path.join(original_dir, f'vcd_{uuid.uuid4().hex}{safe_ext}')
        os.link(original_path, temp_path)
        return temp_path, temp_path
    except OSError:
        pass
    
    try:
        # Try symlink in same directory (zero-copy, works cross-volume)
        # Note: Windows symlinks require Developer Mode or Admin rights
        temp_path = os.path.join(original_dir, f'vcd_{uuid.uuid4().hex}{safe_ext}')
        os.symlink(original_path, temp_path)
        return temp_path, temp_path
    except OSError:
        pass
    
    try:
        # Try symlink in system temp dir (works even if NAS is read-only)
        # Symlinks are zero-byte pointers — no file data is copied over the network
        temp_path = os.path.join(tempfile.gettempdir(), f'vcd_{uuid.uuid4().hex}{safe_ext}')
        os.symlink(os.path.abspath(original_path), temp_path)
        return temp_path, temp_path
    except OSError:
        pass
    
    # Do NOT copy video files - could be multi-GB over network
    # Better to let fpcalc try the original path and fail with a clear error
    return original_path, None

def _try_ffmpeg_fallback(original_path: str, video_timeout: int) -> Tuple[Optional[np.ndarray], float, str]:
    """
    ffmpeg fallback for containers that fpcalc can't handle (MPEG-PS, MPEG-TS, VOB).
    Extracts audio to a temp WAV, then runs fpcalc on that.
    
    Returns (arr_or_None, duration, error_string).
    """
    if not FFMPEG_PATH:
        return None, 0.0, "ffmpeg not available for fallback"
    
    temp_wav = None
    try:
        # Extract audio to temp WAV (PCM 16-bit, 44.1kHz, mono)
        # This transcodes the audio stream into a format fpcalc always handles
        fd, temp_wav = tempfile.mkstemp(suffix='.wav', prefix='vcd_ffmpeg_')
        os.close(fd)
        
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        
        ffmpeg_cmd = [
            FFMPEG_PATH, '-i', original_path,
            '-vn',                    # No video
            '-acodec', 'pcm_s16le',   # Raw PCM (universally supported)
            '-ar', '44100',           # 44.1kHz
            '-ac', '1',               # Mono (saves space, fpcalc only needs mono)
            '-y',                     # Overwrite output
            temp_wav
        ]
        
        result = subprocess.run(
            ffmpeg_cmd, capture_output=True, timeout=video_timeout,
            startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        if result.returncode != 0:
            err = result.stderr.decode('utf-8', errors='ignore').strip()[-200:]
            return None, 0.0, f"ffmpeg failed: {err}"
        
        if not os.path.exists(temp_wav) or os.path.getsize(temp_wav) < 1000:
            return None, 0.0, "ffmpeg produced empty/tiny output"
        
        # Now run fpcalc on the clean WAV
        fpcalc_cmd = [FPCALC_PATH, '-raw', '-length', '0', '-json', temp_wav]
        fpcalc_result = subprocess.run(
            fpcalc_cmd, capture_output=True, timeout=video_timeout,
            startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        if fpcalc_result.returncode != 0:
            err = fpcalc_result.stderr.decode('utf-8', errors='ignore').strip()[:200]
            return None, 0.0, f"fpcalc on ffmpeg WAV failed: {err}"
        
        data = json.loads(fpcalc_result.stdout.decode('utf-8'))
        fp = data.get('fingerprint', [])
        if not fp:
            return None, 0.0, "ffmpeg WAV produced empty fingerprint"
        
        return np.array(fp, dtype=np.uint32), data.get('duration', 0.0), ""
    
    except subprocess.TimeoutExpired:
        return None, 0.0, f"ffmpeg fallback timeout ({video_timeout}s)"
    except Exception as e:
        return None, 0.0, f"ffmpeg fallback error: {str(e)[:150]}"
    finally:
        if temp_wav:
            try:
                os.remove(temp_wav)
            except OSError:
                pass

def extract_audio_fingerprint(args):
    """
    Extract Chromaprint fingerprint.
    Returns 7-tuple: (path, content_key, size, arr, duration, error, stderr_bytes)
    
    If fpcalc fails on MPEG-PS/TS containers and ffmpeg is available,
    automatically falls back to ffmpeg audio extraction.
    """
    path, content_key, size, video_timeout = args
    process = None
    cleanup_path = None
    
    try:
        safe_path, cleanup_path = _make_safe_path(path)
        
        # Use cwd= so fpcalc only sees the filename, not Unicode parent directories
        # Windows handles the directory change natively, bypassing fpcalc's ANSI parsing
        work_dir = os.path.dirname(os.path.abspath(safe_path))
        filename_only = os.path.basename(safe_path)
        cmd = [FPCALC_PATH, '-raw', '-length', '0', '-json', filename_only]
        
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW,
                                   cwd=work_dir)
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
            return path, content_key, size, None, 0.0, "fpcalc stdout exceeded 50MB cap", b''

        if process.returncode != 0:
            err = stderr_bytes.decode('utf-8', errors='ignore').strip()[:200]
            fpcalc_err = err or f"fpcalc exit code {process.returncode}"
            
            # ffmpeg fallback — try on ANY fpcalc failure, not just specific extensions
            if FFMPEG_PATH:
                arr, duration, ffmpeg_err = _try_ffmpeg_fallback(path, video_timeout)
                if arr is not None:
                    return path, content_key, size, arr, duration, "", b''
                # Both failed — report both errors
                return path, content_key, size, None, 0.0, f"{fpcalc_err} | ffmpeg: {ffmpeg_err}", stderr_bytes
            
            return path, content_key, size, None, 0.0, fpcalc_err, stderr_bytes

        data = json.loads(stdout_bytes.decode('utf-8'))
        fp = data.get('fingerprint', [])
        if not fp:
            # Also try ffmpeg fallback for empty fingerprints (audio codec not supported)
            if FFMPEG_PATH:
                arr, duration, ffmpeg_err = _try_ffmpeg_fallback(path, video_timeout)
                if arr is not None:
                    return path, content_key, size, arr, duration, "", b''
            return path, content_key, size, None, 0.0, "Empty fingerprint (no audio stream?)", b''
        return path, content_key, size, np.array(fp, dtype=np.uint32), data.get('duration', 0.0), "", b''

    except subprocess.TimeoutExpired:
        _kill_proc(process)
        return path, content_key, size, None, 0.0, f"Timeout ({video_timeout}s)", b''
    except json.JSONDecodeError as e:
        return path, content_key, size, None, 0.0, f"Invalid fpcalc JSON: {e}", b''
    except Exception as e:
        _kill_proc(process)
        return path, content_key, size, None, 0.0, str(e)[:200], b''
    finally:
        if cleanup_path:
            try:
                os.remove(cleanup_path)
            except OSError:
                pass

# ============================================================================
# COMPARISON
# ============================================================================

def compare_audio_pair(key1: str, key2: str, config: Config):
    """Compare two fingerprints by content_key. Returns raw metrics (no early exit)."""
    global GLOBAL_ARRAYS
    arr1, arr2 = GLOBAL_ARRAYS.get(key1), GLOBAL_ARRAYS.get(key2)
    if arr1 is None or arr2 is None:
        return key1, key2, 0.0, 0.0, 0.0
    
    arr_short, arr_long = (arr1, arr2) if len(arr1) <= len(arr2) else (arr2, arr1)
    if len(arr_short) < config.chunk_size:
        return key1, key2, 0.0, 0.0, 0.0

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
    return key1, key2, match_ratio, length_ratio, matched_seconds

def classify_comparison(match_ratio: float, length_ratio: float, matched_seconds: float, config: Config):
    """Apply thresholds to raw metrics. Returns (is_dup, is_clip)."""
    if matched_seconds < config.intro_filter_seconds and match_ratio > 0:
        return False, False
    is_dup = match_ratio >= config.duplicate_match_ratio and length_ratio < 0.1
    is_clip = not is_dup and match_ratio >= config.clip_match_ratio
    return is_dup, is_clip

def compare_batch(batch: List[Tuple[str, str]], config: Config):
    """Compare a batch of content_key pairs."""
    results = []
    for k1, k2 in batch:
        _, _, mr, lr, ms = compare_audio_pair(k1, k2, config)
        nk1, nk2 = (k1, k2) if k1 <= k2 else (k2, k1)  # Normalize
        results.append((nk1, nk2, mr, lr, ms))
    return results

# ============================================================================
# HTML INTERACTIVE REPORT
# ============================================================================

def _generate_html_report(results_dir, dup_groups, clip_deletions, clip_dismissed, clip_content_keys, fingerprints, display_root, total_delete, total_savings, partial=False):
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
                'relpath': _safe_relpath(v['path'], display_root),
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
            'identical': g.get('identical', False),
            'previously_skipped': g.get('previously_skipped', False),
            'content_keys': g.get('content_keys', []),
        })

    clips_js = []
    for ci, (child, parents, ratio) in enumerate(clip_deletions):
        child_fp = fingerprints.get(child, {})
        parent_files = []
        for p in parents[:3]:
            pfp = fingerprints.get(p, {})
            parent_files.append({
                'path': os.path.abspath(p),
                'dir': os.path.dirname(os.path.abspath(p)),
                'name': os.path.basename(p),
                'size': pfp.get('file_size', 0),
                'size_fmt': format_size(pfp.get('file_size', 0)),
                'dur_fmt': format_duration(pfp.get('duration', 0)),
            })
        ck_child, ck_parents = clip_content_keys[ci] if ci < len(clip_content_keys) else ('', [])
        clips_js.append({
            'id': ci,
            'child_path': os.path.abspath(child),
            'child_dir': os.path.dirname(os.path.abspath(child)),
            'child_name': os.path.basename(child),
            'child_size': child_fp.get('file_size', 0),
            'child_size_fmt': format_size(child_fp.get('file_size', 0)),
            'child_dur': format_duration(child_fp.get('duration', 0)),
            'ratio': f"{ratio*100:.0f}%",
            'parents': parent_files,
            'delete': True,
            'previously_skipped': clip_dismissed[ci] if ci < len(clip_dismissed) else False,
            'ck_child': ck_child,
            'ck_parents': ck_parents,
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
.card {{ background: #16213e; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #0f3460; overflow: hidden; }}
.card.warning {{ border-left-color: #e9a045; }}
.card.identical {{ border-left-color: #4ecca3; }}
.card.prev-skipped {{ opacity: 0.6; border-left-color: #555; }}
.card-header {{ display: flex; justify-content: space-between; align-items: center; padding: 12px 20px; cursor: pointer; user-select: none; }}
.card-header:hover {{ background: #1a2a4e; }}
.card-title {{ font-weight: bold; font-size: 14px; }}
.card-summary {{ font-size: 12px; color: #aaa; margin-left: 12px; flex: 1; text-align: right; }}
.card-summary .keep-name {{ color: #4ecca3; font-weight: bold; }}
.card-summary .skip-label {{ color: #888; }}
.card-summary .action-label {{ color: #e94560; }}
.card-body {{ padding: 0 20px 15px; }}
.card-body.collapsed {{ display: none; }}
.badge {{ padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; white-space: nowrap; }}
.badge-warn {{ background: #e9a045; color: #000; }}
.badge-savings {{ background: #0f3460; color: #e0e0e0; }}
.badge-clip {{ background: #533483; color: #e0e0e0; }}
.badge-identical {{ background: #4ecca3; color: #000; }}
.badge-skipped {{ background: #555; color: #ccc; }}
.badge-rec {{ background: transparent; border: 1px solid #4ecca3; color: #4ecca3; }}
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
.generate {{ position: sticky; bottom: 20px; background: #e94560; color: #fff; border: none; padding: 15px 30px; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; width: 100%; max-width: 500px; margin: 20px auto; display: block; box-shadow: 0 4px 15px rgba(233,69,96,0.4); }}
.generate:hover {{ background: #d63851; }}
.generate:disabled {{ background: #555; box-shadow: none; cursor: default; }}
.section-title {{ font-size: 20px; font-weight: bold; margin: 25px 0 15px; color: #4ecca3; }}
.rename-info {{ font-size: 11px; color: #b388ff; margin-top: 4px; font-style: italic; }}
.counter {{ position: fixed; top: 10px; right: 20px; background: #16213e; padding: 10px 15px; border-radius: 8px; font-size: 13px; z-index: 100; border: 1px solid #333; }}
.file-link {{ color: #7eb8ff; text-decoration: none; cursor: pointer; }}
.file-link:hover {{ text-decoration: underline; color: #a8d4ff; }}
.btn-open {{ background: #2a5a8a; color: #fff; font-size: 11px; padding: 3px 8px; }}
.btn-open:hover {{ background: #3a7aba; }}
.folder-link {{ color: #888; font-size: 11px; text-decoration: none; }}
.folder-link:hover {{ color: #aaa; text-decoration: underline; }}
.rename-input {{ background: #1a1a2e; color: #e0e0e0; border: 1px solid #533483; border-radius: 4px; padding: 3px 6px; font-size: 12px; width: 300px; margin-left: 4px; }}
.rename-input:focus {{ outline: none; border-color: #b388ff; }}
.dir-select {{ background: #1a1a2e; color: #e0e0e0; border: 1px solid #533483; border-radius: 4px; padding: 2px 4px; font-size: 11px; max-width: 400px; margin-left: 4px; }}
.toggle-bar {{ background: #16213e; padding: 10px 20px; border-radius: 8px; margin-bottom: 15px; display: flex; align-items: center; gap: 15px; border: 1px solid #333; font-size: 13px; }}
.toggle-bar label {{ cursor: pointer; }}
.toggle-bar input {{ cursor: pointer; }}
.expand-all {{ background: #0f3460; color: #aaa; border: 1px solid #333; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 12px; }}
.expand-all:hover {{ color: #fff; border-color: #555; }}
</style>
</head>
<body>
<h1>VidClipDuplis — Review Results</h1>
<p class="subtitle">Review matches, then download a custom PowerShell script. All groups default to <b>Skip</b> — nothing happens unless you decide.</p>
{'<div style="background:#e9a045;color:#000;padding:12px 20px;border-radius:8px;margin-bottom:20px;font-weight:bold;">⚠️ PARTIAL RESULTS — Scan was interrupted (Ctrl+C). Not all comparisons were completed. Run again to get full results. Matches shown are from cached + computed comparisons so far.</div>' if partial else ''}

<div class="summary">
 <div class="stat"><div class="num" id="del-count">0</div><div class="label">Files to Delete</div></div>
 <div class="stat"><div class="num" id="rename-count">0</div><div class="label">Renames / Moves</div></div>
 <div class="stat"><div class="num" id="skip-count">0</div><div class="label">Groups Skipped</div></div>
 <div class="stat"><div class="num">{format_size(total_savings)}</div><div class="label">Max Potential Savings</div></div>
</div>

<div class="counter">
 <span id="action-count">0</span> actions &middot; <span id="skip-count2">0</span> skipped
</div>

<div class="toggle-bar">
 <label><input type="checkbox" id="show-prev-skipped" onchange="togglePrevSkipped()"> Show previously skipped (<span id="prev-skip-count">0</span>)</label>
 <button class="expand-all" onclick="expandAll()">Expand All</button>
 <button class="expand-all" onclick="collapseAll()">Collapse All</button>
</div>

<div id="groups-container"></div>
<div id="clips-container"></div>

<div style="max-width:500px;margin:25px auto 5px;padding:12px 16px;background:#16213e;border-radius:8px;border:1px solid #333;font-size:12px;color:#aaa;text-align:center;">
Download your custom script, then save the dismissed-groups file next to VidClipDupli.py so skipped groups stay hidden on the next run.
</div>

<button class="generate" onclick="generateScript()" id="btn-generate">Download Custom PowerShell Script</button>
<button class="generate" onclick="saveDismissed()" style="background:#555;box-shadow:none;margin-top:5px;font-size:13px;">Save Dismissed Groups (vcd_dismissed.json)</button>

<script>
const DATA = {data_json};
const state = {{}};
const clipState = {{}};
const expanded = {{}};
const clipExpanded = {{}};

function init() {{
  const gc = document.getElementById('groups-container');
  const cc = document.getElementById('clips-container');
  const newGroups = DATA.groups.filter(g => !g.previously_skipped);
  const prevGroups = DATA.groups.filter(g => g.previously_skipped);
  const newClips = DATA.clips.filter(c => !c.previously_skipped);
  const prevClips = DATA.clips.filter(c => c.previously_skipped);
  document.getElementById('prev-skip-count').textContent = prevGroups.length + prevClips.length;

  let ghtml = '';
  if (newGroups.length > 0) ghtml += '<div class="section-title">Duplicate Groups (' + newGroups.length + ')</div>';

  DATA.groups.forEach((g, gi) => {{
    const keepIdx = g.files.findIndex(f => f.is_keep);
    const ki = keepIdx >= 0 ? keepIdx : 0;
    state[gi] = {{ keep: ki, rename_from: null, custom_name: null, target_dir: g.files[ki].dir, action: 'skipped' }};
    expanded[gi] = false;
    const isPrev = g.previously_skipped;
    let warn = g.size_warning ? ' warning' : (g.identical ? ' identical' : '');
    if (isPrev) warn += ' prev-skipped';
    let badges = '';
    if (g.identical) badges += '<span class="badge badge-identical">IDENTICAL</span> ';
    if (g.size_warning) badges += '<span class="badge badge-warn">SIZE MISMATCH</span> ';
    badges += '<span class="badge badge-rec">\u2605 ' + escHtml(g.files[ki].filename) + '</span>';
    ghtml += '<div class="card' + warn + '" id="group-' + gi + '"' + (isPrev ? ' data-prev="1" style="display:none"' : '') + '>';
    ghtml += '<div class="card-header" onclick="toggleGroup(' + gi + ')"><span class="card-title">Group ' + (gi+1) + ' (' + g.files.length + ' files) ' + badges + '</span>';
    ghtml += '<span class="card-summary" id="summary-' + gi + '"><span class="skip-label">Skipped</span></span></div>';
    ghtml += '<div class="card-body collapsed" id="body-' + gi + '">';
    g.files.forEach((f, fi) => {{
      const pe = pathEsc(f.path);
      ghtml += '<div class="file-row" id="row-' + gi + '-' + fi + '"><div class="file-info">';
      ghtml += '<div class="file-path"><a class="file-link" href="#" onclick="event.stopPropagation();openFile(\\\'' + pe + '\\\');return false;"><span class="file-name">' + escHtml(f.filename) + '</span></a></div>';
      ghtml += '<div class="file-path" style="font-size:11px;color:#666"><a class="folder-link" href="#" onclick="event.stopPropagation();openFolder(\\\'' + pe + '\\\');return false;">' + escHtml(f.dir) + '</a></div>';
      ghtml += '<div class="file-meta">' + f.size_fmt + ' \u00b7 ' + f.dur_fmt + '</div>';
      ghtml += '<div class="rename-info" id="rename-info-' + gi + '-' + fi + '" style="display:none"></div></div>';
      ghtml += '<div class="actions"><button class="btn btn-open" onclick="event.stopPropagation();openFile(\\\'' + pe + '\\\')">Open</button>';
      ghtml += '<button class="btn btn-keep" onclick="event.stopPropagation();setKeep(' + gi + ',' + fi + ')">Keep</button>';
      ghtml += '<button class="btn btn-delete" onclick="event.stopPropagation();setDelete(' + gi + ',' + fi + ')">Delete</button>';
      ghtml += '<button class="btn btn-rename" onclick="event.stopPropagation();setRename(' + gi + ',' + fi + ')">Use Name</button></div></div>';
    }});
    ghtml += '<div style="text-align:right;margin-top:8px"><button class="btn btn-skip" onclick="event.stopPropagation();setSkip(' + gi + ')">Skip Group</button></div>';
    ghtml += '</div></div>';
  }});
  if (prevGroups.length > 0) ghtml += '<div class="section-title" id="prev-dup-title" style="display:none">Previously Skipped Duplicates (' + prevGroups.length + ')</div>';
  gc.innerHTML = ghtml;

  let chtml = '';
  if (newClips.length > 0) chtml += '<div class="section-title">Redundant Clips (' + newClips.length + ')</div>';
  DATA.clips.forEach((c, ci) => {{
    const parent = c.parents[0] || {{}};
    clipState[ci] = {{ keep: 'parent', rename_to: null, custom_name: null, target_dir: (parent.dir || ''), action: 'skipped' }};
    clipExpanded[ci] = false;
    const isPrev = c.previously_skipped;
    const parentPathEsc = pathEsc(parent.path || '');
    const childPathEsc = pathEsc(c.child_path);
    chtml += '<div class="card' + (isPrev ? ' prev-skipped' : '') + '" id="clip-' + ci + '"' + (isPrev ? ' data-prev="1" style="display:none"' : '') + '>';
    chtml += '<div class="card-header" onclick="toggleClip(' + ci + ')"><span class="card-title">Clip ' + (ci+1) + ' <span class="badge badge-clip">' + c.ratio + '</span> ' + escHtml(c.child_name) + '</span>';
    chtml += '<span class="card-summary" id="clip-summary-' + ci + '"><span class="skip-label">Skipped</span></span></div>';
    chtml += '<div class="card-body collapsed" id="clip-body-' + ci + '">';
    chtml += '<div class="file-row" id="clip-' + ci + '-parent"><div class="file-info">';
    chtml += '<div class="file-path"><a class="file-link" href="#" onclick="event.stopPropagation();openFile(\\\'' + parentPathEsc + '\\\');return false;"><span class="file-name">' + escHtml(parent.name || 'Unknown') + '</span></a> <span style="color:#4ecca3;font-size:11px">PARENT</span></div>';
    chtml += '<div class="file-path" style="font-size:11px;color:#666"><a class="folder-link" href="#" onclick="event.stopPropagation();openFolder(\\\'' + parentPathEsc + '\\\');return false;">' + escHtml(parent.dir || '') + '</a></div>';
    chtml += '<div class="file-meta">' + (parent.size_fmt||'?') + ' \u00b7 ' + (parent.dur_fmt||'?') + '</div>';
    chtml += '<div class="rename-info" id="clip-rename-' + ci + '-parent" style="display:none"></div></div>';
    chtml += '<div class="actions"><button class="btn btn-open" onclick="event.stopPropagation();openFile(\\\'' + parentPathEsc + '\\\')">Open</button>';
    chtml += '<button class="btn btn-keep" onclick="event.stopPropagation();setClipKeep(' + ci + ',\\\'parent\\\')">Keep</button>';
    chtml += '<button class="btn btn-rename" onclick="event.stopPropagation();setClipName(' + ci + ',\\\'clip\\\')">Use Name</button></div></div>';
    chtml += '<div class="file-row" id="clip-' + ci + '-clip"><div class="file-info">';
    chtml += '<div class="file-path"><a class="file-link" href="#" onclick="event.stopPropagation();openFile(\\\'' + childPathEsc + '\\\');return false;"><span class="file-name">' + escHtml(c.child_name) + '</span></a> <span style="color:#e94560;font-size:11px">CLIP</span></div>';
    chtml += '<div class="file-path" style="font-size:11px;color:#666"><a class="folder-link" href="#" onclick="event.stopPropagation();openFolder(\\\'' + childPathEsc + '\\\');return false;">' + escHtml(c.child_dir) + '</a></div>';
    chtml += '<div class="file-meta">' + c.child_size_fmt + ' \u00b7 ' + c.child_dur + '</div>';
    chtml += '<div class="rename-info" id="clip-rename-' + ci + '-clip" style="display:none"></div></div>';
    chtml += '<div class="actions"><button class="btn btn-open" onclick="event.stopPropagation();openFile(\\\'' + childPathEsc + '\\\')">Open</button>';
    chtml += '<button class="btn btn-keep" onclick="event.stopPropagation();setClipKeep(' + ci + ',\\\'clip\\\')">Keep</button>';
    chtml += '<button class="btn btn-rename" onclick="event.stopPropagation();setClipName(' + ci + ',\\\'parent\\\')">Use Name</button></div></div>';
    chtml += '<div style="text-align:right;margin-top:8px"><button class="btn btn-skip" onclick="event.stopPropagation();setClipSkip(' + ci + ')">Skip</button></div>';
    chtml += '</div></div>';
  }});
  if (prevClips.length > 0) chtml += '<div class="section-title" id="prev-clip-title" style="display:none">Previously Skipped Clips (' + prevClips.length + ')</div>';
  cc.innerHTML = chtml;
  updateAllRows();
  updateCounters();
}}

function escHtml(s) {{ const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }}
function pathEsc(p) {{ return p.replace(/\\\\/g, '\\\\\\\\').replace(/'/g, "\\\\'").replace(/`/g, '\\\\`').replace(/"/g, '&quot;'); }}
function toFileUrl(path) {{ let p = path.replace(/\\\\/g, '/'); p = p.replace(/%/g, '%25').replace(/#/g, '%23').replace(/\\?/g, '%3F'); if (p.startsWith('//')) return 'file:' + p; return 'file:///' + p; }}
function openFile(path) {{ window.open(toFileUrl(path), '_blank'); }}
function openFolder(path) {{ const f = path.substring(0, path.lastIndexOf('\\\\')); window.open(toFileUrl(f), '_blank'); }}
function toggleGroup(gi) {{ expanded[gi] = !expanded[gi]; document.getElementById('body-' + gi).className = expanded[gi] ? 'card-body' : 'card-body collapsed'; }}
function toggleClip(ci) {{ clipExpanded[ci] = !clipExpanded[ci]; document.getElementById('clip-body-' + ci).className = clipExpanded[ci] ? 'card-body' : 'card-body collapsed'; }}
function expandAll() {{ DATA.groups.forEach((_, gi) => {{ expanded[gi] = true; document.getElementById('body-' + gi).className = 'card-body'; }}); DATA.clips.forEach((_, ci) => {{ clipExpanded[ci] = true; document.getElementById('clip-body-' + ci).className = 'card-body'; }}); }}
function collapseAll() {{ DATA.groups.forEach((_, gi) => {{ expanded[gi] = false; document.getElementById('body-' + gi).className = 'card-body collapsed'; }}); DATA.clips.forEach((_, ci) => {{ clipExpanded[ci] = false; document.getElementById('clip-body-' + ci).className = 'card-body collapsed'; }}); }}
function togglePrevSkipped() {{ const show = document.getElementById('show-prev-skipped').checked; document.querySelectorAll('[data-prev="1"]').forEach(el => el.style.display = show ? '' : 'none'); const pt1 = document.getElementById('prev-dup-title'); const pt2 = document.getElementById('prev-clip-title'); if (pt1) pt1.style.display = show ? '' : 'none'; if (pt2) pt2.style.display = show ? '' : 'none'; }}

function setKeep(gi, fi) {{ state[gi].keep = fi; state[gi].rename_from = null; state[gi].target_dir = DATA.groups[gi].files[fi].dir; state[gi].action = 'decided'; updateGroupRows(gi); updateGroupSummary(gi); updateCounters(); }}
function setDelete(gi, fi) {{ const g = DATA.groups[gi]; const oi = g.files.findIndex((_, i) => i !== fi); if (oi >= 0) {{ state[gi].keep = oi; state[gi].target_dir = g.files[oi].dir; }} state[gi].rename_from = null; state[gi].action = 'decided'; updateGroupRows(gi); updateGroupSummary(gi); updateCounters(); }}
function setRename(gi, fi) {{ if (state[gi].keep === fi) return; state[gi].rename_from = fi; state[gi].custom_name = DATA.groups[gi].files[fi].filename; state[gi].action = 'decided'; updateGroupRows(gi); updateGroupSummary(gi); updateCounters(); }}
function setSkip(gi) {{ state[gi].action = 'skipped'; updateGroupRows(gi); updateGroupSummary(gi); updateCounters(); }}
function setClipKeep(ci, which) {{ const c = DATA.clips[ci]; clipState[ci].keep = which; clipState[ci].rename_to = null; clipState[ci].target_dir = which === 'parent' ? (c.parents[0] ? c.parents[0].dir : '') : c.child_dir; clipState[ci].action = 'decided'; updateClipRows(ci); updateClipSummary(ci); updateCounters(); }}
function setClipName(ci, useNameFrom) {{ clipState[ci].rename_to = useNameFrom; const c = DATA.clips[ci]; clipState[ci].custom_name = useNameFrom === 'clip' ? c.child_name : (c.parents[0] ? c.parents[0].name : ''); clipState[ci].action = 'decided'; updateClipRows(ci); updateClipSummary(ci); updateCounters(); }}
function setClipSkip(ci) {{ clipState[ci].action = 'skipped'; updateClipRows(ci); updateClipSummary(ci); updateCounters(); }}

function updateGroupSummary(gi) {{
  const el = document.getElementById('summary-' + gi);
  const g = DATA.groups[gi]; const s = state[gi];
  if (s.action === 'skipped') {{ el.innerHTML = '<span class="skip-label">Skipped</span>'; return; }}
  const kf = g.files[s.keep];
  let name = kf.filename;
  if (s.rename_from !== null) name = s.custom_name || g.files[s.rename_from].filename;
  const dir = s.target_dir || kf.dir;
  el.innerHTML = '<span class="action-label">Delete ' + (g.files.length - 1) + '</span> \u00b7 Keep: <span class="keep-name">' + escHtml(name) + '</span> \u2192 ' + escHtml(dir);
}}

function updateClipSummary(ci) {{
  const el = document.getElementById('clip-summary-' + ci);
  const cs = clipState[ci]; const c = DATA.clips[ci];
  if (cs.action === 'skipped') {{ el.innerHTML = '<span class="skip-label">Skipped</span>'; return; }}
  const parent = c.parents[0] || {{}};
  if (cs.keep === 'parent') {{ el.innerHTML = '<span class="action-label">Delete clip</span> \u00b7 Keep: <span class="keep-name">' + escHtml(parent.name || '?') + '</span>'; }}
  else {{ el.innerHTML = '<span class="action-label">Delete parent</span> \u00b7 Keep: <span class="keep-name">' + escHtml(c.child_name) + '</span>'; }}
}}

function updateClipRows(ci) {{
  const c = DATA.clips[ci];
  const parentRow = document.getElementById('clip-' + ci + '-parent');
  const clipRow = document.getElementById('clip-' + ci + '-clip');
  const parentRename = document.getElementById('clip-rename-' + ci + '-parent');
  const clipRename = document.getElementById('clip-rename-' + ci + '-clip');
  parentRow.className = 'file-row'; clipRow.className = 'file-row';
  parentRename.style.display = 'none'; clipRename.style.display = 'none';
  parentRename.innerHTML = ''; clipRename.innerHTML = '';
  const pdir = c.parents[0] ? c.parents[0].dir : '';
  const hasDiffDirs = pdir && c.child_dir && pdir !== c.child_dir;
  const cdirs = hasDiffDirs ? [pdir, c.child_dir] : [];
  if (clipState[ci].action === 'skipped') {{ parentRow.className = 'file-row selected-skip'; clipRow.className = 'file-row selected-skip'; }}
  else if (clipState[ci].keep === 'parent') {{
    parentRow.className = 'file-row selected-keep'; clipRow.className = 'file-row selected-delete';
    let rhtml = '';
    if (clipState[ci].rename_to === 'clip') {{ const cn = clipState[ci].custom_name || c.child_name; rhtml += '<span style="color:#b388ff">Rename to: </span><input type="text" class="rename-input" value="' + escHtml(cn) + '" onchange="clipState[' + ci + '].custom_name=this.value" onclick="event.stopPropagation()" />'; }}
    if (hasDiffDirs) {{ rhtml += (rhtml?'<br>':'') + '<span style="color:#b388ff;font-size:11px">Save in: </span><select class="dir-select" onchange="clipState[' + ci + '].target_dir=this.value" onclick="event.stopPropagation()">'; cdirs.forEach(d => {{ rhtml += '<option value="' + escHtml(d) + '"' + (d===(clipState[ci].target_dir||pdir)?' selected':'') + '>' + escHtml(d) + '</option>'; }}); rhtml += '</select>'; }}
    if (rhtml) {{ parentRename.innerHTML = rhtml; parentRename.style.display = 'block'; }}
  }} else {{
    clipRow.className = 'file-row selected-keep'; parentRow.className = 'file-row selected-delete';
    let rhtml = '';
    if (clipState[ci].rename_to === 'parent') {{ const pn = clipState[ci].custom_name || (c.parents[0] ? c.parents[0].name : ''); rhtml += '<span style="color:#b388ff">Rename to: </span><input type="text" class="rename-input" value="' + escHtml(pn) + '" onchange="clipState[' + ci + '].custom_name=this.value" onclick="event.stopPropagation()" />'; }}
    if (hasDiffDirs) {{ rhtml += (rhtml?'<br>':'') + '<span style="color:#b388ff;font-size:11px">Save in: </span><select class="dir-select" onchange="clipState[' + ci + '].target_dir=this.value" onclick="event.stopPropagation()">'; cdirs.forEach(d => {{ rhtml += '<option value="' + escHtml(d) + '"' + (d===(clipState[ci].target_dir||c.child_dir)?' selected':'') + '>' + escHtml(d) + '</option>'; }}); rhtml += '</select>'; }}
    if (rhtml) {{ clipRename.innerHTML = rhtml; clipRename.style.display = 'block'; }}
  }}
}}

function updateGroupRows(gi) {{
  const g = DATA.groups[gi]; const dirs = [...new Set(g.files.map(x => x.dir))];
  g.files.forEach((f, fi) => {{
    const row = document.getElementById('row-' + gi + '-' + fi);
    const ri = document.getElementById('rename-info-' + gi + '-' + fi);
    row.className = 'file-row'; ri.style.display = 'none'; ri.innerHTML = '';
    if (state[gi].action === 'skipped') {{ row.className = 'file-row selected-skip'; }}
    else if (fi === state[gi].keep) {{
      row.className = 'file-row selected-keep';
      let rhtml = '';
      if (state[gi].rename_from !== null) {{ const cn = state[gi].custom_name || g.files[state[gi].rename_from].filename; rhtml += '<span style="color:#b388ff">Rename to: </span><input type="text" class="rename-input" value="' + escHtml(cn) + '" onchange="state[' + gi + '].custom_name=this.value" onclick="event.stopPropagation()" />'; }}
      if (dirs.length > 1) {{ rhtml += (rhtml?'<br>':'') + '<span style="color:#b388ff;font-size:11px">Save in: </span><select class="dir-select" onchange="state[' + gi + '].target_dir=this.value" onclick="event.stopPropagation()">'; dirs.forEach(d => {{ rhtml += '<option value="' + escHtml(d) + '"' + (d===(state[gi].target_dir||f.dir)?' selected':'') + '>' + escHtml(d) + '</option>'; }}); rhtml += '</select>'; }}
      if (rhtml) {{ ri.innerHTML = rhtml; ri.style.display = 'block'; }}
    }} else {{ row.className = 'file-row selected-delete'; }}
  }});
}}

function updateAllRows() {{ DATA.groups.forEach((_, gi) => {{ updateGroupRows(gi); updateGroupSummary(gi); }}); DATA.clips.forEach((_, ci) => {{ updateClipRows(ci); updateClipSummary(ci); }}); }}

function updateCounters() {{
  let dels = 0, changes = 0, skips = 0, actions = 0;
  DATA.groups.forEach((g, gi) => {{ if (state[gi].action === 'skipped') {{ skips++; return; }} actions++; dels += g.files.length - 1; const kf = g.files[state[gi].keep]; if (state[gi].rename_from !== null || (state[gi].target_dir && state[gi].target_dir !== kf.dir)) changes++; }});
  DATA.clips.forEach((c, ci) => {{ const cs = clipState[ci]; if (cs.action === 'skipped') {{ skips++; return; }} actions++; dels++; const kdir = cs.keep === 'parent' ? (c.parents[0] ? c.parents[0].dir : '') : c.child_dir; if (cs.rename_to !== null || (cs.target_dir && cs.target_dir !== kdir)) changes++; }});
  document.getElementById('del-count').textContent = dels;
  document.getElementById('rename-count').textContent = changes;
  document.getElementById('skip-count').textContent = skips;
  document.getElementById('action-count').textContent = actions;
  document.getElementById('skip-count2').textContent = skips;
  const btn = document.getElementById('btn-generate');
  btn.textContent = actions > 0 ? 'Download Custom PowerShell Script (' + actions + ' actions)' : 'Download Custom PowerShell Script (no actions)';
}}

function generateScript() {{
  let lines = [];
  lines.push('# VidClipDuplis \u2014 Custom deletion/rename script');
  lines.push('# Generated from interactive review');
  lines.push('# Run: powershell -ExecutionPolicy Bypass -File custom_actions.ps1');
  lines.push('');
  lines.push('Write-Host "This script will delete and rename files as you chose." -ForegroundColor Yellow');
  lines.push('$null = Read-Host "Press Enter to continue or Ctrl+C to cancel"');
  lines.push('');
  DATA.groups.forEach((g, gi) => {{
    if (state[gi].action === 'skipped') return;
    const keepIdx = state[gi].keep; const keepFile = g.files[keepIdx];
    lines.push('# Group ' + (gi+1));
    g.files.forEach((f, fi) => {{ if (fi !== keepIdx) lines.push("Remove-Item -LiteralPath '" + f.path.replace(/'/g, "''") + "' -Force"); }});
    if (state[gi].rename_from !== null) {{
      const newName = (state[gi].custom_name || g.files[state[gi].rename_from].filename);
      const targetDir = state[gi].target_dir || keepFile.dir;
      if (targetDir !== keepFile.dir) {{ lines.push("Move-Item -LiteralPath '" + keepFile.path.replace(/'/g,"''") + "' -Destination '" + (targetDir+'\\\\'+newName).replace(/'/g,"''") + "' -Force"); }}
      else if (newName !== keepFile.filename) {{ lines.push("Rename-Item -LiteralPath '" + keepFile.path.replace(/'/g,"''") + "' -NewName '" + newName.replace(/'/g,"''") + "'"); }}
    }} else {{
      const targetDir = state[gi].target_dir || keepFile.dir;
      if (targetDir !== keepFile.dir) {{ lines.push("Move-Item -LiteralPath '" + keepFile.path.replace(/'/g,"''") + "' -Destination '" + (targetDir+'\\\\'+keepFile.filename).replace(/'/g,"''") + "' -Force"); }}
    }}
    lines.push('');
  }});
  let hasClips = false;
  DATA.clips.forEach((c, ci) => {{
    const cs = clipState[ci]; if (cs.action === 'skipped') return;
    if (!hasClips) {{ lines.push('# Redundant clips'); hasClips = true; }}
    const parent = c.parents[0] || {{}};
    let keptPath, keptDir, keptName, deletePath;
    if (cs.keep === 'parent') {{ deletePath=c.child_path; keptPath=parent.path||''; keptDir=parent.dir||''; keptName=parent.name||''; }}
    else {{ deletePath=parent.path||''; keptPath=c.child_path; keptDir=c.child_dir; keptName=c.child_name; }}
    if (deletePath) lines.push("Remove-Item -LiteralPath '" + deletePath.replace(/'/g,"''") + "' -Force");
    const hasRename = cs.rename_to !== null;
    const finalName = hasRename ? (cs.custom_name || (cs.keep==='parent' ? c.child_name : (parent.name||''))) : keptName;
    const targetDir = cs.target_dir || keptDir;
    if (targetDir !== keptDir && keptPath) {{ lines.push("Move-Item -LiteralPath '" + keptPath.replace(/'/g,"''") + "' -Destination '" + (targetDir+'\\\\'+finalName).replace(/'/g,"''") + "' -Force"); }}
    else if (hasRename && finalName !== keptName && keptPath) {{ lines.push("Rename-Item -LiteralPath '" + keptPath.replace(/'/g,"''") + "' -NewName '" + finalName.replace(/'/g,"''") + "'"); }}
    lines.push('');
  }});
  lines.push(''); lines.push('Write-Host "Done!" -ForegroundColor Green');
  const blob = new Blob(['\\ufeff' + lines.join('\\r\\n')], {{ type: 'text/plain' }});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'custom_actions.ps1'; a.click(); URL.revokeObjectURL(a.href);
}}

function saveDismissed() {{
  const pairs = [];
  DATA.groups.forEach((g, gi) => {{
    if (state[gi].action === 'skipped' && g.content_keys) {{
      const keys = g.content_keys;
      if (keys.length >= 2) {{ for (let i=0;i<keys.length;i++) for (let j=i+1;j<keys.length;j++) {{ const a=keys[i]<=keys[j]?keys[i]:keys[j], b=keys[i]<=keys[j]?keys[j]:keys[i]; pairs.push([a,b]); }} }}
      else if (keys.length === 1) pairs.push([keys[0], keys[0]]);
    }}
  }});
  DATA.clips.forEach((c, ci) => {{
    if (clipState[ci].action === 'skipped' && c.ck_child) {{
      (c.ck_parents || []).forEach(ckp => {{ if (ckp) {{ const a=c.ck_child<=ckp?c.ck_child:ckp, b=c.ck_child<=ckp?ckp:c.ck_child; pairs.push([a,b]); }} }});
    }}
  }});
  const seen = new Set();
  const unique = pairs.filter(p => {{ const k=p[0]+':'+p[1]; if (seen.has(k)) return false; seen.add(k); return true; }});
  const data = JSON.stringify({{ dismissed_pairs: unique }}, null, 2);
  const blob = new Blob([data], {{ type: 'application/json' }});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'vcd_dismissed.json'; a.click(); URL.revokeObjectURL(a.href);
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
    """Scan directory for media files. Returns list of (path, size, mtime).
    
    Uses os.walk instead of pathlib.rglob for significantly faster NAS scanning.
    """
    files, seen = [], set()
    for dirpath, _, filenames in os.walk(root_dir):
        for name in filenames:
            ext = os.path.splitext(name)[1].lower()
            if ext not in MEDIA_EXTENSIONS:
                continue
            p = os.path.join(dirpath, name)
            key = p.lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                st = os.stat(p)
                files.append((p, st.st_size, st.st_mtime))
            except OSError:
                pass
    return files

# ============================================================================
# MAIN
# ============================================================================

def main():
    global SHUTDOWN_REQUESTED, _ARRAYS_TEMP_PATH, _debug_logger, FFMPEG_PATH
    signal.signal(signal.SIGINT, signal_handler)

    # Prevent multiple instances from corrupting the database
    if not acquire_instance_lock():
        print("🛑 Another instance of VidClipDuplis is already running.")
        print("   Wait for it to finish, or close it before starting a new scan.")
        sys.exit(1)

    if not os.path.exists(FPCALC_PATH):
        print(f"❌ CRITICAL: {FPCALC_PATH} not found.")
        print("   Download from: https://acoustid.org/chromaprint")
        sys.exit(1)

    FFMPEG_PATH = _find_ffmpeg()

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
    parser.add_argument('--cleanup-cache', action='store_true', help='Remove cached data for files no longer in scanned dirs')
    parser.add_argument('--clear-dismissed', action='store_true', help='Re-show previously skipped groups')
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
    print("  Cache survives file moves and renames!")
    print("  Works on any CPU (Intel, AMD, ARM). No GPU needed.")

    # Resolve directories
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
            for part in dir_input.replace(';', ',').split(','):
                part = part.strip().strip('"').strip("'")
                if part:
                    root_dirs.append(os.path.abspath(part))

    for d in root_dirs:
        if not os.path.isdir(d):
            print(f"❌ Invalid directory: {d}")
            sys.exit(1)

    # Configuration
    try:
        cpu_count = multiprocessing.cpu_count()
    except (NotImplementedError, OSError):
        cpu_count = 4

    script_dir = _BASE_DIR
    cache_path = os.path.join(script_dir, '.audio_cache.db')
    cache = UnifiedCache(cache_path)

    # Handle CLI cache clearing
    if args.clear_cache:
        cache.clear_fingerprints()
        cache.clear_comparisons()
        cache.clear_failed()
        cache.clear_dismissed()
        print("🗑️  All caches cleared")
    if args.clear_comparisons:
        cache.clear_comparisons()
        print("🗑️  Comparison cache cleared")
    if args.clear_failed:
        cache.clear_failed()
        print("🗑️  Failed list cleared")
    if args.clear_dismissed:
        cache.clear_dismissed()
        print("🗑️  Dismissed groups cleared — all groups will appear in next report")

    # Auto-import dismissed pairs from HTML report (vcd_dismissed.json)
    dismissed_json_path = os.path.join(script_dir, 'vcd_dismissed.json')
    if os.path.exists(dismissed_json_path):
        try:
            with open(dismissed_json_path, 'r', encoding='utf-8') as f:
                dismissed_data = json.load(f)
            pairs = [(p[0], p[1]) for p in dismissed_data.get('dismissed_pairs', []) if len(p) == 2]
            if pairs:
                cache.batch_add_dismissed(pairs)
                print(f"📥 Imported {len(pairs)} dismissed pairs from vcd_dismissed.json")
            os.remove(dismissed_json_path)
        except (json.JSONDecodeError, OSError, KeyError) as e:
            print(f"⚠️  Could not import vcd_dismissed.json: {e}")

    has_cli_config = (args.workers > 0 or args.compare_workers > 0 or args.clip_ratio > 0
                      or args.dup_ratio > 0 or args.intro_filter >= 0 or args.no_prompt)

    do_cleanup = args.cleanup_cache

    if has_cli_config:
        config = Config(video_timeout=args.timeout)
        if args.workers > 0:
            config.max_workers = args.workers
        if args.compare_workers > 0:
            config.comparison_workers = args.compare_workers
        else:
            config.comparison_workers = max(1, min(cpu_count - 1, int(cpu_count * 0.75)))
        if args.clip_ratio > 0:
            config.clip_match_ratio = max(0.01, min(1.0, args.clip_ratio))
        if args.dup_ratio > 0:
            config.duplicate_match_ratio = max(0.01, min(1.0, args.dup_ratio))
        if args.intro_filter >= 0:
            config.intro_filter_seconds = args.intro_filter
    else:
        config, interactive_cleanup = interactive_setup(cpu_count, cache)
        do_cleanup = do_cleanup or interactive_cleanup

    cache.validate_params(config)

    print(f"\n⚙️  Settings:")
    print(f"   Extraction workers:  {config.max_workers}")
    print(f"   Comparison workers:  {config.comparison_workers}")
    print(f"   Clip threshold:      {config.clip_match_ratio:.0%}")
    print(f"   Duplicate threshold: {config.duplicate_match_ratio:.0%}")
    print(f"   Intro filter:        {config.intro_filter_seconds}s")
    print(f"   Cache: {cache_path}")
    if FFMPEG_PATH:
        print(f"   ffmpeg: {FFMPEG_PATH} (fallback for any fpcalc failure)")
    else:
        print(f"   ffmpeg: not found (no fallback for fpcalc failures)")

    # Phase 0: Scan files
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
        print("   ❌ No media files found")
        sys.exit(0)

    total_size = sum(s for _, s, _ in media_files)
    print(f"   Found {len(media_files)} files ({format_size(total_size)})")

    # Phase 0.5: Compute content keys and build content_key -> [paths] mapping
    # THIS MAPPING IS THE SOURCE OF TRUTH — not the database!
    print(f"\n🔑 Computing content keys...")
    key_to_paths: Dict[str, List[Tuple[str, int, float]]] = defaultdict(list)  # content_key -> [(path, size, mtime), ...]
    hash_errors = []  # (path, error_reason) for logging
    
    for path, size, mtime in tqdm(media_files, unit="file", desc="   Hashing",
                                    dynamic_ncols=False, smoothing=0.05,
                                    bar_format="   Hashing: {n_fmt}/{total_fmt} files [{bar:25}] {percentage:.0f}% | {rate_fmt} | Elapsed: {elapsed} | ETA: {remaining}"):
        content_key, hash_err = get_quick_hash(path)
        if content_key:
            key_to_paths[content_key].append((path, size, mtime))
        else:
            hash_errors.append((path, hash_err))
    
    if hash_errors:
        print(f"   ⚠️  {len(hash_errors)} files could not be hashed:")
        for failed_path, reason in hash_errors[:5]:
            print(f"      • {os.path.basename(failed_path)}: {reason[:60]}")
        if len(hash_errors) > 5:
            print(f"      ... and {len(hash_errors) - 5} more (see debug log)")
    
    # Count byte-for-byte identical files (same content_key, multiple paths)
    identical_groups = [(ck, paths) for ck, paths in key_to_paths.items() if len(paths) > 1]
    unique_keys = len(key_to_paths)
    total_with_keys = sum(len(paths) for paths in key_to_paths.values())
    
    print(f"   ✓ {total_with_keys} files → {unique_keys} unique content keys")
    if identical_groups:
        identical_file_count = sum(len(paths) for _, paths in identical_groups)
        print(f"   🔥 {len(identical_groups)} groups of byte-for-byte identical files ({identical_file_count} files)")
        print(f"      These will be reported as duplicates WITHOUT fingerprint comparison!")

    # Cache cleanup: remove orphaned data for files no longer in scanned directories
    if do_cleanup:
        if hash_errors:
            print(f"\n⚠️  Skipping cache cleanup — {len(hash_errors)} files could not be hashed")
            print(f"   (locked/inaccessible files would be wrongly treated as deleted)")
        else:
            print(f"\n🧹 Cleaning up orphaned cache entries...")
            print(f"   Scoped to: {', '.join(root_dirs)}")
            print(f"   DB size before: {format_size(cache.get_db_size())}")
            active_keys = set(key_to_paths.keys())
            removed = cache.cleanup_orphans(active_keys, root_dirs)
            total_removed = sum(removed.values())
            if total_removed > 0:
                print(f"   Removed: {removed['fingerprints']:,} fingerprints, {removed['comparisons']:,} comparisons, {removed['failed']:,} failed entries")
                print(f"   DB size after:  {format_size(cache.get_db_size())}")
            else:
                print(f"   No orphaned entries found — cache is clean")

    # Load cached fingerprints
    all_cached_fps = cache.get_all_fingerprints()  # keyed by content_key
    
    # Determine what needs processing
    # We only need ONE fingerprint per content_key (since identical files have identical audio)
    to_process = []  # (path, content_key, size, mtime) - one representative per content_key
    fingerprints = {}  # content_key -> fingerprint data
    skipped_failed = 0
    path_updates = []  # (content_key, new_path) for files that moved
    
    for content_key, paths in key_to_paths.items():
        # Pick one representative path for this content_key
        rep_path, rep_size, rep_mtime = paths[0]
        
        # Check if this content_key is marked as failed
        if cache.is_failed(content_key):
            skipped_failed += len(paths)  # All paths with this key are effectively failed
            continue
        
        # Check if we have a cached fingerprint for this content_key
        cached = all_cached_fps.get(content_key)
        if cached and cached.get('fingerprint') is not None:
            # Validate that file_size still matches (sanity check)
            if cached['file_size'] == rep_size:
                fingerprints[content_key] = {
                    'content_key': content_key,
                    'file_size': rep_size,
                    'duration': cached['duration'],
                    'fingerprint': cached['fingerprint']
                }
                # Track if path changed (file was moved/renamed)
                if cached.get('current_path') != rep_path:
                    path_updates.append((content_key, rep_path))
            else:
                # File size changed — content changed, need to re-extract
                to_process.append((rep_path, content_key, rep_size, rep_mtime))
        else:
            # No cached fingerprint
            to_process.append((rep_path, content_key, rep_size, rep_mtime))
    
    # Batch update paths for moved files
    if path_updates:
        print(f"   📁 {len(path_updates)} files moved/renamed — updating cache paths...")
        for content_key, new_path in path_updates:
            cache.update_path(content_key, new_path)

    print(f"\n📦 Status:")
    print(f"   Unique content keys: {unique_keys}")
    print(f"   Fingerprints cached: {len(fingerprints)}")
    print(f"   To process: {len(to_process)}")
    if skipped_failed:
        print(f"   Skipped failed: {skipped_failed}")

    # Set up results dir and debug log
    results_dir = get_results_folder_path(script_dir, root_dirs)
    if to_process or hash_errors:
        os.makedirs(results_dir, exist_ok=True)
        _debug_logger = setup_debug_logger(results_dir)
        
        # Log hash errors to debug file
        if hash_errors and _debug_logger:
            for failed_path, reason in hash_errors:
                _debug_logger.debug(f"HASH_FAIL | {reason}")

    # Phase 1: Extraction (only for unique content_keys that aren't cached)
    if to_process and not SHUTDOWN_REQUESTED:
        print(f"\n🎵 Phase 1: Extracting audio fingerprints ({len(to_process)} unique files)...")
        print(f"   ⌨️  Press Ctrl+C once to stop gracefully, twice to force quit.")
        
        extraction_args = [(p, ck, s, config.video_timeout) for p, ck, s, m in to_process]
        pbar = tqdm(total=len(to_process), unit="file", dynamic_ncols=False, smoothing=0.02,
                   bar_format="   Extracted: {n_fmt}/{total_fmt} files [{bar:25}] {percentage:.0f}% | {rate_fmt} | Elapsed: {elapsed} | ETA: {remaining}")
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(extract_audio_fingerprint, item): item for item in extraction_args}
            for future in as_completed(futures):
                if SHUTDOWN_REQUESTED:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    path, content_key, size, arr, duration, err, stderr_raw = future.result()
                    if arr is not None:
                        fingerprints[content_key] = {
                            'content_key': content_key,
                            'file_size': size,
                            'duration': duration,
                            'fingerprint': arr
                        }
                        cache.set_fingerprint(content_key, path, arr, size, duration)
                    else:
                        cache.mark_failed(content_key, path, err)
                        failed_count += 1
                        tqdm.write(f"   ❌ {err} — {os.path.basename(path)}")
                        if _debug_logger:
                            ext = os.path.splitext(path)[1].lower()
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

    # Build hash_arrays keyed by content_key for comparison
    # Only include content_keys that have valid fingerprints
    hash_arrays = {}  # content_key -> numpy array
    for content_key, fp_data in fingerprints.items():
        arr = fp_data.get('fingerprint')
        if arr is not None and len(arr) > 0:
            hash_arrays[content_key] = arr

    # Start building results
    # First: handle byte-for-byte identical files (no comparison needed!)
    dup_matches_identical = []  # These are 100% certain duplicates
    
    for content_key, paths in identical_groups:
        if content_key not in hash_arrays:
            continue  # Skip if we don't have a fingerprint
        # All paths with the same content_key are exact duplicates (100% match)
        for i in range(len(paths)):
            for j in range(i+1, len(paths)):
                path1 = paths[i][0]
                path2 = paths[j][0]
                dup_matches_identical.append((path1, path2, 1.0))  # 100% match

    # Get content_keys that need fingerprint comparison
    # ALL keys with valid fingerprints are compared against each other.
    # Byte-identical files (same content_key) are already handled above —
    # but their content_key still needs comparison against OTHER keys
    # to discover clip relationships (e.g., 3 copies of a trailer + full movie).
    content_keys_for_comparison = sorted(hash_arrays.keys())
    
    total_pairs = len(content_keys_for_comparison) * (len(content_keys_for_comparison) - 1) // 2
    
    print(f"\n📊 Fingerprint comparison:")
    print(f"   Total fingerprints: {len(hash_arrays)}")
    print(f"   Byte-identical groups: {len(identical_groups)} (already grouped)")
    print(f"   Content keys to cross-compare: {len(content_keys_for_comparison)}")
    print(f"   Comparison pairs: {total_pairs:,}")

    # Generate all pairs (using content_keys) for files that aren't already identical
    # Pre-filter: skip any pair where the shorter file can NEVER pass the intro filter
    # This is safe and does NOT violate the "no early exit inside compare_audio_pair" rule
    min_matched_seconds_needed = config.intro_filter_seconds
    min_chunks_needed = int(min_matched_seconds_needed / (config.chunk_size / 6.0)) + 1
    min_samples_needed = min_chunks_needed * config.chunk_size

    all_pairs = []
    skipped_by_filter = 0

    for i in range(len(content_keys_for_comparison)):
        for j in range(i + 1, len(content_keys_for_comparison)):
            k1 = content_keys_for_comparison[i]
            k2 = content_keys_for_comparison[j]
            shorter_len = min(len(hash_arrays[k1]), len(hash_arrays[k2]))
            
            if shorter_len < min_samples_needed:
                skipped_by_filter += 1
                continue  # Guaranteed to fail classify_comparison
            
            # Keep normalized order for cache lookup
            a, b = (k1, k2) if k1 <= k2 else (k2, k1)
            all_pairs.append((a, b))

    print(f"   Pre-filtered: {skipped_by_filter:,} impossible pairs (shorter than intro filter)")
    print(f"   Remaining pairs to evaluate: {len(all_pairs):,}")

    # Phase 2: Cache lookup
    dup_matches = list(dup_matches_identical)  # Start with identical file matches
    clip_matches = []
    
    if all_pairs:
        print(f"\n🔍 Phase 2: Cache lookup...")
        t0 = time.time()
        cached_comparisons = cache.get_many_comparisons(all_pairs)
        print(f"   Found {len(cached_comparisons):,} cached ({time.time()-t0:.1f}s)")

        pairs_to_compute = [p for p in all_pairs if p not in cached_comparisons]
        cached_count = len(cached_comparisons)

        # Process cached comparisons - convert content_key matches to path matches
        # For content_keys with multiple paths (identical copies), we must emit
        # matches for ALL paths so clip relationships aren't lost when one copy
        # ends up in dup_delete_set and another in keep_set.
        for (k1, k2), (mr, lr, ms) in cached_comparisons.items():
            is_dup, is_clip = classify_comparison(mr, lr, ms, config)
            paths1 = key_to_paths.get(k1, [])
            paths2 = key_to_paths.get(k2, [])
            if paths1 and paths2:
                if is_dup:
                    # Union-Find is transitive: one pair per key suffices
                    dup_matches.append((paths1[0][0], paths2[0][0], mr))
                elif is_clip:
                    # Clips are directed edges — every path needs its own entry
                    for pt1 in paths1:
                        for pt2 in paths2:
                            clip_matches.append((pt1[0], pt2[0], mr))

        print(f"   Cached: {cached_count:,}, To compute: {len(pairs_to_compute):,}")
    else:
        pairs_to_compute = []
        cached_count = 0

    if dup_matches or clip_matches:
        print(f"   So far: {len(dup_matches)} duplicates, {len(clip_matches)} clips")

    # Phase 3: Comparison
    new_comparisons = 0
    if pairs_to_compute and not SHUTDOWN_REQUESTED:
        print(f"\n⚡ Phase 3: Computing {len(pairs_to_compute):,} comparisons ({config.comparison_workers} workers)...")
        print(f"   ⌨️  Press Ctrl+C once to stop gracefully, twice to force quit.")
        
        _ARRAYS_TEMP_PATH = save_arrays_for_workers(hash_arrays)
        print(f"   Arrays temp file: {os.path.getsize(_ARRAYS_TEMP_PATH)/(1024*1024):.0f} MB")

        batches = [pairs_to_compute[i:i+config.comparison_batch_size] 
                   for i in range(0, len(pairs_to_compute), config.comparison_batch_size)]
        pbar = tqdm(total=len(pairs_to_compute), unit="pair", dynamic_ncols=False, smoothing=0.02,
                   bar_format="   Compared: {n_fmt}/{total_fmt} pairs [{bar:25}] {percentage:.0f}% | {rate_fmt} | Elapsed: {elapsed} | ETA: {remaining}")
        
        try:
            with ProcessPoolExecutor(max_workers=config.comparison_workers, 
                                     initializer=init_worker, initargs=(_ARRAYS_TEMP_PATH,)) as executor:
                future_to_size = {}
                for batch in batches:
                    if SHUTDOWN_REQUESTED:
                        break
                    future_to_size[executor.submit(compare_batch, batch, config)] = len(batch)

                batch_buf = []
                for future in as_completed(future_to_size):
                    if SHUTDOWN_REQUESTED:
                        for f in future_to_size:
                            f.cancel()
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    try:
                        res = future.result()
                        batch_buf.extend(res)
                        for k1, k2, mr, lr, ms in res:
                            new_comparisons += 1
                            is_dup, is_clip = classify_comparison(mr, lr, ms, config)
                            paths1 = key_to_paths.get(k1, [])
                            paths2 = key_to_paths.get(k2, [])
                            if paths1 and paths2:
                                if is_dup:
                                    dup_matches.append((paths1[0][0], paths2[0][0], mr))
                                    tqdm.write(f"   ✅ DUP ({mr*100:.1f}%): {os.path.basename(paths1[0][0])} ↔ {os.path.basename(paths2[0][0])}")
                                elif is_clip:
                                    for pt1 in paths1:
                                        for pt2 in paths2:
                                            clip_matches.append((pt1[0], pt2[0], mr))
                                    # Show direction: which is clip (shorter) vs parent (longer)
                                    l1 = len(hash_arrays.get(k1, []))
                                    l2 = len(hash_arrays.get(k2, []))
                                    if l1 <= l2:
                                        cname, pname = os.path.basename(paths1[0][0]), os.path.basename(paths2[0][0])
                                    else:
                                        cname, pname = os.path.basename(paths2[0][0]), os.path.basename(paths1[0][0])
                                    tqdm.write(f"   📎 CLIP ({mr*100:.1f}%): {cname} ⊂ {pname}")
                        pbar.update(future_to_size[future])
                        if len(batch_buf) >= 5000:
                            cache.batch_set_comparisons(batch_buf)
                            batch_buf = []
                    except Exception as e:
                        tqdm.write(f"   ⚠️  Batch error: {type(e).__name__}: {str(e)[:80]}")
                        pbar.update(future_to_size[future])
                
                if batch_buf:
                    cache.batch_set_comparisons(batch_buf)
            pbar.close()
            cache.wal_checkpoint()
        finally:
            cleanup_arrays_file()

    partial_results = SHUTDOWN_REQUESTED

    print(f"\n✓ {len(dup_matches)+len(clip_matches)} matches ({len(dup_matches)} dups, {len(clip_matches)} clips)")
    if len(dup_matches_identical) > 0:
        print(f"   ({len(dup_matches_identical)} byte-identical, {cached_count:,} cached, {new_comparisons:,} computed)")
    else:
        print(f"   ({cached_count:,} cached, {new_comparisons:,} computed)")

    # Phase 4: Safe grouping
    print("\n📋 Phase 4: Grouping results...")
    
    # Load dismissed pairs for previously-skipped detection
    dismissed_pairs = cache.get_all_dismissed()
    
    # Build path-based fingerprints dict for reporting
    # Map each path to its fingerprint data (via content_key)
    path_fingerprints = {}
    for content_key, paths in key_to_paths.items():
        fp_data = fingerprints.get(content_key)
        if fp_data:
            for path, size, mtime in paths:
                path_fingerprints[path] = {
                    'content_key': content_key,
                    'file_size': size,
                    'duration': fp_data.get('duration', 0),
                }
    
    # Union-Find for exact duplicates only
    uf = UnionFind()
    for p1, p2, _ in dup_matches:
        uf.union(p1, p2)

    # Get all paths that have fingerprints
    all_paths = list(path_fingerprints.keys())
    
    dup_groups_raw = defaultdict(list)
    for path in all_paths:
        root = uf.find(path)
        dup_groups_raw[root].append(path)

    dup_groups, keep_set, dup_delete_set = [], set(), set()
    prev_skipped_dup = 0
    for gp in dup_groups_raw.values():
        if len(gp) < 2:
            keep_set.add(gp[0])
            continue
        sg = sorted(gp, key=lambda p: (
            path_fingerprints[p]['file_size'], 
            path_fingerprints[p].get('duration', 0)
        ), reverse=True)
        keep_set.add(sg[0])
        dup_delete_set.update(sg[1:])
        sizes = [path_fingerprints[p]['file_size'] for p in sg]
        
        # Check if this group is byte-for-byte identical
        content_keys_in_group = list(set(path_fingerprints[p]['content_key'] for p in sg))
        is_identical = len(content_keys_in_group) == 1
        
        # Check if ALL content_key pairs in this group were previously dismissed
        all_dismissed = True
        if len(content_keys_in_group) >= 2:
            for ii in range(len(content_keys_in_group)):
                for jj in range(ii + 1, len(content_keys_in_group)):
                    nk = (content_keys_in_group[ii], content_keys_in_group[jj]) if content_keys_in_group[ii] <= content_keys_in_group[jj] else (content_keys_in_group[jj], content_keys_in_group[ii])
                    if nk not in dismissed_pairs:
                        all_dismissed = False
                        break
                if not all_dismissed:
                    break
        else:
            # Identical files (1 content_key) — check if the single "self-pair" was dismissed
            sk = content_keys_in_group[0]
            all_dismissed = (sk, sk) in dismissed_pairs
        
        if all_dismissed:
            prev_skipped_dup += 1
        
        dup_groups.append({
            'recommend_keep': sg[0],
            'recommend_delete': sg[1:],
            'potential_savings': sum(path_fingerprints[p]['file_size'] for p in sg[1:]),
            'size_warning': (max(sizes) / max(min(sizes), 1)) > 3.0 if not is_identical else False,
            'identical': is_identical,
            'content_keys': content_keys_in_group,
            'previously_skipped': all_dismissed,
            'videos': [
                {
                    'path': p, 
                    'size': path_fingerprints[p]['file_size'], 
                    'duration': path_fingerprints[p].get('duration', 0)
                } 
                for p in sg
            ],
        })
    dup_groups.sort(key=lambda g: (g['previously_skipped'], -g['potential_savings']))

    # Clip relationships
    clip_children = defaultdict(list)
    for p1, p2, mr in clip_matches:
        # Determine parent/child by fingerprint length
        ck1 = path_fingerprints.get(p1, {}).get('content_key')
        ck2 = path_fingerprints.get(p2, {}).get('content_key')
        l1 = len(hash_arrays.get(ck1, [])) if ck1 else 0
        l2 = len(hash_arrays.get(ck2, [])) if ck2 else 0
        child, parent = (p1, p2) if l1 <= l2 else (p2, p1)
        clip_children[child].append((parent, mr))

    clip_deletions = []
    for child, parents in clip_children.items():
        if child in dup_delete_set:
            continue
        if child in keep_set and any(child == g['recommend_keep'] for g in dup_groups):
            continue
        kept = [(par, r) for par, r in parents if par in keep_set]
        if kept:
            clip_deletions.append((child, [p for p, _ in kept], max(r for _, r in kept)))
    clip_deletions.sort(key=lambda x: path_fingerprints[x[0]]['file_size'], reverse=True)

    # Build dismissed info for clips
    clip_dismissed = []
    clip_content_keys = []
    for child, parents, ratio in clip_deletions:
        ck_child = path_fingerprints.get(child, {}).get('content_key', '')
        ck_parents = [path_fingerprints.get(p, {}).get('content_key', '') for p in parents]
        is_dismissed = all(
            ((ck_child, ckp) if ck_child <= ckp else (ckp, ck_child)) in dismissed_pairs
            for ckp in ck_parents if ckp
        ) if ck_child and ck_parents else False
        clip_dismissed.append(is_dismissed)
        clip_content_keys.append((ck_child, ck_parents))
    
    prev_skipped_groups = sum(1 for g in dup_groups if g.get('previously_skipped'))
    prev_skipped_clips = sum(1 for d in clip_dismissed if d)
    if prev_skipped_groups or prev_skipped_clips:
        print(f"   Previously skipped: {prev_skipped_groups} dup groups, {prev_skipped_clips} clips (hidden by default)")

    # Output
    display_root = root_dirs[0]
    print("\n" + "=" * 60)
    if partial_results:
        print("  PARTIAL RESULTS (interrupted)")
    else:
        print("  RESULTS")
    print("=" * 60)
    print("\n   ℹ️  Audio-only matching. Videos with the same background")
    print("   music will match. Review results before deleting.")
    if partial_results:
        print("\n   ⚠️  Scan was interrupted — results are incomplete.")
        print("   Run again to compare remaining pairs. Cached progress is preserved.")

    if not dup_groups and not clip_deletions:
        if partial_results:
            print("\n⏳ No matches found yet in partial scan.")
            print("   Run again to continue comparing — cached progress is preserved.")
        else:
            print("\n✨ No duplicates or clips found!")
        return

    if dup_groups:
        identical_count = sum(1 for g in dup_groups if g.get('identical'))
        print(f"\n📋 EXACT DUPLICATES: {len(dup_groups)} groups")
        if identical_count:
            print(f"   ({identical_count} groups are byte-for-byte identical files)")
        print(f"   Delete: {sum(len(g['recommend_delete']) for g in dup_groups)} files")
        print(f"   Savings: {format_size(sum(g['potential_savings'] for g in dup_groups))}")
        for i, g in enumerate(dup_groups[:15], 1):
            warn = " ⚠️ SIZE MISMATCH" if g.get('size_warning') else ""
            ident = " 🔥 IDENTICAL" if g.get('identical') else ""
            print(f"\n{'─'*50}")
            print(f"Dup Group {i} ({len(g['videos'])} files, save {format_size(g['potential_savings'])}){ident}{warn}")
            if g.get('size_warning'):
                print(f"   ⚠️  Files differ >3x in size — likely shared audio!")
            if g.get('identical'):
                print(f"   🔥  Byte-for-byte identical copies!")
            for v in g['videos']:
                marker = "✓ KEEP  " if v['path'] == g['recommend_keep'] else "✗ DELETE"
                print(f"   {marker} {_safe_relpath(v['path'], display_root)}")
                print(f"            {format_size(v['size'])}, {format_duration(v['duration']) if v['duration'] else '?'}")
        if len(dup_groups) > 15:
            print(f"\n   ... and {len(dup_groups)-15} more groups")

    if clip_deletions:
        clip_savings = sum(path_fingerprints[c]['file_size'] for c, _, _ in clip_deletions)
        print(f"\n📎 REDUNDANT CLIPS: {len(clip_deletions)} files, {format_size(clip_savings)}")
        for i, (child, parents, ratio) in enumerate(clip_deletions[:15], 1):
            pnames = ", ".join(os.path.basename(p) for p in parents[:3])
            if len(parents) > 3:
                pnames += f" (+{len(parents)-3})"
            print(f"\n{'─'*50}")
            print(f"Clip {i} ({ratio*100:.0f}% match)")
            print(f"   ✗ DELETE  {_safe_relpath(child, display_root)}")
            print(f"             {format_size(path_fingerprints[child]['file_size'])}, {format_duration(path_fingerprints[child].get('duration', 0))}")
            print(f"   ↳ Contained in: {pnames}")
        if len(clip_deletions) > 15:
            print(f"\n   ... and {len(clip_deletions)-15} more clips")

    # Save results
    os.makedirs(results_dir, exist_ok=True)
    total_delete = sum(len(g['recommend_delete']) for g in dup_groups) + len(clip_deletions)
    total_savings = (
        sum(g['potential_savings'] for g in dup_groups) + 
        sum(path_fingerprints[c]['file_size'] for c, _, _ in clip_deletions)
    )

    # JSON report
    report = {
        'scan_info': {
            'directories': root_dirs, 
            'total_files': len(media_files), 
            'fingerprinted': len(path_fingerprints),
            'failed': cache.get_failed_count(), 
            'scan_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'config': {k: getattr(config, k) for k in ['clip_match_ratio', 'duplicate_match_ratio', 'intro_filter_seconds', 'match_threshold', 'chunk_size']},
            'partial': partial_results
        },
        'summary': {
            'duplicate_groups': len(dup_groups), 
            'identical_groups': sum(1 for g in dup_groups if g.get('identical')),
            'redundant_clips': len(clip_deletions),
            'files_to_delete': total_delete, 
            'potential_savings': format_size(total_savings)
        },
        'duplicate_groups': [
            {
                'group_id': i, 
                'file_count': len(g['videos']), 
                'potential_savings': format_size(g['potential_savings']),
                'size_warning': g.get('size_warning', False),
                'identical': g.get('identical', False),
                'recommend_keep': _safe_relpath(g['recommend_keep'], display_root),
                'recommend_delete': [_safe_relpath(p, display_root) for p in g['recommend_delete']],
                'files': [
                    {
                        'path': _safe_relpath(v['path'], display_root), 
                        'size': format_size(v['size']),
                        'duration': format_duration(v['duration']) if v['duration'] else '?',
                        'action': 'KEEP' if v['path'] == g['recommend_keep'] else 'DELETE'
                    } 
                    for v in g['videos']
                ]
            }
            for i, g in enumerate(dup_groups, 1)
        ],
        'redundant_clips': [
            {
                'clip': _safe_relpath(c, display_root), 
                'clip_size': format_size(path_fingerprints[c]['file_size']),
                'clip_duration': format_duration(path_fingerprints[c].get('duration', 0)), 
                'match_ratio': f"{r*100:.1f}%",
                'contained_in': [_safe_relpath(p, display_root) for p in ps]
            } 
            for c, ps, r in clip_deletions
        ],
    }
    with open(os.path.join(results_dir, 'duplicate_report.json'), 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # .bat file
    with open(os.path.join(results_dir, 'delete_duplicates.bat'), 'w', encoding='utf-8') as f:
        f.write('@echo off\nchcp 65001 > nul\n')
        f.write(f'REM {total_delete} files, {format_size(total_savings)} potential savings\n')
        f.write('echo WARNING: This will PERMANENTLY delete files!\npause\n\n')
        for g in dup_groups:
            f.write(f'REM Keep: {_safe_relpath(g["recommend_keep"], display_root)}\n')
            if g.get('size_warning'):
                f.write('REM SIZE MISMATCH — uncomment only if verified:\n')
                for p in g['recommend_delete']:
                    f.write(f'REM del "{_bat_safe_path(p)}"\n')
            else:
                for p in g['recommend_delete']:
                    f.write(f'del "{_bat_safe_path(p)}"\n')
            f.write('\n')
        for child, parents, ratio in clip_deletions:
            f.write(f'REM Clip ({ratio*100:.0f}%% match) of {os.path.basename(parents[0])}\n')
            f.write(f'del "{_bat_safe_path(child)}"\n\n')
        f.write('echo Done!\npause\n')

    # .ps1 file — MUST use utf-8-sig (BOM) so PowerShell correctly reads Unicode paths
    with open(os.path.join(results_dir, 'delete_duplicates.ps1'), 'w', encoding='utf-8-sig') as f:
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

    # PowerShell launcher — bypasses ExecutionPolicy without requiring system changes
    with open(os.path.join(results_dir, 'RUN_CUSTOM_ACTIONS.bat'), 'w', encoding='utf-8') as f:
        f.write('@echo off\n')
        f.write('chcp 65001 > nul\n')
        f.write('echo This will run your custom PowerShell script (custom_actions.ps1).\n')
        f.write('echo Save the downloaded .ps1 from the HTML report into this folder first.\n')
        f.write('echo.\n')
        f.write('if not exist "%~dp0custom_actions.ps1" (\n')
        f.write('    echo ERROR: custom_actions.ps1 not found in this folder.\n')
        f.write('    echo Download it from review_results.html first, then save it here.\n')
        f.write('    pause\n')
        f.write('    exit /b 1\n')
        f.write(')\n')
        f.write('pause\n')
        f.write('PowerShell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0custom_actions.ps1"\n')
        f.write('pause\n')

    # Interactive HTML report
    _generate_html_report(results_dir, dup_groups, clip_deletions, clip_dismissed, clip_content_keys, path_fingerprints, display_root, total_delete, total_savings, partial=partial_results)

    print(f"\n📄 Results saved to: {results_dir}")
    print("   - review_results.html     (interactive — open in browser)")
    print("   - duplicate_report.json")
    print("   - delete_duplicates.bat   (cmd.exe)")
    print("   - delete_duplicates.ps1   (PowerShell — handles long paths)")
    print("   - RUN_CUSTOM_ACTIONS.bat       (launches PowerShell with bypass)")
    if _debug_logger and _debug_logger.handlers:
        print("   - fpcalc_debug.log        (anonymized failure details)")
    print(f"\n   Total: {total_delete} files, {format_size(total_savings)} savings")
    if partial_results:
        print("\n⚠️  PARTIAL RESULTS — scan was interrupted. Run again to continue.")
        print("   Cached progress is preserved. Open review_results.html to see matches found so far.")
    else:
        print("\n✨ Done! Open review_results.html in your browser to review.")

if __name__ == '__main__':
    main()
