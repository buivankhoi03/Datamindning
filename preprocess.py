#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
preprocess.py  –  Tiền xử lý & làm sạch dữ liệu báo tiếng Việt
=================================================================
⚠️  CHỈ ĐỌC file gốc, KHÔNG BAO GIỜ ghi đè lên file gốc.
    Kết quả luôn được ghi ra file MỚI (cleaned_data.jsonl).
    File gốc giữ nguyên 100% để có thể chạy lại bất kỳ lúc nào.

Input : raw_samples_30000_final_20260309.jsonl   (dữ liệu thô — giữ nguyên)
Output: cleaned_data.jsonl                       (bản ghi sạch, reindex id)
        cleaning_report.txt                      (báo cáo thống kê)

Các bước xử lý:
  1. Loại bản ghi thiếu title HOẶC content (bắt buộc phải có cả hai)
  2. Loại bản ghi trùng URL (giữ bản đầu tiên)
  3. Chuẩn hoá Unicode (NFC), xoá ký tự thay thế U+FFFD / null
  4. Gộp khoảng trắng thừa, strip đầu/cuối
  5. Loại bài có content quá ngắn (< 200 ký tự) hoặc quá dài (> 50 000 ký tự)
  6. Loại bài trùng nội dung (hash content, giữ bản đầu)
  7. Reindex id liên tục từ 1
"""

import json
import re
import hashlib
import unicodedata
from pathlib import Path
from collections import Counter

# ──────────────── cấu hình ────────────────
INPUT  = "raw_samples_30000_final_20260309.jsonl"   # CHỈ ĐỌC – không ghi đè
OUTPUT = "cleaned_data.jsonl"                       # file mới
REPORT = "cleaning_report.txt"

MIN_CONTENT_LEN = 200     # ký tự
MAX_CONTENT_LEN = 50_000  # ký tự
# ──────────────────────────────────────────


def normalize_text(text: str) -> str:
    """Chuẩn hoá Unicode NFC, xoá ký tự lỗi, gộp khoảng trắng."""
    if not text:
        return ""
    # NFC
    text = unicodedata.normalize("NFC", text)
    # Xoá U+FFFD, null
    text = text.replace("\ufffd", "").replace("\x00", "")
    # Gộp khoảng trắng liên tiếp (giữ nguyên \n)
    text = re.sub(r"[^\S\n]+", " ", text)
    # Gộp nhiều dòng trống liên tiếp thành 1
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def main():
    src = Path(INPUT)
    if not src.exists():
        print(f"Không tìm thấy file: {INPUT}")
        return

    # ── Đọc toàn bộ ──
    records = []
    bad_json = 0
    with src.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                bad_json += 1

    total_raw = len(records)
    print(f"Đọc xong: {total_raw} bản ghi ({bad_json} dòng JSON lỗi)")

    # ── Bước 1: Loại thiếu title / content ──
    step1_drop = 0
    kept = []
    for r in records:
        t = (r.get("title") or "").strip()
        c = (r.get("content") or "").strip()
        if not t or not c:
            step1_drop += 1
            continue
        kept.append(r)
    records = kept
    print(f"  B1  loại thiếu title/content   : -{step1_drop}  → còn {len(records)}")

    # ── Bước 2: Loại trùng URL ──
    seen_url = set()
    step2_drop = 0
    kept = []
    for r in records:
        u = r.get("url", "")
        if u in seen_url:
            step2_drop += 1
            continue
        seen_url.add(u)
        kept.append(r)
    records = kept
    print(f"  B2  loại trùng URL             : -{step2_drop}  → còn {len(records)}")

    # ── Bước 3+4: Chuẩn hoá text ──
    for r in records:
        r["title"]       = normalize_text(r.get("title", ""))
        r["description"] = normalize_text(r.get("description", ""))
        r["content"]     = normalize_text(r.get("content", ""))
    print(f"  B3-4 chuẩn hoá Unicode + ws    : done")

    # ── Bước 5: Loại content quá ngắn / quá dài ──
    step5_short = 0
    step5_long  = 0
    kept = []
    for r in records:
        clen = len(r["content"])
        if clen < MIN_CONTENT_LEN:
            step5_short += 1
            continue
        if clen > MAX_CONTENT_LEN:
            step5_long += 1
            continue
        kept.append(r)
    records = kept
    print(f"  B5  loại quá ngắn (<{MIN_CONTENT_LEN})        : -{step5_short}")
    print(f"      loại quá dài  (>{MAX_CONTENT_LEN})      : -{step5_long}  → còn {len(records)}")

    # ── Bước 6: Loại trùng nội dung (hash content) ──
    seen_hash = set()
    step6_drop = 0
    kept = []
    for r in records:
        h = content_hash(r["content"])
        if h in seen_hash:
            step6_drop += 1
            continue
        seen_hash.add(h)
        kept.append(r)
    records = kept
    print(f"  B6  loại trùng nội dung (hash) : -{step6_drop}  → còn {len(records)}")

    # ── Bước 7: Reindex id ──
    for i, r in enumerate(records, start=1):
        r["id"] = i
    print(f"  B7  reindex id 1..{len(records)}")

    # ── Ghi file output ──
    dst = Path(OUTPUT)
    with dst.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total_clean = len(records)
    removed = total_raw - total_clean

    # ── Thống kê bổ sung ──
    source_count = Counter(r["source"] for r in records)
    content_lens = [len(r["content"]) for r in records]
    content_lens.sort()

    # ── In báo cáo ──
    report_lines = [
        "=" * 60,
        "  BÁO CÁO TIỀN XỬ LÝ DỮ LIỆU",
        "=" * 60,
        f"File gốc        : {INPUT}",
        f"File sạch        : {OUTPUT}",
        "",
        f"Tống số bản ghi gốc     : {total_raw}",
        f"  - JSON lỗi             : {bad_json}",
        f"  - Thiếu title/content  : {step1_drop}",
        f"  - Trùng URL            : {step2_drop}",
        f"  - Content quá ngắn     : {step5_short}",
        f"  - Content quá dài      : {step5_long}",
        f"  - Trùng nội dung       : {step6_drop}",
        f"  → Tổng loại bỏ         : {removed}",
        "",
        f"Bản ghi sạch còn lại    : {total_clean}",
        "",
        "Phân bổ theo nguồn:",
    ]
    for s, n in source_count.most_common():
        pct = n / total_clean * 100
        report_lines.append(f"  {s:15s} : {n:>6d}  ({pct:.1f}%)")

    report_lines += [
        "",
        "Thống kê độ dài content (ký tự):",
        f"  min    = {content_lens[0]}",
        f"  Q1     = {content_lens[len(content_lens)//4]}",
        f"  median = {content_lens[len(content_lens)//2]}",
        f"  Q3     = {content_lens[3*len(content_lens)//4]}",
        f"  max    = {content_lens[-1]}",
        f"  avg    = {sum(content_lens)//len(content_lens)}",
        "=" * 60,
    ]

    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    Path(REPORT).write_text(report_text, encoding="utf-8")
    print(f"\nĐã lưu báo cáo  → {REPORT}")
    print(f"Đã lưu dữ liệu  → {OUTPUT}")


if __name__ == "__main__":
    main()
