#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_stats.py  –  Khảo sát & thống kê dữ liệu thô
====================================================
Đọc file gốc (chỉ đọc, KHÔNG ghi đè) và in ra báo cáo tổng quan:
  • Tổng số bản ghi, JSON lỗi
  • Tỷ lệ thiếu title / description / content
  • Trùng URL, trùng nội dung
  • Unicode lỗi (U+FFFD)
  • Phân bổ theo nguồn (source)
  • Phân phối độ dài content (min, Q1, median, Q3, max, avg)
  • Top 10 URL domain phổ biến nhất
  • Bản ghi content ngắn nhất / dài nhất

Chạy:
  python data_stats.py
  python data_stats.py --input ten_file.jsonl
"""

import json
import argparse
import hashlib
from pathlib import Path
from collections import Counter
from urllib.parse import urlparse


def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Khảo sát & thống kê dữ liệu thô")
    parser.add_argument(
        "--input",
        default="raw_samples_30000_final_20260309.jsonl",
        help="File JSONL đầu vào (mặc định: raw_samples_30000_final_20260309.jsonl)",
    )
    parser.add_argument(
        "--report",
        default="data_stats_report.txt",
        help="File xuất báo cáo (mặc định: data_stats_report.txt)",
    )
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        print(f"❌ Không tìm thấy file: {args.input}")
        return

    # ── 1. Đọc toàn bộ (chỉ đọc, không ghi đè) ──
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

    total = len(records)

    # ── 2. Thống kê thiếu trường ──
    missing_title = sum(1 for r in records if not (r.get("title") or "").strip())
    missing_desc = sum(1 for r in records if not (r.get("description") or "").strip())
    missing_content = sum(1 for r in records if not (r.get("content") or "").strip())
    missing_url = sum(1 for r in records if not (r.get("url") or "").strip())
    missing_source = sum(1 for r in records if not (r.get("source") or "").strip())

    # ── 3. Trùng URL ──
    url_counter = Counter(r.get("url", "") for r in records)
    dup_url = sum(v - 1 for v in url_counter.values() if v > 1)
    dup_url_unique = sum(1 for v in url_counter.values() if v > 1)

    # ── 4. Trùng nội dung (hash content) ──
    content_hashes = Counter()
    for r in records:
        c = (r.get("content") or "").strip()
        if c:
            content_hashes[md5(c)] += 1
    dup_content = sum(v - 1 for v in content_hashes.values() if v > 1)
    dup_content_unique = sum(1 for v in content_hashes.values() if v > 1)

    # ── 5. Unicode lỗi (U+FFFD) ──
    unicode_err = 0
    for r in records:
        for field in ("title", "description", "content"):
            if "\ufffd" in (r.get(field) or ""):
                unicode_err += 1
                break

    # ── 6. Phân bổ theo nguồn ──
    source_counter = Counter(r.get("source", "unknown") for r in records)

    # ── 7. Độ dài content ──
    content_lens = sorted(len((r.get("content") or "").strip()) for r in records)
    non_zero_lens = [x for x in content_lens if x > 0]

    def percentile(arr, p):
        idx = int(len(arr) * p / 100)
        idx = min(idx, len(arr) - 1)
        return arr[idx]

    # ── 8. Phân nhóm độ dài content ──
    len_buckets = Counter()
    for cl in content_lens:
        if cl == 0:
            len_buckets["0 (trống)"] += 1
        elif cl < 200:
            len_buckets["1-199 (quá ngắn)"] += 1
        elif cl < 500:
            len_buckets["200-499"] += 1
        elif cl < 1000:
            len_buckets["500-999"] += 1
        elif cl < 2000:
            len_buckets["1000-1999"] += 1
        elif cl < 5000:
            len_buckets["2000-4999"] += 1
        elif cl < 10000:
            len_buckets["5000-9999"] += 1
        elif cl < 50000:
            len_buckets["10000-49999"] += 1
        else:
            len_buckets["50000+ (quá dài)"] += 1

    bucket_order = [
        "0 (trống)", "1-199 (quá ngắn)", "200-499", "500-999",
        "1000-1999", "2000-4999", "5000-9999", "10000-49999", "50000+ (quá dài)"
    ]

    # ── 9. Top domain ──
    domains = Counter()
    for r in records:
        u = r.get("url", "")
        try:
            domains[urlparse(u).netloc] += 1
        except Exception:
            pass

    # ── 10. ID range ──
    ids = [r.get("id") for r in records if isinstance(r.get("id"), int)]
    min_id = min(ids) if ids else None
    max_id = max(ids) if ids else None

    # ═══════════════════════ BÁO CÁO ═══════════════════════
    lines = []
    lines.append("=" * 65)
    lines.append("   BÁO CÁO KHẢO SÁT DỮ LIỆU THÔ")
    lines.append("=" * 65)
    lines.append(f"File               : {args.input}")
    lines.append(f"Kích thước file    : {src.stat().st_size / 1024 / 1024:.1f} MB")
    lines.append("")

    lines.append("─── TỔNG QUAN ───")
    lines.append(f"  Tổng bản ghi         : {total:,}")
    lines.append(f"  Dòng JSON lỗi        : {bad_json:,}")
    lines.append(f"  ID range             : {min_id} → {max_id}")
    lines.append("")

    lines.append("─── THIẾU TRƯỜNG ───")
    lines.append(f"  Thiếu title          : {missing_title:>6,}  ({missing_title/total*100:.1f}%)")
    lines.append(f"  Thiếu description    : {missing_desc:>6,}  ({missing_desc/total*100:.1f}%)")
    lines.append(f"  Thiếu content        : {missing_content:>6,}  ({missing_content/total*100:.1f}%)")
    lines.append(f"  Thiếu url            : {missing_url:>6,}  ({missing_url/total*100:.1f}%)")
    lines.append(f"  Thiếu source         : {missing_source:>6,}  ({missing_source/total*100:.1f}%)")
    lines.append("")

    lines.append("─── TRÙNG LẶP ───")
    lines.append(f"  URL trùng            : {dup_url:>6,} bản ghi  ({dup_url_unique} URL bị trùng)")
    lines.append(f"  Content trùng (hash) : {dup_content:>6,} bản ghi  ({dup_content_unique} nội dung bị trùng)")
    lines.append("")

    lines.append("─── UNICODE LỖI ───")
    lines.append(f"  Bản ghi chứa U+FFFD : {unicode_err:>6,}")
    lines.append("")

    lines.append("─── PHÂN BỔ THEO NGUỒN ───")
    for s, n in source_counter.most_common():
        pct = n / total * 100
        bar = "█" * int(pct / 2)
        lines.append(f"  {s:15s} : {n:>6,}  ({pct:5.1f}%)  {bar}")
    lines.append("")

    lines.append("─── PHÂN PHỐI ĐỘ DÀI CONTENT ───")
    if non_zero_lens:
        lines.append(f"  min    = {non_zero_lens[0]:>8,} ký tự")
        lines.append(f"  Q1     = {percentile(non_zero_lens, 25):>8,} ký tự")
        lines.append(f"  median = {percentile(non_zero_lens, 50):>8,} ký tự")
        lines.append(f"  Q3     = {percentile(non_zero_lens, 75):>8,} ký tự")
        lines.append(f"  max    = {non_zero_lens[-1]:>8,} ký tự")
        lines.append(f"  avg    = {sum(non_zero_lens)//len(non_zero_lens):>8,} ký tự")
    lines.append("")

    lines.append("─── PHÂN NHÓM ĐỘ DÀI CONTENT ───")
    for bucket in bucket_order:
        n = len_buckets.get(bucket, 0)
        pct = n / total * 100
        bar = "█" * max(1, int(pct / 2)) if n > 0 else ""
        lines.append(f"  {bucket:22s} : {n:>6,}  ({pct:5.1f}%)  {bar}")
    lines.append("")

    lines.append("─── TOP 10 DOMAIN ───")
    for domain, n in domains.most_common(10):
        lines.append(f"  {domain:30s} : {n:>6,}")
    lines.append("")

    # Mẫu bản ghi ngắn nhất / dài nhất
    records_with_content = [r for r in records if (r.get("content") or "").strip()]
    if records_with_content:
        shortest = min(records_with_content, key=lambda r: len(r["content"]))
        longest = max(records_with_content, key=lambda r: len(r["content"]))
        lines.append("─── BẢN GHI CONTENT NGẮN NHẤT ───")
        lines.append(f"  id={shortest.get('id')}  source={shortest.get('source')}  len={len(shortest['content'])}")
        lines.append(f"  url={shortest.get('url')}")
        lines.append(f"  title={shortest.get('title','')[:80]}")
        lines.append("")
        lines.append("─── BẢN GHI CONTENT DÀI NHẤT ───")
        lines.append(f"  id={longest.get('id')}  source={longest.get('source')}  len={len(longest['content'])}")
        lines.append(f"  url={longest.get('url')}")
        lines.append(f"  title={longest.get('title','')[:80]}")

    lines.append("")
    lines.append("=" * 65)

    report = "\n".join(lines)
    print(report)

    # Lưu ra file
    Path(args.report).write_text(report, encoding="utf-8")
    print(f"\n📄 Đã lưu báo cáo → {args.report}")


if __name__ == "__main__":
    main()
