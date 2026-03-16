#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_fields.py  –  Thêm 2 trường mới vào cleaned_data.jsonl
===========================================================
Trường thêm vào mỗi bản ghi:
  • word_count  : số từ trong content (split whitespace)
  • category    : chuyên mục bài báo (trích từ URL path slug)

Ghi đè trực tiếp lên cleaned_data.jsonl
"""

import json
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter

INPUT  = "cleaned_data.jsonl"
OUTPUT = "cleaned_data.jsonl"          # ghi đè lên chính file

# ── Bảng ánh xạ slug URL → tên chuyên mục ──────────────────────────────────
SLUG_MAP = {
    "the-thao"          : "Thể thao",
    "giai-tri"          : "Giải trí",
    "phap-luat"         : "Pháp luật",
    "suc-khoe"          : "Sức khoẻ",
    "giao-duc"          : "Giáo dục",
    "khoa-hoc"          : "Khoa học",
    "cong-nghe"         : "Công nghệ",
    "xa-hoi"            : "Xã hội",
    "doi-song"          : "Đời sống",
    "the-gioi"          : "Thế giới",
    "thoi-su"           : "Thời sự",
    "kinh-doanh"        : "Kinh tế",
    "bat-dong-san"      : "Bất động sản",
    "lao-dong-viec-lam" : "Lao động - Việc làm",
    "du-lich"           : "Du lịch",
    "van-hoa"           : "Văn hoá",
    "o-to-xe-may"       : "Ô tô - Xe máy",
    "tinh-yeu-gioi-tinh": "Đời sống",
    "nhip-song-tre"     : "Đời sống",
    "tam-long-nhan-ai"  : "Xã hội",
    "noi-vu"            : "Thời sự",
    "tam-diem"          : "Thời sự",
    "ban-doc"           : "Bạn đọc",
    "dien-dan"          : "Khác",
    "blog"              : "Khác",
    "dnews"             : "Khác",
    "doanh-nghiep"      : "Kinh tế",
    "thi-truong"        : "Kinh tế",
    "nha-dat"           : "Bất động sản",
    "chinh-tri"         : "Thời sự",
    "tet-2022"          : "Xã hội",
    "tet-2023"          : "Xã hội",
    "tet-2024"          : "Xã hội",
    "tet"               : "Xã hội",
}

# ── Từ khoá fallback (ưu tiên từ trên xuống, so khớp trong title+content) ──
KEYWORD_MAP = [
    ("Thể thao", [
        "bóng đá", "cầu thủ", "sea games", "olympic", "huy chương", "bàn thắng",
        "hlv", "vòng đấu", "giải vô địch", "premier league", "la liga", "champions league",
        "tennis", "golf", "bơi lội", "điền kinh", "boxing", "võ", "bóng chuyền",
        "bóng rổ", "cầu lông", "đua xe", "thể dục", "vận động viên", "tuyển quốc gia",
        "world cup", "euro", "asian games", "thi đấu", "trận đấu", "câu lạc bộ",
    ]),
    ("Giải trí", [
        "ca sĩ", "diễn viên", "nghệ sĩ", "hoa hậu", "hot boy", "hot girl",
        "phim điện ảnh", "bộ phim", "nhạc sĩ", "âm nhạc", "ca khúc", "concert",
        "gameshow", "hoa hậu", "người mẫu", "người nổi tiếng", "sao việt",
        "rapper", "idol", "mv", "showbiz", "liveshow", "lễ trao giải",
        "phim truyền hình", "đạo diễn", "diễn xuất",
    ]),
    ("Pháp luật", [
        "xét xử", "bị cáo", "tòa án", "tuyên phạt", "khởi tố", "điều tra",
        "bắt giữ", "phạt tù", "công an", "cảnh sát", "tội phạm", "vụ án",
        "luật sư", "viện kiểm sát", "hội đồng xét xử", "truy nã", "truy tố",
        "giam giữ", "lừa đảo", "tham nhũng", "hối lộ",
    ]),
    ("Sức khoẻ", [
        "bệnh viện", "bác sĩ", "bệnh nhân", "điều trị", "phẫu thuật", "ung thư",
        "vaccine", "tiêm chủng", "covid", "dịch bệnh", "thuốc", "sức khỏe",
        "y tế", "bộ y tế", "cấp cứu", "dinh dưỡng", "tâm lý", "sức khoẻ sinh sản",
        "bệnh hiểm nghèo", "viện dưỡng lão",
    ]),
    ("Giáo dục", [
        "học sinh", "sinh viên", "giáo viên", "trường học", "đại học", "thi",
        "tốt nghiệp", "tuyển sinh", "học bổng", "điểm thi", "kỳ thi", "lớp học",
        "chương trình học", "giáo dục", "bộ giáo dục", "phổ thông", "mầm non",
        "thpt", "thcs", "đào tạo", "dạy học",
    ]),
    ("Công nghệ", [
        "điện thoại", "smartphone", "iphone", "android", "laptop", "máy tính",
        "trí tuệ nhân tạo", "ai", "chatgpt", "robot", "phần mềm", "ứng dụng",
        "mạng xã hội", "internet", "5g", "chip", "cpu", "gpu", "metaverse",
        "blockchain", "tiền điện tử", "bitcoin", "công nghệ", "startup",
    ]),
    ("Khoa học", [
        "nghiên cứu", "khoa học", "phát minh", "khám phá", "vũ trụ", "nasa",
        "hành tinh", "thiên văn", "sinh học", "hóa học", "vật lý", "gene",
        "biến đổi khí hậu", "môi trường", "tuyệt chủng", "hóa thạch",
    ]),
    ("Kinh tế", [
        "chứng khoán", "cổ phiếu", "gdp", "lạm phát", "ngân hàng", "tín dụng",
        "doanh nghiệp", "xuất khẩu", "nhập khẩu", "thị trường", "kinh tế",
        "đầu tư", "tỷ giá", "lãi suất", "ngoại tệ", "doanh thu", "lợi nhuận",
        "doanh nhân", "tập đoàn", "công ty", "thương mại",
    ]),
    ("Bất động sản", [
        "bất động sản", "chung cư", "căn hộ", "nhà đất", "đất nền", "dự án",
        "phân lô", "bán nền", "thị trường nhà đất", "sốt đất", "giá nhà",
        "cho thuê nhà", "mặt bằng", "khu đô thị",
    ]),
    ("Thế giới", [
        "mỹ", "trung quốc", "nga", "ukraine", "israel", "hamas", "nato",
        "liên hợp quốc", "tổng thống", "thủ tướng", "bộ trưởng ngoại giao",
        "chiến tranh", "xung đột", "ngoại giao", "quốc tế", "thế giới",
        "châu âu", "châu mỹ", "châu phi", "trung đông", "đông nam á",
    ]),
    ("Thời sự", [
        "thủ tướng", "chủ tịch nước", "quốc hội", "chính phủ", "bộ trưởng",
        "đảng", "nghị quyết", "chính sách", "hội nghị", "bầu cử", "thời sự",
        "an ninh", "quốc phòng", "biên giới", "chủ quyền",
    ]),
    ("Xã hội", [
        "người dân", "cộng đồng", "tai nạn", "lũ lụt", "hỏa hoạn", "cháy nổ",
        "mưa bão", "thiên tai", "từ thiện", "tình nguyện", "nhân đạo",
        "trợ cấp", "hộ nghèo", "an sinh xã hội", "phụ nữ", "trẻ em",
        "người già", "xã hội",
    ]),
    ("Du lịch", [
        "du lịch", "điểm đến", "danh lam", "thắng cảnh", "resort", "khách sạn",
        "tour", "phượt", "visa", "hộ chiếu", "sân bay", "chuyến bay",
        "ẩm thực", "đặc sản",
    ]),
    ("Văn hoá", [
        "văn hóa", "lễ hội", "di sản", "bảo tàng", "triển lãm", "mỹ thuật",
        "kiến trúc", "truyền thống", "phong tục", "tập quán", "thờ cúng",
        "nghệ thuật", "dân gian",
    ]),
    ("Ô tô - Xe máy", [
        "ô tô", "xe máy", "xe điện", "ôtô", "mercedes", "toyota", "honda",
        "vinfast", "xe hơi", "đăng kiểm", "biển số", "tài xế", "lái xe",
        "giao thông", "đường bộ",
    ]),
    ("Lao động - Việc làm", [
        "việc làm", "tuyển dụng", "lương", "thất nghiệp", "người lao động",
        "công nhân", "hợp đồng lao động", "bảo hiểm xã hội", "nghỉ thai sản",
        "lao động", "xuất khẩu lao động",
    ]),
    ("Đời sống", [
        "gia đình", "hôn nhân", "ly hôn", "con cái", "tình yêu", "hẹn hò",
        "nấu ăn", "công thức", "mẹo vặt", "kinh nghiệm sống", "nhà ở",
        "nội thất", "thời trang", "làm đẹp",
    ]),
]


def get_category(url: str, title: str = "", description: str = "") -> str:
    """
    Bước 1: Trích slug URL → SLUG_MAP.
    Bước 2 (fallback): Nếu kết quả là 'Khác' → dùng từ khoá trong title + description.
    """
    cat = "Khác"
    try:
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if parts:
            cat = SLUG_MAP.get(parts[0].lower(), "Khác")
    except Exception:
        pass

    if cat != "Khác":
        return cat

    # Fallback: tìm từ khoá trong title + description (ngắn → nhanh)
    text = (title + " " + description).lower()
    best_cat = "Khác"
    best_count = 0
    for category, keywords in KEYWORD_MAP:
        count = sum(1 for kw in keywords if kw in text)
        if count > best_count:
            best_count = count
            best_cat = category

    return best_cat


def word_count(text: str) -> int:
    """Đếm số từ bằng cách split whitespace."""
    if not text:
        return 0
    return len(text.split())


def main():
    src = Path(INPUT)
    if not src.exists():
        print(f"Không tìm thấy file: {INPUT}")
        return

    # Đọc toàn bộ
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

    print(f"Đọc: {len(records):,} bản ghi  ({bad_json} lỗi JSON)")

    # Thêm 2 trường mới
    cat_counter = Counter()
    for r in records:
        r["word_count"] = word_count(r.get("content", ""))
        r["category"]   = get_category(
            r.get("url", ""),
            r.get("title", ""),
            r.get("description", ""),
        )
        cat_counter[r["category"]] += 1

    # Ghi lại file
    with src.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Báo cáo
    total = len(records)
    wc_all = [r["word_count"] for r in records]
    wc_all.sort()

    print(f"\n✓ Đã thêm trường  word_count  và  category  → {OUTPUT}")
    print(f"\n─── Phân bổ theo chuyên mục (category) ───")
    for cat, n in cat_counter.most_common():
        pct = n / total * 100
        bar = "█" * max(1, int(pct / 2))
        print(f"  {cat:25s} : {n:>6,}  ({pct:5.1f}%)  {bar}")

    print(f"\n─── Thống kê word_count ───")
    print(f"  min    = {wc_all[0]:,}")
    print(f"  median = {wc_all[len(wc_all)//2]:,}")
    print(f"  max    = {wc_all[-1]:,}")
    print(f"  avg    = {sum(wc_all)//len(wc_all):,}")


if __name__ == "__main__":
    main()
