#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import random
import re
import time
import argparse
import gzip
from collections import deque
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}
TIMEOUT = 20


def fetch(url, session):
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            content_type = (r.headers.get("Content-Type") or "").lower()
            if url.lower().endswith(".gz") or ("gzip" in content_type and "html" not in content_type):
                try:
                    return gzip.decompress(r.content).decode("utf-8", errors="ignore")
                except Exception:
                    pass
            return r.text
    except Exception:
        return None
    return None


def norm(s):
    return re.sub(r"\s+", " ", s or "").strip()


def parse_xml_locs(xml_text):
    soup = BeautifulSoup(xml_text, "xml")
    locs = [x.get_text(strip=True) for x in soup.find_all("loc")]
    if locs:
        return locs
    return [norm(x) for x in re.findall(r"<loc>\s*(.*?)\s*</loc>", xml_text or "", flags=re.I | re.S)]


def parse_rss_links(rss_text):
    links = [norm(x) for x in re.findall(r"<link>\s*(https?://[^<]+)\s*</link>", rss_text or "", flags=re.I)]
    out = []
    seen = set()
    for u in links:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def detect_source(url):
    host = urlparse(url).netloc.lower()
    if "vnexpress.net" in host:
        return "vnexpress"
    if "dantri.com.vn" in host:
        return "dantri"
    if "thanhnien.vn" in host:
        return "thanhnien"
    return None


def is_article_url(url, source):
    u = url.lower()
    if any(k in u for k in ["/video", "/podcast", "/photo", "/tag/", "/tags/", "/author/"]):
        return False
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|mp4|webm|mp3|pdf)(\?|$)", u):
        return False
    if source == "vnexpress":
        return u.endswith(".html")
    if source == "dantri":
        return u.endswith(".htm") or u.endswith(".html")
    if source == "thanhnien":
        return bool(re.search(r"-\d{12,}\.(htm|html)$", u))
    return False


def get_candidate_urls_by_source(session, max_sitemaps_per_source=0, max_urls_per_sitemap=0):
    roots = {
        "vnexpress": ["https://vnexpress.net/sitemap.xml"],
        "dantri": [
            "https://dantri.com.vn/sitemaps/articles.xml",
            "https://dantri.com.vn/sitemaps/category-sitemap.xml",
            "https://dantri.com.vn/google-news-sitemap.xml",
        ],
        "thanhnien": ["https://thanhnien.vn/sitemap.xml"],
    }

    out = {"vnexpress": [], "dantri": [], "thanhnien": []}
    seen_by_source = {"vnexpress": set(), "dantri": set(), "thanhnien": set()}

    for source, root_list in roots.items():
        sitemap_links = []
        for root in root_list:
            xml = fetch(root, session)
            if not xml:
                continue
            locs = parse_xml_locs(xml)
            if locs and any("sitemap" in x.lower() for x in locs):
                sitemap_links.extend([x for x in locs if "sitemap" in x.lower()])
            else:
                sitemap_links.append(root)

        # unique sitemap list
        uniq_sm = []
        seen_sm = set()
        for sm in sitemap_links:
            if sm in seen_sm:
                continue
            seen_sm.add(sm)
            uniq_sm.append(sm)
        sitemap_links = uniq_sm

        if max_sitemaps_per_source and max_sitemaps_per_source > 0:
            sitemap_links = sitemap_links[:max_sitemaps_per_source]

        if not sitemap_links:
            continue

        for sm in sitemap_links:
            sm_xml = fetch(sm, session)
            if not sm_xml:
                continue

            urls = parse_xml_locs(sm_xml)
            if max_urls_per_sitemap and max_urls_per_sitemap > 0:
                urls = urls[:max_urls_per_sitemap]

            for u in urls:
                if detect_source(u) != source:
                    continue
                if not is_article_url(u, source):
                    continue
                if u in seen_by_source[source]:
                    continue
                seen_by_source[source].add(u)
                out[source].append(u)

        if source == "vnexpress" and len(out[source]) == 0:
            rss_index = fetch("https://vnexpress.net/rss", session)
            if rss_index:
                rss_feeds = sorted(set(re.findall(r"https://vnexpress\.net/rss/[^\"'>\s]+\.rss", rss_index, flags=re.I)))
                if "https://vnexpress.net/rss/tin-moi-nhat.rss" not in rss_feeds:
                    rss_feeds.append("https://vnexpress.net/rss/tin-moi-nhat.rss")

                for feed in rss_feeds:
                    feed_xml = fetch(feed, session)
                    if not feed_xml:
                        continue

                    feed_urls = parse_rss_links(feed_xml)
                    for u in feed_urls:
                        if detect_source(u) != source:
                            continue
                        if not is_article_url(u, source):
                            continue
                        if u in seen_by_source[source]:
                            continue
                        seen_by_source[source].add(u)
                        out[source].append(u)

        random.shuffle(out[source])

    return out


def build_balanced_queue(urls_by_source, limit):
    sources = ["vnexpress", "dantri", "thanhnien"]
    target = {s: limit // len(sources) for s in sources}
    for s in sources[:limit % len(sources)]:
        target[s] += 1

    queues = {s: deque(urls_by_source.get(s, [])) for s in sources}
    taken = {s: 0 for s in sources}
    ordered = []

    active_sources = set(sources)
    while len(ordered) < limit and active_sources:
        progressed = False
        for s in sources:
            if s not in active_sources:
                continue

            if taken[s] >= target[s] and any(taken[x] < target[x] and queues[x] for x in sources):
                continue

            if not queues[s]:
                active_sources.remove(s)
                continue

            ordered.append((s, queues[s].popleft()))
            taken[s] += 1
            progressed = True

            if len(ordered) >= limit:
                break

        if not progressed:
            break

    return ordered


def parse_article(html, url, source):
    soup = BeautifulSoup(html, "html.parser")

    # title
    title_node = soup.select_one("h1.title-detail, h1.title-page, h1.detail-title, h1")
    title = norm(title_node.get_text(" ", strip=True)) if title_node else ""

    # description
    desc = ""
    desc_node = soup.select_one("p.description, h2.singular-sapo, .detail-sapo")
    if desc_node:
        desc = norm(desc_node.get_text(" ", strip=True))
    else:
        meta_desc = soup.select_one("meta[name='description']")
        if meta_desc:
            desc = norm(meta_desc.get("content", ""))

    # content selectors fallback
    selectors = [
        "article.fck_detail p.Normal",
        "article.fck_detail p",
        ".singular-content p",
        ".detail-content p",
        "article p",
    ]

    parts = []
    for sel in selectors:
        ps = soup.select(sel)
        if ps:
            temp = []
            for p in ps:
                t = norm(p.get_text(" ", strip=True))
                if len(t) > 20:
                    temp.append(t)
            if len(" ".join(temp)) > 300:
                parts = temp
                break

    content = "\n".join(parts)

    if len(content) < 300:
        return None

    return {
        "source": source,
        "url": url,
        "title": title,
        "description": desc,
        "content": content
    }


def crawl_raw_samples(
    limit,
    output_path,
    sleep_sec,
    max_sitemaps_per_source=0,
    max_urls_per_sitemap=0,
    save_html=False,
    resume=False,
):
    session = requests.Session()
    urls_by_source = get_candidate_urls_by_source(
        session,
        max_sitemaps_per_source=max_sitemaps_per_source,
        max_urls_per_sitemap=max_urls_per_sitemap,
    )
    candidates = build_balanced_queue(urls_by_source, limit)

    total_candidates = sum(len(v) for v in urls_by_source.values())
    print(
        "Đã lấy candidate: "
        f"vnexpress={len(urls_by_source['vnexpress'])}, "
        f"dantri={len(urls_by_source['dantri'])}, "
        f"thanhnien={len(urls_by_source['thanhnien'])} "
        f"| total={total_candidates}"
    )
    if len(candidates) < limit:
        print(f"Cảnh báo: chỉ có {len(candidates)} candidate hợp lệ cho mục tiêu {limit} mẫu")

    collected = 0
    started_at = time.time()
    collected_by_source = {"vnexpress": 0, "dantri": 0, "thanhnien": 0}
    seen_urls = set()

    if resume:
        try:
            with open(output_path, "r", encoding="utf-8") as rf:
                for line in rf:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except Exception:
                        continue
                    collected += 1
                    url = item.get("url")
                    if url:
                        seen_urls.add(url)
                    src = item.get("source")
                    if src in collected_by_source:
                        collected_by_source[src] += 1
            print(
                f"Resume: đã có sẵn {collected} mẫu "
                f"(vnexpress={collected_by_source['vnexpress']}, "
                f"dantri={collected_by_source['dantri']}, "
                f"thanhnien={collected_by_source['thanhnien']})"
            )
        except FileNotFoundError:
            pass

    mode = "a" if resume else "w"
    with open(output_path, mode, encoding="utf-8") as f:
        for source, url in candidates:
            if collected >= limit:
                break

            if url in seen_urls:
                continue

            html = fetch(url, session)
            if not html:
                continue

            record = {
                "id": collected + 1,
                "source": source,
                "url": url,
                "fetched_at": int(time.time())
            }

            if save_html:
                record["html"] = html
            else:
                parsed = parse_article(html, url, source)
                if parsed:
                    record["title"] = parsed.get("title", "")
                    record["description"] = parsed.get("description", "")
                    record["content"] = parsed.get("content", "")

            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            collected += 1
            seen_urls.add(url)
            collected_by_source[source] += 1

            if collected % 50 == 0:
                elapsed = time.time() - started_at
                print(
                    f"Đã thu thập {collected}/{limit} mẫu raw "
                    f"(vnexpress={collected_by_source['vnexpress']}, "
                    f"dantri={collected_by_source['dantri']}, "
                    f"thanhnien={collected_by_source['thanhnien']}) "
                    f"| elapsed: {elapsed:.1f}s"
                )

            time.sleep(sleep_sec)

    elapsed = time.time() - started_at
    print(
        f"Hoàn tất: {collected} mẫu raw -> {output_path} "
        f"(vnexpress={collected_by_source['vnexpress']}, "
        f"dantri={collected_by_source['dantri']}, "
        f"thanhnien={collected_by_source['thanhnien']}) "
        f"| elapsed: {elapsed:.1f}s"
    )


def main():
    parser = argparse.ArgumentParser(description="Crawl raw HTML samples for data mining")
    parser.add_argument("--limit", type=int, default=1000, help="Số mẫu raw cần crawl")
    parser.add_argument("--output", default="raw_samples.jsonl", help="Tên file output JSONL")
    parser.add_argument("--sleep", type=float, default=0.2, help="Độ trễ giữa các request (giây)")
    parser.add_argument(
        "--max-sitemaps-per-source",
        type=int,
        default=0,
        help="Số sitemap tối đa mỗi nguồn (0 = không giới hạn)",
    )
    parser.add_argument(
        "--max-urls-per-sitemap",
        type=int,
        default=0,
        help="Số URL tối đa đọc từ mỗi sitemap (0 = không giới hạn)",
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Lưu full HTML vào output (mặc định tắt để giảm dung lượng)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Tiếp tục crawl vào file output hiện có, không ghi đè",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit phải > 0")
    if args.max_sitemaps_per_source < 0:
        raise ValueError("--max-sitemaps-per-source phải >= 0")
    if args.max_urls_per_sitemap < 0:
        raise ValueError("--max-urls-per-sitemap phải >= 0")

    crawl_raw_samples(
        limit=args.limit,
        output_path=args.output,
        sleep_sec=args.sleep,
        max_sitemaps_per_source=args.max_sitemaps_per_source,
        max_urls_per_sitemap=args.max_urls_per_sitemap,
        save_html=args.save_html,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()