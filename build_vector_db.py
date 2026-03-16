#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

import chromadb
from sentence_transformers import SentenceTransformer
from tqdm import tqdm


def load_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    records = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue
    return records


def build_doc_text(record: Dict[str, Any], max_content_chars: int = 1200) -> str:
    title = (record.get("title") or "").strip()
    description = (record.get("description") or "").strip()
    content = (record.get("content") or "").strip()
    content = content[:max_content_chars]
    return "\n".join([part for part in [title, description, content] if part])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ChromaDB vector database from cleaned_data.jsonl")
    parser.add_argument("--input", default="cleaned_data.jsonl", help="Input JSONL file")
    parser.add_argument("--db-path", default="chroma_db", help="Persistent ChromaDB folder")
    parser.add_argument("--collection", default="news_articles_v1", help="Collection name")
    parser.add_argument("--model", default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", help="SentenceTransformer model")
    parser.add_argument("--batch-size", type=int, default=32, help="Embedding/upsert batch size")
    parser.add_argument("--max-content-chars", type=int, default=1200, help="Max content chars used for embedding")
    parser.add_argument("--start", type=int, default=0, help="Start offset (for partial builds)")
    parser.add_argument("--limit", type=int, default=0, help="Max records to process after start (0 = all)")
    parser.add_argument("--device", default="cpu", help="Embedding device: cpu or cuda")
    parser.add_argument("--reset", action="store_true", help="Delete old collection before rebuild")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    print(f"Loading data from: {input_path}")
    records = load_jsonl(input_path)
    if not records:
        raise ValueError("No valid records found in input JSONL")
    print(f"Loaded records: {len(records)}")

    print(f"Loading embedding model: {args.model} (device={args.device})")
    model = SentenceTransformer(args.model, device=args.device)

    client = chromadb.PersistentClient(path=args.db_path)

    if args.reset:
        try:
            client.delete_collection(args.collection)
            print(f"Deleted existing collection: {args.collection}")
        except Exception:
            pass

    collection = client.get_or_create_collection(name=args.collection, metadata={"hnsw:space": "cosine"})

    ids_all: List[str] = []
    docs_all: List[str] = []
    metas_all: List[Dict[str, Any]] = []

    for record in records:
        record_id = str(record.get("id"))
        if not record_id:
            continue

        doc = build_doc_text(record, max_content_chars=args.max_content_chars)
        if not doc:
            continue

        meta = {
            "source": str(record.get("source", "")),
            "category": str(record.get("category", "Khác")),
            "url": str(record.get("url", "")),
            "title": str(record.get("title", ""))[:500],
            "word_count": int(record.get("word_count", 0) or 0),
            "fetched_at": int(record.get("fetched_at", 0) or 0),
        }

        ids_all.append(record_id)
        docs_all.append(doc)
        metas_all.append(meta)

    total_all = len(ids_all)
    start = max(0, args.start)
    end = total_all if args.limit <= 0 else min(total_all, start + args.limit)

    if start >= end:
        raise ValueError(f"Invalid range: start={start}, end={end}, total={total_all}")

    ids_all = ids_all[start:end]
    docs_all = docs_all[start:end]
    metas_all = metas_all[start:end]

    total = len(ids_all)
    print(f"Vectorizing + upserting records: {total} (range: {start}..{end - 1})")

    batch_size = max(8, args.batch_size)
    for start in tqdm(range(0, total, batch_size), desc="Building vectors"):
        end = min(start + batch_size, total)
        docs_batch = docs_all[start:end]
        embeds = model.encode(
            docs_batch,
            batch_size=batch_size,
            show_progress_bar=False,
            normalize_embeddings=True,
            convert_to_numpy=True,
        ).tolist()

        collection.upsert(
            ids=ids_all[start:end],
            documents=docs_batch,
            embeddings=embeds,
            metadatas=metas_all[start:end],
        )

    print("\nBuild completed.")
    print(f"DB path: {args.db_path}")
    print(f"Collection: {args.collection}")
    print(f"Count in collection: {collection.count()}")


if __name__ == "__main__":
    main()
