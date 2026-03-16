#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

import chromadb
from sentence_transformers import SentenceTransformer


def main() -> None:
    parser = argparse.ArgumentParser(description="Query ChromaDB vector database")
    parser.add_argument("--query", required=True, help="Search query text")
    parser.add_argument("--db-path", default="chroma_db", help="Persistent ChromaDB folder")
    parser.add_argument("--collection", default="news_articles_v1", help="Collection name")
    parser.add_argument("--model", default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", help="SentenceTransformer model")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results")
    parser.add_argument("--category", default="", help="Optional category filter")
    parser.add_argument("--source", default="", help="Optional source filter")
    args = parser.parse_args()

    model = SentenceTransformer(args.model)
    query_embedding = model.encode([args.query], normalize_embeddings=True).tolist()[0]

    client = chromadb.PersistentClient(path=args.db_path)
    collection = client.get_collection(args.collection)

    where = {}
    if args.category:
        where["category"] = args.category
    if args.source:
        where["source"] = args.source

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=max(1, args.top_k),
        where=where or None,
        include=["documents", "metadatas", "distances"],
    )

    docs = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    dists = results.get("distances", [[]])[0]

    print(f"Query: {args.query}")
    print(f"Top-{args.top_k} results\n")

    for idx, (doc, meta, dist) in enumerate(zip(docs, metas, dists), start=1):
        score = 1 - float(dist)
        print(f"[{idx}] score={score:.4f} | category={meta.get('category')} | source={meta.get('source')}")
        print(f"title: {meta.get('title')}")
        print(f"url  : {meta.get('url')}")
        preview = (doc or "").replace("\n", " ")[:240]
        print(f"text : {preview}...")
        print("-" * 100)


if __name__ == "__main__":
    main()
