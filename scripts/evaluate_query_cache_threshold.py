#!/usr/bin/env python3

import argparse
import random
from collections import defaultdict
from pathlib import Path

import numpy as np


def _load_normalized_embeddings(path: Path) -> np.ndarray:
    embeddings = np.load(path).astype(np.float32)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    embeddings /= np.clip(norms, 1e-12, None)
    return embeddings


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Estimate a sane cosine-similarity threshold for the QueryHashCache using "
            "MS MARCO query embeddings."
        )
    )
    parser.add_argument(
        "--base",
        type=Path,
        default=Path("data/ms_marco"),
        help="Directory containing query_embeddings_100k.npy and query_to_doc.txt",
    )
    parser.add_argument(
        "--pairs",
        type=int,
        default=20_000,
        help="Number of positive and negative pairs to sample",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed",
    )
    args = parser.parse_args()

    base: Path = args.base
    embeddings_path = base / "query_embeddings_100k.npy"
    mapping_path = base / "query_to_doc.txt"

    if not embeddings_path.exists():
        raise SystemExit(f"Missing embeddings: {embeddings_path}")
    if not mapping_path.exists():
        raise SystemExit(f"Missing mapping: {mapping_path}")

    query_embeddings = _load_normalized_embeddings(embeddings_path)
    doc_ids = np.loadtxt(mapping_path, dtype=np.int64)
    if len(doc_ids) != len(query_embeddings):
        raise SystemExit(
            f"Length mismatch: embeddings={len(query_embeddings)} mapping={len(doc_ids)}"
        )

    by_doc: dict[int, list[int]] = defaultdict(list)
    for idx, doc_id in enumerate(doc_ids.tolist()):
        by_doc[int(doc_id)].append(idx)

    multi_docs = [doc for doc, idxs in by_doc.items() if len(idxs) >= 2]
    all_docs = list(by_doc.keys())
    if not multi_docs:
        raise SystemExit("No documents with >=2 queries; cannot sample positive pairs")
    if len(all_docs) < 2:
        raise SystemExit("Need at least 2 documents to sample negative pairs; found 1")

    rng = random.Random(args.seed)

    def cosine(i: int, j: int) -> float:
        return float(np.dot(query_embeddings[i], query_embeddings[j]))

    pos = np.empty(args.pairs, dtype=np.float32)
    for k in range(args.pairs):
        doc = rng.choice(multi_docs)
        i, j = rng.sample(by_doc[doc], 2)
        pos[k] = cosine(i, j)

    neg = np.empty(args.pairs, dtype=np.float32)
    for k in range(args.pairs):
        d1, d2 = rng.sample(all_docs, 2)
        i = rng.choice(by_doc[d1])
        j = rng.choice(by_doc[d2])
        neg[k] = cosine(i, j)

    def pct(arr: np.ndarray, q: float) -> float:
        return float(np.percentile(arr, q))

    print(f"queries={len(query_embeddings)} docs={len(by_doc)} docs_with_2plus_queries={len(multi_docs)}")
    print(
        "pos_cosine: mean={:.4f} p50={:.4f} p90={:.4f} p99={:.4f}".format(
            float(pos.mean()), pct(pos, 50), pct(pos, 90), pct(pos, 99)
        )
    )
    print(
        "neg_cosine: mean={:.4f} p90={:.4f} p99={:.4f} p99.9={:.4f}".format(
            float(neg.mean()), pct(neg, 90), pct(neg, 99), pct(neg, 99.9)
        )
    )

    thresholds = [0.45, 0.50, 0.52, 0.55, 0.58, 0.60, 0.65, 0.70, 0.75, 0.80]
    for t in thresholds:
        tpr = float((pos >= t).mean())
        fpr = float((neg >= t).mean())
        print(f"t={t:.2f}  TPR={tpr:.3f}  FPR={fpr:.4f}")

    chosen_t = 0.52
    tpr = float((pos >= chosen_t).mean())
    fpr = float((neg >= chosen_t).mean())
    fnr = 1.0 - tpr
    print(
        "summary: t={:.2f}  TPR={:.3f}  FPR={:.4f}  FNR={:.3f} (balanced sample)".format(
            chosen_t, tpr, fpr, fnr
        )
    )


if __name__ == "__main__":
    main()
