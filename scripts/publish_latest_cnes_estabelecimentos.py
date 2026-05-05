#!/usr/bin/env python3
"""Prepare the latest CNES estabelecimento partition tree for GitHub."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SOURCE_DIR = Path("data/processed/cnes/estabelecimentos")
DEFAULT_PUBLISH_DIR = Path("data/github/cnes/estabelecimentos")


def find_latest_competencia(source_dir: Path) -> str:
    candidates = [
        path.name
        for path in source_dir.iterdir()
        if path.is_dir() and path.name.isdigit() and len(path.name) == 6
    ]

    if not candidates:
        raise FileNotFoundError(f"Nenhuma competencia encontrada em {source_dir}")

    return max(candidates)


def copy_tree_limited(
    source: Path, target: Path, limit_municipios: int | None = None
) -> int:
    copied = 0

    for parquet_file in sorted(source.glob("*/*/*.parquet")):
        relative_path = parquet_file.relative_to(source)
        target_file = target / relative_path
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(parquet_file, target_file)
        copied += 1

        if limit_municipios is not None and copied >= limit_municipios:
            break

    return copied


def write_manifest(
    publish_dir: Path,
    competencia: str,
    files_count: int,
    source_dir: Path,
    test_mode: bool,
) -> Path:
    manifest = {
        "dataset": "CNES estabelecimentos",
        "competencia": competencia,
        "source": str(source_dir),
        "files_count": files_count,
        "test_mode": test_mode,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = publish_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return manifest_path


def publish_latest(
    source_dir: Path,
    publish_dir: Path,
    limit_municipios: int | None = None,
) -> tuple[str, int, Path]:
    competencia = find_latest_competencia(source_dir)
    source_competencia_dir = source_dir / competencia

    if publish_dir.exists():
        shutil.rmtree(publish_dir)

    target_competencia_dir = publish_dir / competencia
    files_count = copy_tree_limited(
        source_competencia_dir, target_competencia_dir, limit_municipios=limit_municipios
    )
    manifest_path = write_manifest(
        publish_dir=publish_dir,
        competencia=competencia,
        files_count=files_count,
        source_dir=source_competencia_dir,
        test_mode=limit_municipios is not None,
    )

    return competencia, files_count, manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publica somente a competencia CNES mais recente em uma arvore limpa."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"Diretorio processado local. Padrao: {DEFAULT_SOURCE_DIR}",
    )
    parser.add_argument(
        "--publish-dir",
        type=Path,
        default=DEFAULT_PUBLISH_DIR,
        help=f"Diretorio versionavel para GitHub. Padrao: {DEFAULT_PUBLISH_DIR}",
    )
    parser.add_argument(
        "--limit-municipios",
        type=int,
        default=None,
        help="Limita a quantidade de arquivos municipais copiados para teste.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        competencia, files_count, manifest_path = publish_latest(
            source_dir=args.source_dir,
            publish_dir=args.publish_dir,
            limit_municipios=args.limit_municipios,
        )
    except Exception as exc:
        print(f"Erro ao preparar publicacao: {exc}")
        return 1

    print(f"Competencia publicada: {competencia}")
    print(f"Arquivos parquet copiados: {files_count}")
    print(f"Manifesto: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
