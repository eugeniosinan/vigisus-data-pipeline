#!/usr/bin/env python3
"""Prepare the latest CNES estabelecimento UF files for GitHub."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


DEFAULT_SOURCE_DIR = Path("data/processed/cnes/estabelecimentos")
DEFAULT_PUBLISH_DIR = Path("data/github/cnes/estabelecimentos")


def find_latest_competencia(source_dir: Path) -> str:
    candidates = [
        path.stem.removeprefix("tbEstabelecimento")
        for path in source_dir.glob("tbEstabelecimento*.parquet")
        if path.stem.removeprefix("tbEstabelecimento").isdigit()
        and len(path.stem.removeprefix("tbEstabelecimento")) == 6
    ]

    if not candidates:
        raise FileNotFoundError(f"Nenhuma competencia encontrada em {source_dir}")

    return max(candidates)


def write_uf_files(
    source_file: Path, target: Path, competencia: str, limit_ufs: int | None = None
) -> int:
    df = pd.read_parquet(source_file, engine="pyarrow")
    written = 0

    if "co_estado_gestor" not in df.columns or "co_municipio_gestor" not in df.columns:
        df = df.rename(columns={column: column.lower() for column in df.columns})

    for estado, estado_df in sorted(df.groupby("co_estado_gestor", dropna=False)):
        estado_dir = "sem_estado" if pd.isna(estado) or estado == "" else str(estado)
        target_file = target / estado_dir / f"tbEstabelecimento{competencia}_UF{estado_dir}.parquet"
        target_file.parent.mkdir(parents=True, exist_ok=True)
        estado_df.to_parquet(target_file, index=False, engine="pyarrow")
        written += 1

        if limit_ufs is not None and written >= limit_ufs:
            break

    return written


def write_manifest(
    publish_dir: Path,
    competencia: str,
    files_count: int,
    source_file: Path,
    test_mode: bool,
) -> Path:
    manifest = {
        "dataset": "CNES estabelecimentos",
        "competencia": competencia,
        "partition_by": "co_estado_gestor",
        "filter_column": "co_municipio_gestor",
        "source": str(source_file),
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
    limit_ufs: int | None = None,
) -> tuple[str, int, Path]:
    competencia = find_latest_competencia(source_dir)
    source_file = source_dir / f"tbEstabelecimento{competencia}.parquet"

    if publish_dir.exists():
        shutil.rmtree(publish_dir)

    target_competencia_dir = publish_dir / competencia
    files_count = write_uf_files(
        source_file, target_competencia_dir, competencia=competencia, limit_ufs=limit_ufs
    )
    manifest_path = write_manifest(
        publish_dir=publish_dir,
        competencia=competencia,
        files_count=files_count,
        source_file=source_file,
        test_mode=limit_ufs is not None,
    )

    return competencia, files_count, manifest_path


def has_manifest(publish_dir: Path) -> bool:
    return (publish_dir / "manifest.json").exists()


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
        "--limit-ufs",
        type=int,
        default=None,
        help="Limita a quantidade de UFs geradas para teste.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        competencia, files_count, manifest_path = publish_latest(
            source_dir=args.source_dir,
            publish_dir=args.publish_dir,
            limit_ufs=args.limit_ufs,
        )
    except FileNotFoundError as exc:
        if has_manifest(args.publish_dir):
            print(
                "Nenhum Parquet local novo para publicar. "
                f"Manifesto existente mantido em: {args.publish_dir / 'manifest.json'}"
            )
            return 0

        print(f"Erro ao preparar publicacao: {exc}")
        return 1
    except Exception as exc:
        print(f"Erro ao preparar publicacao: {exc}")
        return 1

    print(f"Competencia publicada: {competencia}")
    print(f"Arquivos parquet por UF gerados: {files_count}")
    print(f"Manifesto: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
