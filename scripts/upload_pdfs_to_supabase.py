#!/usr/bin/env python3
"""
Upload les PDFs locaux (dossier ./pdfs) vers Supabase Storage.

Prérequis :
    pip install supabase

Configuration dans st.secrets ou env var :
    SUPABASE_URL       = "https://xxxxx.supabase.co"
    SUPABASE_SERVICE_KEY = "eyJhbGciOi..."   # service role, admin
    SUPABASE_BUCKET    = "brvm-pdfs"         # nom du bucket (créer avant)

Usage :
    # Dry run (liste ce qui serait uploadé)
    python3 scripts/upload_pdfs_to_supabase.py --dry-run

    # Upload réel
    python3 scripts/upload_pdfs_to_supabase.py

    # Upload avec écrasement (sinon skip si déjà présent)
    python3 scripts/upload_pdfs_to_supabase.py --overwrite

    # Ne remonter que les PDFs >= date donnée
    python3 scripts/upload_pdfs_to_supabase.py --since 2026-01-01

Après l'upload, l'app (via fetch_publication.py) peut pointer vers les URLs
publiques : https://{project}.supabase.co/storage/v1/object/public/{bucket}/{path}
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _get_secret(key: str):
    """Lit depuis st.secrets (si disponible) ou os.environ."""
    try:
        import streamlit as st
        v = st.secrets.get(key)
        if v:
            return v
    except Exception:
        pass
    return os.environ.get(key)


def _iter_pdfs(root: Path, since_date: str = None):
    """Retourne la liste (local_path, remote_key) des PDFs à uploader.
    remote_key suit la structure : {sous-dossier}/{nom.pdf}"""
    from datetime import datetime
    since_dt = None
    if since_date:
        since_dt = datetime.strptime(since_date, "%Y-%m-%d")

    for pdf in root.rglob("*.pdf"):
        if since_dt:
            if datetime.fromtimestamp(pdf.stat().st_mtime) < since_dt:
                continue
        rel = pdf.relative_to(root)
        # Normaliser les slashes pour Supabase (toujours /)
        remote_key = str(rel).replace(os.sep, "/")
        yield pdf, remote_key


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Lister sans uploader")
    parser.add_argument("--overwrite", action="store_true", help="Écraser les fichiers existants")
    parser.add_argument("--since", type=str, help="YYYY-MM-DD — uploader seulement les fichiers récents")
    parser.add_argument("--bucket", type=str, default=None, help="Override le nom du bucket")
    args = parser.parse_args()

    url = _get_secret("SUPABASE_URL")
    key = _get_secret("SUPABASE_SERVICE_KEY")
    bucket = args.bucket or _get_secret("SUPABASE_BUCKET") or "brvm-pdfs"

    if not args.dry_run:
        if not url or not key:
            print("❌ SUPABASE_URL et SUPABASE_SERVICE_KEY requis.")
            print("   Définir dans .streamlit/secrets.toml ou comme env vars.")
            sys.exit(1)

    pdf_root = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "pdfs"
    if not pdf_root.exists():
        print(f"❌ Dossier PDFs introuvable : {pdf_root}")
        sys.exit(1)

    pdfs = list(_iter_pdfs(pdf_root, since_date=args.since))
    total_size = sum(p[0].stat().st_size for p in pdfs)
    print(f"{'='*60}")
    print(f"UPLOAD PDFs → Supabase Storage")
    print(f"{'='*60}")
    print(f"Source      : {pdf_root}")
    print(f"Bucket      : {bucket}")
    print(f"Fichiers    : {len(pdfs)}")
    print(f"Taille      : {total_size/1024/1024:.1f} MB")
    if args.since:
        print(f"Since       : {args.since}")
    print(f"Mode        : {'DRY RUN' if args.dry_run else ('OVERWRITE' if args.overwrite else 'SKIP existants')}")
    print()

    if args.dry_run:
        for local, remote in pdfs[:20]:
            print(f"  [{local.stat().st_size//1024} KB] {remote}")
        if len(pdfs) > 20:
            print(f"  ... et {len(pdfs)-20} autres")
        return

    # Connexion Supabase
    try:
        from supabase import create_client
    except ImportError:
        print("❌ Package `supabase` non installé.")
        print("   pip install supabase")
        sys.exit(1)

    client = create_client(url, key)
    storage = client.storage.from_(bucket)

    uploaded = 0
    skipped = 0
    errors = 0
    for local, remote in pdfs:
        try:
            with open(local, "rb") as fh:
                data = fh.read()

            try:
                if args.overwrite:
                    storage.update(remote, data, {"content-type": "application/pdf"})
                else:
                    storage.upload(remote, data, {"content-type": "application/pdf"})
                uploaded += 1
                print(f"  ✅ {remote}")
            except Exception as e:
                msg = str(e).lower()
                if "duplicate" in msg or "already exists" in msg or "409" in msg:
                    skipped += 1
                    print(f"  ⏭️  {remote} (déjà présent)")
                else:
                    errors += 1
                    print(f"  ❌ {remote} — {e}")
        except Exception as e:
            errors += 1
            print(f"  ❌ {remote} — {e}")

    print(f"\n{'='*60}")
    print(f"Uploadés : {uploaded} · Skippés : {skipped} · Erreurs : {errors}")
    print(f"{'='*60}")
    if uploaded and not errors:
        print(f"\n💡 URL publique : {url}/storage/v1/object/public/{bucket}/<path>")


if __name__ == "__main__":
    main()
