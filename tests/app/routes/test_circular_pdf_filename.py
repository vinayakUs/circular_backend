"""Regression tests for the View Circular PDF response filename.

The route must use the circular number for both S3 and local files,
regardless of the underlying object's filename.
"""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from flask import Flask

from app.routes import circulars


PDF_BYTES = b"%PDF-1.4\n% test circular\n"


def _client_with_record(record_id, circular_number, file_path):
    app = Flask(__name__)
    app.config.update(TESTING=True)
    circulars.register_routes(app, rag_generator=None)
    db = patch.object(circulars, "get_postgres_client")
    records = patch.object(circulars, "CircularRepository")
    assets = patch.object(circulars, "AssetRepository")
    db_mock = db.start()
    records_mock = records.start()
    assets_mock = assets.start()
    db_mock.return_value.get_pool.return_value = object()
    records_mock.return_value.get_record_by_id.return_value = SimpleNamespace(
        full_reference=circular_number,
        circular_id=circular_number,
    )
    assets_mock.return_value.get_primary_asset.return_value = SimpleNamespace(
        file_path=file_path, mime_type="application/pdf"
    )
    return app.test_client(), (db, records, assets), records_mock, assets_mock


def _stop_patches(patchers):
    for patcher in reversed(patchers):
        patcher.stop()


def test_s3_pdf_uses_circular_number_not_s3_object_name():
    record_id = uuid4()
    client, patchers, _, _ = _client_with_record(
        record_id,
        "SEBI/HO/MRD2/DCAP/CIR/P/2019/145",
        "s3://bucket/SEBI/source.pdf",
    )
    try:
        with patch("storage.s3_client.get_s3_client") as s3:
            s3.return_value.download_bytes.return_value = PDF_BYTES
            response = client.get(f"/api/circulars/{record_id}/content")
            s3.return_value.download_bytes.assert_called_once_with(
                "s3://bucket/SEBI/source.pdf"
            )
        assert response.status_code == 200
        assert response.data == PDF_BYTES
        assert response.mimetype == "application/pdf"
        assert (
            response.headers["Content-Disposition"]
            == "inline; filename=SEBI_HO_MRD2_DCAP_CIR_P_2019_145.pdf"
        )
    finally:
        _stop_patches(patchers)


def test_local_pdf_uses_same_circular_number_not_local_basename():
    record_id = uuid4()
    with TemporaryDirectory() as temporary:
        pdf = Path(temporary) / "NSE.pdf"
        pdf.write_bytes(PDF_BYTES)
        client, patchers, _, _ = _client_with_record(
            record_id, "NSE/FAOP/73791", str(pdf)
        )
        try:
            response = client.get(f"/api/circulars/{record_id}/content")
            assert response.status_code == 200
            assert response.data == PDF_BYTES
            assert response.mimetype == "application/pdf"
            assert (
                response.headers["Content-Disposition"]
                == "inline; filename=NSE_FAOP_73791.pdf"
            )
        finally:
            _stop_patches(patchers)


def test_missing_circular_returns_404_without_querying_asset():
    record_id = uuid4()
    client, patchers, records, assets = _client_with_record(
        record_id, "NSE/FAOP/73791", "s3://bucket/source.pdf"
    )
    records.return_value.get_record_by_id.return_value = None
    try:
        response = client.get(f"/api/circulars/{record_id}/content")
        assert response.status_code == 404
        assets.return_value.get_primary_asset.assert_not_called()
    finally:
        _stop_patches(patchers)


def test_missing_asset_returns_404():
    record_id = uuid4()
    client, patchers, _, assets = _client_with_record(
        record_id, "NSE/FAOP/73791", "s3://bucket/source.pdf"
    )
    assets.return_value.get_primary_asset.return_value = None
    try:
        response = client.get(f"/api/circulars/{record_id}/content")
        assert response.status_code == 404
    finally:
        _stop_patches(patchers)
