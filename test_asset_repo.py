import sys
sys.path.insert(0, '.')

from db import get_db_client
from ingestion.repository.asset_repository import AssetRepository, CircularAsset
from uuid import UUID

pool = get_db_client().get_pool()

# Insert a test circular first
with pool.acquire() as conn:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO circulars (id, circular_id, source, full_reference, title, issue_date, status)
        VALUES (SYS_GUID(), 'CIRC-001', 'SEBI', 'SEBI/CIRCULAR/2026/001', 'Test Circular', DATE '2026-01-15', 'FETCHED')
    """)
    conn.commit()
    cursor.execute("SELECT id FROM circulars WHERE circular_id = 'CIRC-001'")
    circ_id = cursor.fetchone()[0]
    circ_uuid = UUID(bytes=circ_id)
    print('Created circular UUID:', circ_uuid)

repo = AssetRepository(pool)

print()
print('=== POSITIVE TESTS ===')

print()
print('--- replace_assets() ---')
assets = [
    CircularAsset(asset_role='original_pdf', file_path='/data/sebi/circular-001.pdf', mime_type='application/pdf', content_hash='abc123', file_size_bytes=1024),
    CircularAsset(asset_role='extracted_pdf', file_path='/data/sebi/circular-001-extracted.pdf', mime_type='application/pdf'),
]
result = repo.replace_assets(circ_uuid, assets)
print('replace_assets result count:', len(result))
for a in result:
    print(f'  role={a.asset_role} path={a.file_path} mime={a.mime_type}')

print()
print('--- list_assets() ---')
assets = repo.list_assets(circ_uuid)
print('list_assets count:', len(assets))
for a in assets:
    print(f'  role={a.asset_role} path={a.file_path}')

print()
print('--- get_primary_asset() ---')
primary = repo.get_primary_asset(circ_uuid)
print('Primary asset:', primary.asset_role, primary.file_path if primary else None)

print()
print('--- replace_assets() again (replace with single asset) ---')
assets = [
    CircularAsset(asset_role='original_zip', file_path='/data/sebi/circular-001.zip', mime_type='application/zip'),
]
result = repo.replace_assets(circ_uuid, assets)
print('replace_assets result count:', len(result), '(should be 1, zip deleted pdfs)')

print()
print('ALL POSITIVE TESTS PASSED')