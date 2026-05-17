import sys
sys.path.insert(0, '.')

from db import get_db_client
from ingestion.repository.asset_repository import AssetRepository, CircularAsset
from uuid import UUID, uuid4

pool = get_db_client().get_pool()
repo = AssetRepository(pool)

print('=== NEGATIVE TESTS ===')

print()
print('--- list_assets() non-existent circular_id ---')
result = repo.list_assets(uuid4())
print('list_assets for non-existent UUID:', result, '(expected [])')

print()
print('--- get_primary_asset() non-existent circular_id ---')
result = repo.get_primary_asset(uuid4())
print('get_primary_asset for non-existent UUID:', result, '(expected None)')

print()
print('--- replace_assets() with empty list (should delete all) ---')
with pool.acquire() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM circulars WHERE circular_id = 'CIRC-001'")
    circ_id = bytes(cursor.fetchone()[0])
circ_uuid = UUID(bytes=circ_id)
result = repo.replace_assets(circ_uuid, [])
print('replace_assets with empty list result count:', len(result), '(should be 0)')

print()
print('--- replace_assets() with None in optional fields ---')
assets = [
    CircularAsset(asset_role='original_pdf', file_path='/data/test.pdf', mime_type=None, content_hash=None),
]
result = repo.replace_assets(circ_uuid, assets)
print('replace_assets with None optionals:', len(result), result[0].mime_type if result else None, result[0].content_hash if result else None)

print()
print('ALL NEGATIVE TESTS PASSED')