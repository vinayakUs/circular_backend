  python -m ingestion.scrapper.runner --sources NSE,SEBI --from-date 2026-05-10 --to-date 2026-05-20

  run-indexer --delete-index --reset-db --reset-bloom


Create the ES index once:

run-indexer --setup-index

Index pending fetched circulars:

run-indexer

Useful variants:

run-indexer --batch-size 100
run-indexer --record-id <circular-record-uuid>
run-indexer
run-indexer --batch-size 100
run-indexer --setup-index
run-indexer --delete-index
run-indexer --delete-index --reset-db --reset-bloom
run-indexer --record-id 123e4567-e89b-12d3-a456-426614174000
