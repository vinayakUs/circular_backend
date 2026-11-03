python -m ingestion.scrapper.runner --sources SEBI --from-date 2019-01-01 --to-date 2020-01-01


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


 Terminal 1 — Start the Prefect server:
  prefect server start
  
  Terminal 2 — Start the worker:
  PREFECT_API_URL=http://localhost:4200/api prefect worker start --type process
  --name ingestion-worker
  
  Terminal 3 — Deploy the flow:
  cd /root/circular_backend
  prefect deploy --prefect-file prefect.yaml

  Trigger a run (manual):
  prefect deployment run "scraper-flow/scraper-deployment"

  Check runs in the UI:
  http://localhost:4200


  find /root/circular_backend -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null; echo "Done"

  
nohup python -m ingestion.processor.runner --limit 50 --log-file /root circular_backend/logs/processor.log > /dev/null 2>&1 &

nohup python -m ingestion.indexer.runner --log-file /root/circular_backend/logs/indexer.log > /dev/null 2>&1 &


  mkdir -p /root/circular_backend/logs

  nohup python -m services.notification_worker \                                                                                                      
    --log-file /root/circular_backend/logs/notification.log \
    > /dev/null 2>&1 &






LD_LIBRARY_PATH=/root/circular_backend/.venv/lib NUMBA_THREADING_LAYER=tbb python run.py

LD_LIBRARY_PATH=/root/circular_backend/.venv/lib NUMBA_THREADING_LAYER=tbb python run.py --log-file /root/circular_backend/logs/app.log



  python -m ingestion.scrapper.runner --sources SEBI --from-date 2025-06-24 --to-date 2026-06-24 --log-file /root/circular_backend/logs/scrapper.log

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
run-indexer --record-id ea0ffae5-c3dc-41d3-8b38-67e80664dc26


 Terminal 1 — Start the Prefect server:
  prefect server start
  
  Terminal 2 — Start the worker:
  PREFECT_API_URL=http://localhost:4200/api prefect worker start --type process
  --name ingestion-worker
  
  Terminal 3 — Deploy the flow:
  cd /root/circular_backend
  prefect deploy --prefect-file prefect.yaml

  Trigger a run (manual):
  prefect deployment run "scraper-flow/scraper-deployment"

  Check runs in the UI:
  http://localhost:4200



psql -h localhost -U circulardbusr -d circulardb

user - circulardbusr
password = circulardbusr


ps -eo pid,etime,cmd | grep ingestion*
 PIP_REQUIRE_VIRTUALENV=false pip install -e . -v --no-deps --no-build-isolation



nohup /mnt/softwares/anaconda/miniconda3/envs/circular_env/bin/python -u /mnt/uat/test/circular/backend/run.py > /mnt/uat/test/circular/backend/logs/nohup.log 2>&1 &

  find /root/circular_backend -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null; echo "Done"
nohup env NUMBA_THREADING_LAYER=tbb /mnt/softwares/anaconda/miniconda3/envs/circular_env/bin/python /mnt/uat/circular/backend/run.py > /mnt/uat/circular/backend/logs/nohup.out 2>&1 &
  
nohup python -m ingestion.processor.runner --limit 500 --log-file /mnt/uat/circular/backend/logs/processor.log > /dev/null 2>&1 &
  find /root/circular_backend -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null; echo "Done"

  
nohup python -m ingestion.processor.runner --limit 50 --log-file /root circular_backend/logs/processor.log > /dev/null 2>&1 &


  mkdir -p /root/circular_backend/logs

  nohup python -m services.notification_worker \                                                                                                      
    --log-file /root/circular_backend/logs/notification.log \
    > /dev/null 2>&1 &
