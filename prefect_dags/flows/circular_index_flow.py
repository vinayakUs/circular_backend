"""Prefect flow for circular indexing and notification."""
from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger
from prefect.task_runners import ProcessPoolTaskRunner
from prefect_dags.tasks.circular_index_tasks import noop, index_circular, notify_circular, process_circular, run_designation_processor, run_nse_processor


@flow(name="circular-index", log_prints=True, task_runner=ProcessPoolTaskRunner(max_workers=5))
def circular_index(
    circular_id: str,
    needs_index: bool = False,
    needs_process: bool = False,
    needs_notify: bool = False,
) -> dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"circular-index circular_id={circular_id} "
                f"needs_index={needs_index} needs_process={needs_process} needs_notify={needs_notify}")

    results = {}

    # parallel index + notify
     
    index_future = (index_circular.submit(circular_id) if needs_index else noop.submit("index"))
    notify_future = (notify_circular.submit(circular_id) if needs_notify else noop.submit("notify"))

    # wait for index before process subflow, but notify can run in parallel
    index_result = index_future.result()
    results["index"] = index_result
    logger.info(f"index_circular result={index_result}")

    if needs_process:
        nse_future = run_nse_processor.submit(circular_id, index_result)
        designation_future = run_designation_processor.submit(circular_id, index_result)

    results["notify"] = notify_future.result()  # wait for notify to complete


    results["applicability"] = nse_future.result() 
    results["designation"] = designation_future.result() 




    # if needs_index:
    #     index_result = index_circular(circular_id)
    #     results["index"] = index_result
    #     logger.info(f"index_circular result={index_result}")



    # process_future = (process_circular.submit(circular_id) if needs_process else noop.submit("process"))

    # notify_future = (notify_circular.submit(circular_id) if needs_notify else noop.submit("notify"))
    
    # results["process"] = process_future.result()
    # results["notify"] = notify_future.result()

    # # After index, process and notify run in parallel via ProcessPoolTaskRunner
    # if needs_process and needs_notify:
    #     process_future = process_circular.submit(circular_id)
    #     notify_future = notify_circular.submit(circular_id)
    #     results["process"] = process_future.result()
    #     results["notify"] = notify_future.result()
    # elif needs_process:
    #     results["process"] = process_circular(circular_id)
    # elif needs_notify:
    #     results["notify"] = notify_circular(circular_id)

    return results