"""
Transformer: a task or set of tasks that transform the data into a different format or structure. For now, only aggregation is supported. This can be expanded to include more complex transformations or multiple tasks as needed.

#TODO: One example of a useful transformation is filtering specific columns. Another is partitioning based on date (the `toggle_date_partition` option is was originally planned and removed.). Another example could be a lightweight join to optimize frequent queries, though this would increase storage and ETL processing costs. You can even round up the floating point numbers (lot of unpractical decimals!) to save query scan as well as storage costs...these are not to mention the other functional transformations you may want to do on the data
"""

import datetime
import os
import time
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
from log import get_logger

logger = get_logger()


def aggregate_to_one_hour(table, worker_id):
    """
    Aggregates 15-minute data into 1-hour intervals.
    """
    MILLISECONDS_IN_AN_HOUR = 3600 * 1000
    timestamp_column = table["timestamp"]
    floored_timestamps = []

    # Iterate through each chunk in the ChunkedArray
    for chunk in timestamp_column.chunks:
        # Convert each datetime object to milliseconds
        floored_chunk = [
            int(ts.timestamp() * 1000)
            // MILLISECONDS_IN_AN_HOUR
            * MILLISECONDS_IN_AN_HOUR
            for ts in chunk.to_pylist()
        ]

        # Convert the floored timestamps back
        floored_chunk_as_datetime = [
            datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)
            for ts in floored_chunk
        ]

        floored_timestamps.append(pa.array(floored_chunk_as_datetime))

    # Back into a single ChunkedArray
    floored_timestamps_array = pa.chunked_array(floored_timestamps)

    # Replace timestamp -> floored timestamps
    table = table.remove_column(table.schema.get_field_index("timestamp"))
    table = table.append_column("timestamp", floored_timestamps_array)

    # Now group by floored_timestamp
    grouped_table = table.group_by("timestamp")

    # Aggregate (note that, by this point, the floored_timestamp is only of hour resolution and the aggregation will group rows with the same hour)
    #! see TODO in etl_job.py
    aggregation = [
        (
            "bldg_id",
            "min",
        ),  #! Since all values in a group should be the same, 'min' is used here and assume that the bldg_id is the same for all rows in a group
        ("timestamp", "min"),  # all values are floored to the same hour
        # aggregate all other columns as mean (see TODO above)
        # ("out.district_cooling.cooling.energy_consumption", "mean"),
        # ("out.district_cooling.cooling.energy_consumption_intensity", "mean"),
        # ("out.district_cooling.total.energy_consumption", "mean"),
        # ("out.district_cooling.total.energy_consumption_intensity", "mean"),
        # ("out.district_heating.heating.energy_consumption", "mean"),
        # ("out.district_heating.heating.energy_consumption_intensity", "mean"),
        # ("out.district_heating.total.energy_consumption", "mean"),
        # ("out.district_heating.total.energy_consumption_intensity", "mean"),
        # ("out.district_heating.water_systems.energy_consumption", "mean"),
        # ("out.district_heating.water_systems.energy_consumption_intensity", "mean"),
        # ("out.electricity.cooling.energy_consumption", "mean"),
        # ("out.electricity.cooling.energy_consumption_intensity", "mean"),
        # ("out.electricity.exterior_lighting.energy_consumption", "mean"),
        # ("out.electricity.exterior_lighting.energy_consumption_intensity", "mean"),
        # ("out.electricity.fans.energy_consumption", "mean"),
        # ("out.electricity.fans.energy_consumption_intensity", "mean"),
        # ("out.electricity.heat_recovery.energy_consumption", "mean"),
        # ("out.electricity.heat_recovery.energy_consumption_intensity", "mean"),
        # ("out.electricity.heat_rejection.energy_consumption", "mean"),
        # ("out.electricity.heat_rejection.energy_consumption_intensity", "mean"),
        # ("out.electricity.heating.energy_consumption", "mean"),
        # ("out.electricity.heating.energy_consumption_intensity", "mean"),
        # ("out.electricity.interior_equipment.energy_consumption", "mean"),
        # ("out.electricity.interior_equipment.energy_consumption_intensity", "mean"),
        # ("out.electricity.interior_lighting.energy_consumption", "mean"),
        # ("out.electricity.interior_lighting.energy_consumption_intensity", "mean"),
        # ("out.electricity.pumps.energy_consumption", "mean"),
        # ("out.electricity.pumps.energy_consumption_intensity", "mean"),
        # ("out.electricity.refrigeration.energy_consumption", "mean"),
        # ("out.electricity.refrigeration.energy_consumption_intensity", "mean"),
        # ("out.electricity.total.energy_consumption", "mean"),
        # ("out.electricity.total.energy_consumption_intensity", "mean"),
        # ("out.electricity.water_systems.energy_consumption", "mean"),
        # ("out.electricity.water_systems.energy_consumption_intensity", "mean"),
        # ("out.natural_gas.heating.energy_consumption", "mean"),
        # ("out.natural_gas.heating.energy_consumption_intensity", "mean"),
        # ("out.natural_gas.interior_equipment.energy_consumption", "mean"),
        # ("out.natural_gas.interior_equipment.energy_consumption_intensity", "mean"),
        # ("out.natural_gas.total.energy_consumption", "mean"),
        # ("out.natural_gas.total.energy_consumption_intensity", "mean"),
        # ("out.natural_gas.water_systems.energy_consumption", "mean"),
        # ("out.natural_gas.water_systems.energy_consumption_intensity", "mean"),
        # ("out.other_fuel.cooling.energy_consumption", "mean"),
        # ("out.other_fuel.cooling.energy_consumption_intensity", "mean"),
        # ("out.other_fuel.heating.energy_consumption", "mean"),
        # ("out.other_fuel.heating.energy_consumption_intensity", "mean"),
        # ("out.other_fuel.total.energy_consumption", "mean"),
        # ("out.other_fuel.total.energy_consumption_intensity", "mean"),
        # ("out.other_fuel.water_systems.energy_consumption", "mean"),
        ("out.site_energy.total.energy_consumption", "mean"),
        ("out.site_energy.total.energy_consumption_intensity", "mean"),
    ]
    try:
        logger.debug(f"1232: Aggregation started in Worker {worker_id}...")
        aggregated_table = grouped_table.aggregate(aggregation)
        logger.debug(f"1233: Worker-{worker_id} Aggregation successful")
        return aggregated_table
    except Exception as e:
        logger.error(f"1234: Error in Worker-{worker_id} during aggregation: {e}")


def worker_process(process_input_queue, process_result_queue, state):
    """
    Worker process that continuously pulls files from the process input queue, processes them,
    and pushes the results to the process result queue.
    """
    worker_id = os.getpid()  # Get the unique process ID
    logger.info(f"1227: Worker process started ${worker_id}...")

    while True:
        try:
            if not process_input_queue.empty():
                file_key_data = process_input_queue.get()

                if file_key_data is None:  # Poison pill received
                    logger.info(f"1228: Worker-{worker_id} Poison pill received in worker")
                    process_result_queue.put(None)
                    break

                # Process the file
                file_key, file_bytes = file_key_data
                logger.info(f"1229: Worker-{worker_id} Processing file: {file_key} ...")

                #! Skip metadata files (no transformation on them for now) by checking for '/metadata' in file_key
                #!Assumption: metadata files will always have '/metadata' in the key
                #TODO! Ideally, this should bypass the transformation pipeline (unfortunately, we didn't implement any bypass mechanism in the pipeline)
                if '/metadata' in file_key:
                    logger.debug(f"1230: Worker-{worker_id} found metadata file {file_key}. Skipping...")
                    process_result_queue.put((file_key, file_bytes))
                    logger.debug(f"1231: Worker-{worker_id} Enqueued metadata file without transformation: {file_key}")
                    continue

                file_data = BytesIO(file_bytes)
                table = pq.read_table(file_data)
                transformed_table = aggregate_to_one_hour(table, worker_id)

                output_buffer = BytesIO()
                pq.write_table(transformed_table, output_buffer, compression="snappy")
                output_buffer.seek(0)  # Reset buffer position
                transformed_bytes = output_buffer.read()

                logger.info(
                    f"1236: Worker-{worker_id} Processed file: {file_key} with size {len(transformed_bytes)} bytes"
                )

                # Enqueue in a process-safe queue
                process_result_queue.put((file_key, transformed_bytes))
                state["transformed"] += 1

            else:
                # Avoid busy waiting
                time.sleep(1)

        except Exception as e:
            logger.error(f"1238: Error in Worker-{worker_id} processing file {file_key}: {e}")
            raise
