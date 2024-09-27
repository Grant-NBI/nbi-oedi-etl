"""
Monitor:  two components

* ETLMonitor class to track incoming files, processed files, uploaded files, transformations, and discrepancies. It can also logger.info a summary of the ETL job. This is a crude monitor. It can be improved by adding more tasks to monitor.

* monitor_idle_tasks to monitor tasks and handle idle timeout.
"""

import asyncio
import hashlib
import json
import os
import tempfile
import time

from log import get_logger

logger = get_logger()


class ETLMonitor:
    """
    ETLMonitor class to track and log the progress of an ETL job.

    Attributes:
        name (str): The name of the ETL job.
        start_time (float): The start time of the ETL job.
        state (dict): A shared dictionary to maintain the counts of fetched, transformed, uploaded files, etc.
        listed_map (HashMap): A hash map used to store hashes of listed files.
        uploaded_map (HashMap): A hash map used to store hashes of uploaded files.

    Note:
        Only 'listed' and 'uploaded' files are tracked using hashes to minimize memory usage.
        Discrepancies are calculated between these two stages.
    """

    def __init__(self, name, start_time, state):
        self.name = name
        self.start_time = start_time
        self.state = state  # Shared state dictionary passed from etl_job
        self.listed_map = HashMap()
        self.uploaded_map = HashMap()

    def record_listed(self, file_key):
        self.state["listed"] += 1
        #keep track of incoming files
        self.listed_map.store_file_key(file_key)
        logger.debug(
            "Listed file: %s, Total listed: %d", file_key, self.state["listed"]
        )

    def record_fetched(self):
        self.state["fetched"] += 1
        logger.debug("Total fetched: %d", self.state["fetched"])

    def record_transferred_to_worker(self):
        self.state["transferred_to_worker"] += 1
        logger.debug(
            "Total transferred to workers: %d", self.state["transferred_to_worker"]
        )

    #! Can't have record_transformed as it's across workers and only use shared dict (monitor is not picklable)

    def record_transferred_to_uploader(self):
        self.state["transferred_to_uploader"] += 1
        logger.debug(
            "Total transferred to uploader: %d", self.state["transferred_to_uploader"]
        )

    def record_uploaded(self, file_key):
        self.state["uploaded"] += 1
        # keep track of outgoing files
        self.uploaded_map.store_file_key(file_key)
        logger.debug(
            "Uploaded file: %s, Total uploaded: %d", file_key, self.state["uploaded"]
        )

    def get_discrepancies(self):
        discrepancies = []

        listed_hashes = set(self.listed_map.hash_to_key_map.keys())
        uploaded_hashes = set(self.uploaded_map.hash_to_key_map.keys())

        missing_in_upload = listed_hashes - uploaded_hashes

        if missing_in_upload:
            missing_files = [
                self.listed_map.retrieve_file_key(h) for h in missing_in_upload
            ]
            discrepancies.append(
                {
                    "stage": "Listed but not Uploaded",
                    "files": missing_files,
                }
            )

        return discrepancies

    def print_summary(self):
        end_time = time.time()
        elapsed_time = end_time - self.start_time

        summary = {
            "job_name": self.name,
            "start_time": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(self.start_time)
            ),
            "total_time_seconds": round(elapsed_time, 2),
            "end_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)),
            "total_files_listed": self.state["listed"],
            "total_files_fetched": self.state["fetched"],
            "total_files_transferred_to_worker": self.state["transferred_to_worker"],
            "total_transformed_files": self.state["transformed"],
            "total_files_transferred_to_uploader": self.state[
                "transferred_to_uploader"
            ],
            "total_uploaded_files": self.state["uploaded"],
            "discrepancies_count": len(self.get_discrepancies()),
            "discrepancies": self.get_discrepancies(),
        }

        logger.info("ETL Job Summary: %s", json.dumps(summary, indent=4))

        # Cleanup temporary hash maps
        self.listed_map.cleanup()
        self.uploaded_map.cleanup()


class HashMap:
    """
    A hash map that stores file keys as hashes and saves the mapping between hashes and file keys
    in a temporary file for memory efficiency.

    Attributes:
        temp_file (str): The path to the temporary file storing the hash-to-key mappings.
        hash_to_key_map (dict): An in-memory map for reverse lookup of file keys based on their hash.
    """

    def __init__(self):
        # Create a temporary file for storing hash-to-key mappings
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, mode="w+")
        self.hash_to_key_map = {}

    def _hash_key(self, file_key):
        """
        Generate a fast and compact hash for the given file key.

        Args:
            file_key (str): The file key to hash.

        Returns:
            str: A compact hash for the given file key.
        """
        return hashlib.blake2b(file_key.encode("utf-8"), digest_size=8).hexdigest()

    def store_file_key(self, file_key):
        """
        Store a file key by generating its hash and writing the mapping to the temporary file.

        Args:
            file_key (str): The file key to store.

        Returns:
            str: The hash of the file key.
        """
        file_hash = self._hash_key(file_key)
        self.hash_to_key_map[file_hash] = file_key
        # Write to temp file for later reverse lookup
        with open(self.temp_file.name, "a", encoding="utf-8") as f:
            f.write(f"{file_hash} {file_key}\n")
        return file_hash

    def retrieve_file_key(self, file_hash):
        """
        Retrieve the file key corresponding to the given hash.

        Args:
            file_hash (str): The hash of the file key to retrieve.

        Returns:
            str: The file key corresponding to the hash, or None if not found.
        """
        # First check in the in-memory map
        if file_hash in self.hash_to_key_map:
            return self.hash_to_key_map[file_hash]

        # If not found, search the temp file
        with open(self.temp_file.name, "r", encoding="utf-8") as f:
            for line in f:
                stored_hash, file_key = line.strip().split(" ")
                if stored_hash == file_hash:
                    return file_key
        return None

    def cleanup(self):
        """
        Clean up the temporary file after use.
        """
        try:
            os.remove(self.temp_file.name)
        except OSError as e:
            print(f"Error: {e.strerror} - {e.filename}")

    def __del__(self):
        # Ensure cleanup happens even if not called manually
        self.cleanup()


async def monitor_idle_tasks(
    queues, idle_timeout_in_minutes, tasks_to_cancel, process_executor
):
    """
    Monitors the provided queues and handles idle timeout by cancelling tasks and shutting down the process executor.

    Args:
        queues (list): A list of queue objects to monitor.
        idle_timeout_in_minutes (int): The idle timeout duration in minutes.
        tasks_to_cancel (list): A list of asyncio tasks to cancel upon timeout.
        process_executor (concurrent.futures.ProcessPoolExecutor): The process executor to shut down upon timeout.

    Returns:
        None

    Behavior:
        - Continuously checks if all queues are empty.
        - If all queues are empty and the idle timeout has passed, cancels the provided tasks and shuts down the process executor.
        - Resets the idle timer if activity is detected in any queue.
        - Sleeps for 30 seconds between checks.
    """
    last_activity_time = time.time()
    while True:
        # Check if all queues are empty
        if all_queues_empty(queues):
            current_time = time.time()
            elapsed_idle_time_in_minutes = (current_time - last_activity_time) / 60

            # Check if idle timeout has passed
            if elapsed_idle_time_in_minutes > idle_timeout_in_minutes:
                logger.info(
                    "1077: Idle timeout reached after %.2f minutes. Terminating process...",
                    elapsed_idle_time_in_minutes,
                )

                # Cancel the tasks
                for task in tasks_to_cancel:
                    task.cancel()

                # Shutdown the process executor
                process_executor.shutdown(wait=False)
                return  # Exit monitor task
        else:
            # Activity detected => reset
            logger.info("1087: Activity detected. Resetting idle timer.")
            last_activity_time = time.time()

        # Check every 30 seconds
        await asyncio.sleep(30)


def all_queues_empty(queues):
    """
    Helper function to check if all provided queues are empty.
    """
    return all(q.empty() for q in queues)
