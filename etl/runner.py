import subprocess


def run_test():
    """
    Executes the ETL integration test script and logs the output to 'debug.log'.
    """
    subprocess.run(
        ["poetry", "run", "python", "tests/test_etl_integration.py"],
        check=True,
    )


if __name__ == "__main__":
    run_test()
