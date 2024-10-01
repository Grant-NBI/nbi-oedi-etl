"""
#! must be built for python3.9 (The latest supported for pythonshell in AWS Glue)
This script is executed via `poetry run build`, which triggers the wheel-building process.
Inside this script, a temporary Python 3.9 virtual environment is created to ensure
compatibility with AWS Glue, which requires packages built for Python 3.9.

Note:  `poetry run build`: Triggers this build and this script itself runs in a `poetry environment`, which is most certainly not a Python 3.9 environment unless you selected python3.9 environment

"""

import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile


def find_python39():
    """
    Attempts to locate Python 3.9 executable on both Windows and Unix-like systems.
    Looks in standard locations and parses the output of available python executables
    to find one matching Python 3.9.
    """
    try:
        # Windows: use 'where python'
        if platform.system() == "Windows":
            result = subprocess.run(
                ["where", "python"], capture_output=True, text=True, check=True
            )
        else:
            # Posix: use 'which python3'
            result = subprocess.run(
                ["which", "python3"], capture_output=True, text=True, check=True
            )

        if result.returncode != 0:
            raise FileNotFoundError("Unable to find any Python executable.")

        python_paths = result.stdout.strip().splitlines()

        # Regex to match Python 3.9 versions
        python39_re = re.compile(r"python(?:3\.9)?(?:\.exe)?$", re.IGNORECASE)

        # Iterate over found Python executables and find a Python 3.9 match
        for path in python_paths:
            if python39_re.search(path):
                return path

        raise FileNotFoundError("Python 3.9 not found on the system.")

    except Exception as e:
        raise FileNotFoundError(f"Error locating Python 3.9: {str(e)}")


def create_temp_venv():
    """
    Create a temporary Python 3.9 virtual environment and install setuptools and wheel.
    """
    temp_dir = tempfile.mkdtemp()
    print(f"Creating a temporary Python 3.9 virtual environment in: {temp_dir}")

    try:
        python39_exec = find_python39()  # Find Python39
        print(f"Found Python 3.9 at: {python39_exec}")

        venv_path = os.path.join(temp_dir, 'venv')

        # new venv with python3.9
        subprocess.run([python39_exec, '-m', 'venv', venv_path], check=True)

        # Activate venv with python3.9
        python_exec = os.path.join(venv_path, 'Scripts', 'python' if platform.system() == 'Windows' else 'bin/python')

        # need setuptools & tomli, wheel to build the wheel
        print("Installing setuptools and wheel in the virtual environment...")
        subprocess.run([python_exec, '-m', 'pip', 'install', '--upgrade', 'pip', 'setuptools', 'wheel', 'tomli'], check=True)

        return temp_dir, python_exec

    except FileNotFoundError as e:
        print(e)
        sys.exit(1)  #





def clean_up(temp_dir):
    """
    Clean up the temporary virtual environment.
    """
    print(f"Cleaning up temporary environment: {temp_dir}")
    shutil.rmtree(temp_dir)


def build_wheel(python_exec):
    print("Building the wheel inside the Python 3.9 environment...")
    subprocess.run([python_exec, "setup.py", "bdist_wheel"], check=True)


def build():
    temp_dir, python_exec = create_temp_venv()
    try:
        build_wheel(python_exec)
    finally:
        clean_up(temp_dir)


if __name__ == "__main__":
    build()
