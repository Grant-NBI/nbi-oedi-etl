import os
import subprocess
import shutil
import zipfile
from tempfile import mkdtemp

def package_etl():
    # Define paths
    etl_root = os.getcwd()
    package_dir_original = os.path.join(etl_root, "oedi-etl")  # Original directory with hyphen
    package_build_dir = os.path.join(etl_root, "package")
    zip_filename = os.path.join(package_build_dir, "oedi-etl.zip")
    requirements_txt = os.path.join(package_build_dir, "requirements.txt")
    dependencies_dir = os.path.join(package_build_dir, "dependencies")

    # Prep the build directory
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
    if os.path.exists(dependencies_dir):
        shutil.rmtree(dependencies_dir)
    os.makedirs(package_build_dir, exist_ok=True)

    # Extract requirements.txt and install dependencies
    subprocess.run(["poetry", "export", "-f", "requirements.txt", "--output", requirements_txt], check=True)
    subprocess.run(["pip", "install", "-r", requirements_txt, "--target", dependencies_dir], check=True)

    # Create a temporary directory to store the copied and renamed oedi-etl package
    temp_dir = mkdtemp()

    try:
        # Copy the original directory to the temporary directory and rename it to oedi_etl
        package_dir_temp = os.path.join(temp_dir, "oedi_etl")
        shutil.copytree(package_dir_original, package_dir_temp)

        # Package the dependencies and source code
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add dependencies to the zip
            for root, dirs, files in os.walk(dependencies_dir):
                dirs[:] = [d for d in dirs if d != '__pycache__']  # Exclude __pycache__ dirs
                for file in files:
                    if file.endswith('.pyc'):  # Skip .pyc files
                        continue
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=dependencies_dir)
                    zipf.write(file_path, arcname)

            # Add the copied and renamed oedi_etl package to the zip
            for root, dirs, files in os.walk(package_dir_temp):
                dirs[:] = [d for d in dirs if d != '__pycache__']  # Exclude __pycache__ dirs
                for file in files:
                    if file.endswith('.pyc'):
                        continue
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=temp_dir)  # Reference the temp dir
                    zipf.write(file_path, arcname)

    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)

    print(f"Packaged oedi_etl with dependencies as {zip_filename}")

if __name__ == "__main__":
    package_etl()
