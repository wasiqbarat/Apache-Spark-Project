import os
import sys
import shutil
import urllib.request
from pathlib import Path

def setup_hadoop():
    # Define paths
    hadoop_home = Path("C:/hadoop")
    bin_dir = hadoop_home / "bin"
    libexec_dir = hadoop_home / "libexec"
    etc_dir = hadoop_home / "etc/hadoop"
    
    # Create directories
    for dir in [bin_dir, libexec_dir, etc_dir]:
        dir.mkdir(parents=True, exist_ok=True)
    
    # Download winutils.exe for Hadoop 2.8.0
    winutils_path = bin_dir / "winutils.exe"
    if not winutils_path.exists():
        print("Downloading winutils.exe for Hadoop 2.8.0...")
        winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-2.8.0/bin/winutils.exe"
        try:
            urllib.request.urlretrieve(winutils_url, str(winutils_path))
            print("winutils.exe downloaded successfully")
        except Exception as e:
            print(f"Error downloading winutils.exe: {e}")
            print("Please download winutils.exe manually from:")
            print("https://github.com/cdarlint/winutils/blob/master/hadoop-2.8.0/bin/winutils.exe")
            print(f"And place it in: {bin_dir}")
    
    # Set environment variables
    os.environ['HADOOP_HOME'] = str(hadoop_home.absolute())
    os.environ['PATH'] = str(bin_dir.absolute()) + os.pathsep + os.environ['PATH']
    os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME', '')  # Ensure JAVA_HOME is set
    
    # Create dummy hadoop.dll if it doesn't exist (sometimes needed on Windows)
    hadoop_dll = bin_dir / "hadoop.dll"
    if not hadoop_dll.exists():
        try:
            with open(hadoop_dll, 'wb') as f:
                f.write(b'')  # Create empty binary file
        except Exception as e:
            print(f"Error creating hadoop.dll: {e}")

    # Create hdfs-config.sh
    hdfs_config_content = """#!/bin/bash
HADOOP_HOME="C:/hadoop"
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_HOME HADOOP_CONF_DIR
"""
    
    with open(libexec_dir / "hdfs-config.sh", "w", newline="\n") as f:
        f.write(hdfs_config_content)
    
    # Create core-site.xml
    core_site_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:8020</value>
    </property>
</configuration>
"""
    
    with open(etc_dir / "core-site.xml", "w") as f:
        f.write(core_site_content)

    return hadoop_home

if __name__ == "__main__":
    hadoop_home = setup_hadoop()
    print(f"Hadoop environment setup completed at {hadoop_home}")
    print(f"HADOOP_HOME = {os.environ['HADOOP_HOME']}")
