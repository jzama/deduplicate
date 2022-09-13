import sqlite3
from glob import glob
import os
import shutil
import tarfile
import json
import hashlib

year_path = r"/home/izoom/BI_ARCHIVES.FROM_RINGS/2021"

newDatasetPath = r"/home/izoom/BIG_LOCAL_DISKS/datasetv12"

temp_folder = r"/home/izoom/BIG_LOCAL_DISKS/temp"

tar_files_to_extract = 5


conn = sqlite3.connect("testv13.db")

c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS file_info(
    fileId TEXT UNIQUE,
    emailId TEXT UNIQUE,
    textResume TEXT UNIQUE)""")


with open('seenv13.txt', 'r') as file:
    lines = file.readlines()
    all_folders_completed = [line.rstrip() for line in lines]

all_files_in_year_path = glob(f"{year_path}/*.tar.gz")
all_files_in_year_path_relevant = [i for i in all_files_in_year_path if i not in all_folders_completed]

for b,a in enumerate(all_files_in_year_path_relevant):

    if b == tar_files_to_extract:
        print(f"{tar_files_to_extract} tar files extracted, hence stopping.")
        break

    if len([f.path for f in os.scandir(temp_folder) if f.is_dir()]) > 0:
        assert print("There is already a folder in Temp folder. Please delete that folder.")

    for i in glob(f"{temp_folder}/*"):
        os.remove(i)

#tarfile_name = ".".join(a.split("/")[-1].split(".")[0:-2])

    print("Tar file Extracting...")

    tar = tarfile.open(a, "r:gz")
    tar.extractall(path=temp_folder)
    tar.close()

    print("Tar file Extracted")

    folder_path = [f.path for f in os.scandir(temp_folder) if f.is_dir()][0]

    # for l in glob(f"{folder_path}/*json_cv"):
    #     shutil.move(l, temp_folder)

    # shutil.rmtree(folder_path)

    all_json_files = glob(f"{folder_path}/*json_cv")

    # print(all_json_files)

    for x,i in enumerate(all_json_files):

        duplicate_count = 0

        file_id = ".".join(i.split("/")[-1].split(".")[0:-1])

        # subfolder_name = file_id[2:6]

        try:

            with open(i, 'r') as myfile:
                data=myfile.read()

            obj = json.loads(data)

        except Exception as e:
            print(e)
            continue

        try:
            email_id = obj.get("Resume").get("StructuredResume").get("ContactMethod").get("InternetEmailAddress_main")
        except Exception as e:
            email_id = None

        try:
            text_resume = obj.get("Resume").get("TextResume")
        except Exception as e:
            text_resume = None

        if text_resume:
            text_resume_hash = hashlib.md5(text_resume.encode()).hexdigest()
        else:
            text_resume_hash = None

        if not email_id:
            if not text_resume:
                print(f"file: {file_id} skipped because it didn't have email id and text resume both")
                continue

        # if not os.path.exists(f"{newDatasetPath}/{tarfile_name}/{subfolder_name}"):
        #     os.makedirs(f"{newDatasetPath}/{tarfile_name}/{subfolder_name}")

        # shutil.move(i, f"{newDatasetPath}/{tarfile_name}/{subfolder_name}")

        try:
            c.execute(f"""INSERT INTO file_info VALUES (?, ?, ?)""", (file_id, email_id, text_resume_hash))
            shutil.move(i, newDatasetPath)
        except Exception as e:
            print(e)
            duplicate_count = duplicate_count+1
            continue


    conn.commit()

    shutil.rmtree(folder_path)

    # for i in glob(f"{temp_folder}/*"):
    #     os.remove(i)

    with open('seenv13.txt', 'a') as f:
        f.write("%s\n" % a)
        # all_folders_completed.append(tarfile_name)

    print(f"{len(all_json_files) - duplicate_count} added out of {len(all_json_files)}. And {duplicate_count} duplicates found.")
    print(f"{a} Completed")