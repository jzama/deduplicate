import sqlite3
from datetime import datetime
from glob import glob
import os
import shutil
import pandas as pd
import tarfile
import json
import hashlib

year_path = r"/home/izoom/BI_ARCHIVES.FROM_RINGS/2021"

newDatasetPath = r"/home/izoom/BIG_LOCAL_DISKS/datasetv11"

temp_folder = r"/home/izoom/BIG_LOCAL_DISKS/temp"


conn = sqlite3.connect("testv11.db")

c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS file_info(
    fileId TEXT PRIMARY KEY,
    emailId TEXT,
    revisionDate DATE,
    textResume TEXT,
    createdTime DATE,
    CREATE INDEX emailId_index ON file_info(emailId),
    CREATE INDEX textResume_index ON file_info(textResume))""")


with open('seenv11.txt', 'r') as file:
    lines = file.readlines()
    all_folders_completed = [line.rstrip() for line in lines]

all_files_in_year_path = glob(f"{year_path}/*.tar.gz")
all_files_in_year_path_relevant = [i for i in all_files_in_year_path if i not in all_folders_completed]

for a in all_files_in_year_path_relevant:

    if len([f.path for f in os.scandir(temp_folder) if f.is_dir()]) > 0:
        assert print("There is already a folder in Temp folder. Please delete that folder.")

    for i in glob(f"{temp_folder}/*"):
        os.remove(i)

    print("Tar file Extracting...")

    tar = tarfile.open(a, "r:gz")
    tar.extractall(path=temp_folder)
    tar.close()

    print("Tar file Extracted")

    folder_path = [f.path for f in os.scandir(temp_folder) if f.is_dir()][0]

    for l in glob(f"{folder_path}/*json_cv"):
        shutil.move(l, temp_folder)

    shutil.rmtree(folder_path)

    all_json_files = glob(f"{temp_folder}/*json_cv")

    # print(all_json_files)

    for x,i in enumerate(all_json_files):

        file_id = ".".join(i.split("/")[-1].split(".")[0:-1])

        # print(file_id)
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
            email_id = ""

        try:
            text_resume = obj.get("Resume").get("TextResume")
        except Exception as e:
            text_resume = ""

        if text_resume != "":
            text_resume_hash = hashlib.md5(text_resume.encode()).hexdigest()


        date = obj.get("Resume").get("StructuredResume").get("RevisionDate")

        if not date:
            date = ""

        created_time = obj.get("CreatedTime")

        if not created_time:
            created_time = ""

        if email_id == "":
            if text_resume == "":
                print(f"file: {file_id} skipped because it didn't have email_id, text resume and family name")
                continue

        if date == "":
            if created_time == "":
                print(f"file: {file_id} did not have revision date and created time")


        old_file_id = pd.read_sql(
            """
            SELECT *
            FROM file_info
            where fileId = '{}'
            LIMIT 1
            """.format(file_id), conn)

        old_file_id_len = len(old_file_id)

        if old_file_id_len != 0:
            print(f"File:{file_id} was seen before, hence skipping")
            continue


        if email_id != "":

            old_file_email = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where emailId = '{}'
                LIMIT 1
                """.format(email_id), conn)

            old_file_email_len = len(old_file_email)

        else:

            old_file_email_len = 0


        if old_file_email_len == 0:

            if text_resume != "":

                old_file_text_resume = pd.read_sql(
                    """
                    SELECT *
                    FROM file_info
                    where textResume = '{}'
                    LIMIT 1
                    """.format(text_resume_hash), conn)

                old_file_text_resume_len = len(old_file_text_resume)

            else:

                old_file_text_resume_len = 0

            # old_file_text_resume_len = c.execute("SELECT * FROM file_info WHERE textResume = '{}'".format(text_resume_hash))

        if date != "":

            if old_file_email_len > 0:

                # print(f"Email duplicates found for : {file_id}")

                old_file_info = old_file_email.to_dict("records")

                if date <= old_file_info[0].get("revisionDate"):
                    # print(f"Discarded {file_id} because it had an older revisionDate than {old_file_info[0].get('fileId')}")
                    continue

                else:
                    # print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}/{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{created_time}')""")
                    conn.commit()


            elif old_file_text_resume_len > 0:

                # print(f"TextResume duplicates found for : {file_id}")

                old_file_info = old_file_text_resume.to_dict("records")

                if date <= old_file_info[0].get("revisionDate"):
                    # print(f"Discarded {file_id} because it had an older revisionDate than {old_file_info[0].get('fileId')}")
                    continue

                else:
                    # print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}/{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{created_time}')""")
                    conn.commit()

            else:
                # print(f"No duplicates found for : {file_id}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{created_time}')""")
                conn.commit()


        elif created_time != "":

            if old_file_email_len > 0:

                # print(f"Email duplicates found for : {file_id}")

                old_file_info = old_file_email.to_dict("records")

                if created_time <= old_file_info[0].get("createdTime"):
                    # print(f"Discarded {file_id} because it had an older createdTime than {old_file_info[0].get('fileId')}")
                    continue

            elif old_file_text_resume_len > 0:

                # print(f"TextResume duplicates found for : {file_id}")

                old_file_info = old_file_text_resume.to_dict("records")

                if created_time <= old_file_info[0].get("createdTime"):
                    # print(f"Discarded {file_id} because it had an older createdTime than {old_file_info[0].get('fileId')}")
                    continue

            else:
                # print(f"No duplicates found for : {file_id}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{created_time}')""")
                conn.commit()


    for i in glob(f"{temp_folder}/*"):
        os.remove(i)

    with open('seenv11.txt', 'a') as f:
        f.write("%s\n" % a)
        all_folders_completed.append(a)

    print(f"{a} Completed")
