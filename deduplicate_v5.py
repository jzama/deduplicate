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

newDatasetPath = r"/home/izoom/BIG_LOCAL_DISKS/new_dataset_2021"

temp_folder = r"/home/izoom/BIG_LOCAL_DISKS/temp"

conn = sqlite3.connect("test.db")
c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS file_info(
    fileId TEXT PRIMARY KEY,
    emailId TEXT,
    revisionDate DATE,
    textResume TEXT,
    familyNameCode TEXT,
    createdTime DATE)""")


with open('seen_tars.txt', 'r') as file:
    lines = file.readlines()
    all_folders_completed = [line.rstrip() for line in lines]

all_files_in_year_path = glob(f"{year_path}/*.tar.gz")
all_files_in_year_path_relevant = [i for i in all_files_in_year_path if i not in all_folders_completed]

for a in all_files_in_year_path_relevant:

    tar = tarfile.open(a, "r:gz")
    tar.extractall(path=temp_folder)
    tar.close()

    if len([f.path for f in os.scandir(temp_folder) if f.is_dir()]) > 1:
        assert print("There are more than 1 folder in the temp folder. Please manually extract all the files from both the folders and move them to the temp folder. Delete the empty folders and run the script again")

    folder_path = [f.path for f in os.scandir(temp_folder) if f.is_dir()][0]

    for l in glob(f"{folder_path}/*json_cv"):
        shutil.move(l, temp_folder)

    shutil.rmtree(folder_path)

    all_json_files = glob(f"{temp_folder}/*json_cv")

    for x,i in enumerate(all_json_files):

        file_id = ".".join(all_json_files[0].split("/")[-1].split(".")[0:-1])

        with open(i, 'r') as myfile:
            data=myfile.read()

        obj = json.loads(data)

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

        try:
            given_name = obj['Resume']['StructuredResume']['PersonName']['GivenName']
            family_name = obj['Resume']['StructuredResume']['PersonName']['FamilyName']
            country_code = obj['Resume']['StructuredResume']['ContactMethod']['PostalAddress_main']['CountryCode']
            given_family_code = given_name + family_name + country_code
        except Exception as e:
            given_family_code = ""

        date = obj.get("Resume").get("StructuredResume").get("RevisionDate")

        if not date:
            date = ""

        created_time = obj.get("CreatedTime")

        if not created_time:
            created_time = ""

        if email_id == "":
            if text_resume == "":
                if given_family_code == "":
                    print(f"file: {file_id} skipped because it didn't have email_id, text resume and family name")
                    continue

        if date == "":
            if created_time == "":
                print(f"file: {file_id} did not have revision date and created time")

        if email_id != "":

            old_file_email = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where emailId = '{}'
                """.format(email_id), conn)

            old_file_email_len = len(old_file_email)

        else:

            old_file_email_len = 0


        if text_resume != "":

            old_file_text_resume = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where textResume = '{}'
                """.format(text_resume_hash), conn)

            old_file_text_resume_len = len(old_file_text_resume)

        else:

            old_file_text_resume_len = 0


        if given_family_code != "":

            old_file_given_family_code = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where familyNameCode = '{}'
                """.format(given_family_code), conn)

            old_file_given_family_code_len = len(old_file_given_family_code)

        else:

            old_file_given_family_code_len = 0

        if date != "":

            if old_file_email_len > 0:

                print(f"Email duplicates found for : {file_id}")

                old_file_info = old_file_email.to_dict("records")

                if date <= old_file_info[0].get("revisionDate"):
                    print(f"Discarded {file_id} because it had an older revisionDate than {old_file_info[0].get('fileId')}")
                    continue

                else:
                    print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}/{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{given_family_code}', '{created_time}')""")
                    conn.commit()

            elif old_file_text_resume_len > 0:

                print(f"TextResume duplicates found for : {file_id}")

                old_file_info = old_file_text_resume.to_dict("records")

                if date <= old_file_info[0].get("revisionDate"):
                    print(f"Discarded {file_id} because it had an older revisionDate than {old_file_info[0].get('fileId')}")
                    continue

                else:
                    print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}/{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{given_family_code}', '{created_time}')""")
                    conn.commit()

            elif old_file_given_family_code_len > 0:

                print(f"FamilyCode duplicates found for : {file_id}")

                old_file_info = old_file_given_family_code.to_dict("records")

                if date <= old_file_info[0].get("revisionDate"):
                    print(f"Discarded {file_id} because it had an older revisionDate than {old_file_info[0].get('fileId')}")
                    continue

                else:
                    print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}/{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{given_family_code}', '{created_time}')""")
                    conn.commit()

            else:
                print(f"No duplicates found for : {file_id}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{given_family_code}', '{created_time}')""")
                conn.commit()


        elif created_time != "":

            if old_file_email_len > 0:

                print(f"Email duplicates found for : {file_id}")

                old_file_info = old_file_email.to_dict("records")

                if created_time <= old_file_info[0].get("createdTime"):
                    print(f"Discarded {file_id} because it had an older createdTime than {old_file_info[0].get('fileId')}")
                    continue

            elif old_file_text_resume_len > 0:

                print(f"TextResume duplicates found for : {file_id}")

                old_file_info = old_file_text_resume.to_dict("records")

                if created_time <= old_file_info[0].get("createdTime"):
                    print(f"Discarded {file_id} because it had an older createdTime than {old_file_info[0].get('fileId')}")
                    continue

            elif old_file_given_family_code_len > 0:

                print(f"FamilyCode duplicates found for : {file_id}")

                old_file_info = old_file_given_family_code.to_dict("records")

                if created_time <= old_file_info[0].get("createdTime"):
                    print(f"Discarded {file_id} because it had an older createdTime than {old_file_info[0].get('fileId')}")
                    continue

            else:
                print(f"No duplicates found for : {file_id}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{given_family_code}', '{created_time}')""")
                conn.commit()


    for i in glob(f"{temp_folder}/*"):
        os.remove(i)

    with open('seen_tars.txt', 'a') as f:
        f.write("%s\n" % a)
        all_folders_completed.append(a)
