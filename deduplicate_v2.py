import sqlite3
from datetime import datetime
from glob import glob
import os
import shutil
import pandas as pd
import tarfile
import json
import hashlib

year_path = r"/home/izoom/BIG_LOCAL_DISKS/new_dataset_2021"

newDatasetPath = r"/home/izoom/BI_ARCHIVES.FROM_RINGS/new_dataset_2021"

temp_folder = r"/home/izoom/BIG_LOCAL_DISKS/temp"

conn = sqlite3.connect("test.db")
c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS file_info(
    fileId TEXT PRIMARY KEY,
    emailId TEXT,
    date DATE,
    textResume TEXT,
    monthsOfWork INTEGER)""")

df_email = pd.read_sql(
    """
    SELECT emailId
    FROM file_info
    """, conn
)

all_emails_list = list(df_email['emailId'])

df_file_id = pd.read_sql(
    """
    SELECT fileId
    FROM file_info
    """, conn
)

all_file_id_list = list(df_file_id['fileId'])

df_text_resume = pd.read_sql(
    """
    SELECT textResume
    FROM file_info
    """, conn
)

all_text_resume_list = list(df_text_resume['textResume'])

with open('seen_tars.txt', 'r') as file:
    lines = file.readlines()
    all_folders_completed = [line.rstrip() for line in lines]


all_files_in_year_path = glob(f"{year_path}/*.tar.gz")
all_files_in_year_path_relevant = [i for i in all_files_in_year_path if i not in all_folders_completed]

for a in all_files_in_year_path_relevant:

    tar = tarfile.open(a, "r:gz")
    tar.extractall(path=temp_folder)
    tar.close()

    # folder_path = [f.path for f in os.scandir(temp_folder) if f.is_dir()][0]
    # print(folder_path)

    all_json_files = glob(f"{temp_folder}/*json_cv")
    # print(all_json_files)

    length_of_folder = len(all_json_files)

    for x,i in enumerate(all_json_files):

        file_id = i.split("/")[-1].split(".")[0]

        if file_id in all_file_id_list:
            continue

        # read file
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

        date = obj.get("CreatedTime")

        employment_history = obj.get("Resume").get("StructuredResume").get("EmploymentHistory")

        months_of_work = sum([i.get("MonthsOfWork") for i in employment_history])

        if email_id == "":
            if text_resume == "":
                continue

        if ((email_id != "") & (email_id in all_emails_list)):

            print(f"Email duplicates found for : {file_id}")

            old_file = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where emailId = '{}'
                """.format(email_id), conn)

            old_file_info = old_file.to_dict("records")

            if date < old_file_info[0].get("date"):
                print(f"Discarded {file_id} because it had an older date than {old_file_info[0].get('fileId')}")
                # print(date, old_file_info[0].get("date"))
                continue

            elif date == old_file_info[0].get("date"):

                print(f"Date is same for {file_id} and {old_file_info[0].get('fileId')}")

                old_file_months_of_work = old_file_info[0].get("monthsOfWork")

                if months_of_work > old_file_months_of_work:
                    print(f"Months of work for {file_id} > {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}\{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    all_file_id_list.append(file_id)
                    all_emails_list.append(email_id)
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{months_of_work}')""")
                else:
                    continue


            else:
                print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                os.remove(f"{newDatasetPath}\{old_file_name}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                all_file_id_list.append(file_id)
                all_emails_list.append(email_id)
                c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{months_of_work}')""")
                conn.commit()

        elif ((text_resume != "") & (text_resume_hash in all_text_resume_list)):

            print(f"TextResume duplicates found for : {file_id}")

            old_file = pd.read_sql(
                """
                SELECT *
                FROM file_info
                where textResume = '{}'
                """.format(text_resume_hash), conn)

            old_file_info = old_file.to_dict("records")

            if date < old_file_info[0].get("date"):
                print(f"Discarded {file_id} because it had an older date than {old_file_info[0].get('fileId')}")
                # print(date, old_file_info[0].get("date"))
                continue

            elif date == old_file_info[0].get("date"):

                print(f"Date is same for {file_id} and {old_file_info[0].get('fileId')}")

                old_file_months_of_work = old_file_info[0].get("monthsOfWork")

                if months_of_work > old_file_months_of_work:
                    print(f"Months of work for {file_id} > {old_file_info[0].get('fileId')}")
                    old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                    os.remove(f"{newDatasetPath}\{old_file_name}")
                    shutil.copy2(f"{i}", f"{newDatasetPath}")
                    all_file_id_list.append(file_id)
                    all_emails_list.append(email_id)
                    c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                    c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{months_of_work}')""")
                else:
                    continue

                # check for the total months of work count
                # if the months of work is greater than the older file, then delete the old file and keep the new one

            else:
                print(f"{file_id} has the latest date as compared to {old_file_info[0].get('fileId')}")
                old_file_name = old_file_info[0].get("fileId") + ".json_cv"
                os.remove(f"{newDatasetPath}\{old_file_name}")
                shutil.copy2(f"{i}", f"{newDatasetPath}")
                all_file_id_list.append(file_id)
                all_emails_list.append(email_id)
                c.execute(f"""DELETE from file_info where fileId = '{old_file_info[0].get("fileId")}'""")
                c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{months_of_work}')""")
                conn.commit()

        else:
            print(f"No duplicates found for : {file_id}")
            shutil.copy2(f"{i}", f"{newDatasetPath}")
            all_file_id_list.append(file_id)
            all_emails_list.append(email_id)
            c.execute(f"""INSERT INTO file_info VALUES ('{file_id}', '{email_id}', '{date}', '{text_resume_hash}', '{months_of_work}')""")
            conn.commit()

        # c.execute(f"""INSERT OR IGNORE INTO file_info VALUES ('{file_id}', '{email_id}', '{date}')""")

        # conn.commit()

    # if length_of_folder - 1 == x:

        # print("Deleting...")

        # shutil.rmtree(folder_path)

        # files = glob.('/home/izoom/BIG_LOCAL_DISKS/new_dataset_2021*')

    # for f in all_json_files:
    #     os.remove(f)

    for i in glob(f"{temp_folder}/*"):
        os.remove(i)

    with open('seen_tars.txt', 'a') as f:
        f.write("%s\n" % a)
        all_folders_completed.append(a)
