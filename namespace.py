import mysql.connector
from mysql.connector import Error


def parent_id(cursor, database, path):
    def get_root_directory_id(cursor):
        cursor.execute("SELECT dir_id FROM directories WHERE parent_dir_id IS NULL")
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return -1

    def get_child_directory_id(cursor, parent_dir_id, dir_name):
        cursor.execute(
            "SELECT dir_id FROM directories WHERE parent_dir_id = %s AND dir_name = %s",
            (parent_dir_id, dir_name)
        )
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return -1

    try:
        dirs = path.split('/')[2:-1]
        current_dir_id = get_root_directory_id(cursor)

        for dir_name in dirs:
            current_dir_id = get_child_directory_id(cursor, current_dir_id, dir_name)
            if current_dir_id == -1:
                return -1

        return current_dir_id

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1



def child_id(cursor, database, path):
    try:
        parent_dir_id = parent_id(cursor, database, path)
        dirs = path.split('/')[2:]
	
        cursor.execute(
            "SELECT file_id FROM files WHERE parent_dir_id = %s AND file_name = %s",
            (parent_dir_id, dirs[-1])
        )
        result = cursor.fetchone()
	
        if result:
            return result[0]
        else:
            return -1
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1


def child_dir_id(cursor, database, path):
    if(path == "/root"):
        return 1
    try:
        parent_dir_id = parent_id(cursor, database, path)
        dirs = path.split('/')[2:]
	
        cursor.execute(
            "SELECT dir_id FROM directories WHERE parent_dir_id = %s AND dir_name = %s",
            (parent_dir_id, dirs[-1])
        )
        result = cursor.fetchone()
	
        if result:
            return result[0]
        else:
            return -1
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1



def get_accessible_files(cursor, db, dir_id):
    accessible_files = []
    
    def get_files_recursive(current_dir_id):
        # Fetch all files in the current directory
        query = "SELECT file_id FROM files WHERE parent_dir_id = %s"
        cursor.execute(query, (current_dir_id,))
        files = cursor.fetchall()

        for file in files:
            accessible_files.append(file[0])

        query = "SELECT dir_id FROM directories WHERE parent_dir_id = %s"
        cursor.execute(query, (current_dir_id,))
        subdirectories = cursor.fetchall()

        for subdirectory in subdirectories:
            get_files_recursive(subdirectory[0])

    get_files_recursive(dir_id)

    return accessible_files

def path_milega_file_id_Return_krna_hai(cursor, db, path):
    try:
        ID = child_id(cursor, db, path)

        if ID == -1:
            #print("Invalid path.")
            return -1

        query = "UPDATE files set time_of_modification = CURRENT_TIMESTAMP where file_id= %s"
        cursor.execute(query, (ID,))
        db.commit()
        return ID
    except Error as e:
        #print(f"Error deleting file: {e}")
        db.rollback()
        return -1
	

def create_directory(cursor, db, path):
    try:
        parentID = parent_id(cursor, db, path)
        
        if parentID == -1:
            #print("Invalid path.")
            return -1

        dirs = path.split('/')[2:]
        DIRname = dirs[-1]
        query = "INSERT INTO directories (dir_name, parent_dir_id) VALUES (%s, %s)"
        cursor.execute(query, (DIRname, parentID))
        db.commit()
        #print(f"Directory {DIRname} created successfully.")
        return 1
    except Error as e:
        #print(f"Error creating directory: {e}")
        db.rollback()
        return -1

def delete_directory(cursor, db, path):
    try:
        ID = child_dir_id(cursor, db, path)

        if ID == -1:
            #print("Invalid path.")
            return -1

        file_ids = get_accessible_files(cursor, db, ID)
        query = "DELETE FROM directories WHERE dir_id = %s"
        cursor.execute(query, (ID,))
        db.commit()
        #print("Directory with ID %s deleted successfully." % ID)
        return file_ids
    except Error as e:
        #print(f"Error deleting directory: {e}")
        db.rollback()
        return -1

def delete_file(cursor, db, path):
    try:
        ID = child_id(cursor, db, path)

        if ID == -1:
            #print("Invalid path.")
            return -1

        query = "DELETE FROM files WHERE file_id = %s"
        cursor.execute(query, (ID,))
        db.commit()
        #print("File with ID %s deleted successfully." % ID)
        return ID
    except Error as e:
        #print(f"Error deleting file: {e}")
        db.rollback()
        return -1


def move_directory(cursor, db, path, new_path):
    try:
        ID = child_dir_id(cursor, db, path)

        if ID == -1:
            #print("Invalid path.")
            return -1

        #new_path = input("Enter path of the New Directory where you want to move it:")
        new_parent = child_dir_id(cursor, db, new_path)

        if new_parent == -1:
            #print("Invalid destination path.")
            return -2

        query = "UPDATE directories SET parent_dir_id = %s WHERE dir_id = %s"
        cursor.execute(query, (new_parent, ID))
        db.commit()
        return 1
        #print("Directory with ID %s moved successfully." % ID)
    except Error as e:
        #print(f"Error moving directory: {e}")
        db.rollback()
        return -3

def move_file(cursor, db, path, new_path):
    try:
        ID = child_id(cursor, db, path)

        if ID == -1:
            #print("Invalid path.")
            return -1

        #new_path = input("Enter path of the New Directory where you want to move it:")
        new_parent = child_dir_id(cursor, db, new_path)

        if new_parent == -1:
            #print("Invalid destination path.")
            return -2

        query = "UPDATE files SET parent_dir_id = %s WHERE file_id = %s"
        cursor.execute(query, (new_parent, ID))
        db.commit()
        return 1
        #print("File with ID %s moved successfully." % ID)
    except Error as e:
        #print(f"Error moving file: {e}")
        db.rollback()
        return -3

def copy(cursor, db, original_dir_id, new_parent_id, new_file_ids):
    try:
        query = "SELECT * FROM directories WHERE dir_id = %s"
        cursor.execute(query, (original_dir_id,))
        original_directory = cursor.fetchone()

        if not original_directory:
            #print("Source directory not found.")
            return

        # Create a new directory entry with the same information
        query = "INSERT INTO directories (dir_name, parent_dir_id) VALUES (%s, %s)"
        cursor.execute(query, (original_directory[1], new_parent_id))
        new_dir_id = cursor.lastrowid

        # Retrieve and copy files within the directory
        query = "SELECT * FROM files WHERE parent_dir_id = %s"
        cursor.execute(query, (original_dir_id,))
        files_to_copy = cursor.fetchall()

        for file in files_to_copy:
            # Create a new file entry with the same information
            query = "INSERT INTO files (file_name, size, parent_dir_id) VALUES (%s, %s, %s)"
            cursor.execute(query, (file[1], file[4], new_dir_id))
            new_file_id = cursor.lastrowid

            new_file_ids.append(new_file_id)  # Append new_file_id to the provided list

            # Retrieve and copy blocks associated with the file
            query = "SELECT * FROM blocks WHERE file_id = %s"
            cursor.execute(query, (file[0],))
            blocks_to_copy = cursor.fetchall()

            for block in blocks_to_copy:
                # Create a new block entry with the same information
                query = "INSERT INTO blocks (file_id, block_id, datanode_id) VALUES (%s, %s, %s)"
                cursor.execute(query, (new_file_id, block[1], block[2]))

        # Retrieve and copy directories within the directory
        query = "SELECT * FROM directories WHERE parent_dir_id = %s"
        cursor.execute(query, (original_dir_id,))
        dirs_to_copy = cursor.fetchall()

        for dir in dirs_to_copy:
            copy(cursor, db, dir[0], new_dir_id, new_file_ids)

        db.commit()
        #print(f"Directory with ID {original_dir_id} copied to {new_parent_id} successfully.")

    except Error as e:
        #print(f"Error copying directory: {e}")
        db.rollback()
        return -1

def copy_directory(cursor, db, path, new_path):
    try:
        original_dir_id = child_dir_id(cursor, db, path)

        if original_dir_id == -1:
            #print("Source directory not found.")
            return -1

        #new_path = input("Enter path of the New Directory where you want to move it:")
        new_parent = child_dir_id(cursor, db, new_path)

        if new_parent == -1:
            #print("Invalid destination path.")
            return -2
        new_file_ids = []
        old_file_ids = get_accessible_files(cursor, db, original_dir_id)
        copy(cursor, db, original_dir_id, new_parent, new_file_ids)
        nested_list = list(zip(old_file_ids, new_file_ids))
        return nested_list

    except Error as e:
        #print(f"Error copying directory: {e}")
        db.rollback()
        return -3
    
        



def copy_file(cursor, db, path, new_path):
    try:
        # Get the ID of the source file
        src_file_id = child_id(cursor, db, path)

        if src_file_id == -1:
            #print("Source file not found.")
            return -1

        #new_path = input("Enter path of the New Directory where you want to move it:")
        new_parent = child_dir_id(cursor, db, new_path)

        if new_parent == -1:
            #print("Invalid destination path.")
            return -2

        # Retrieve information about the source file
        query = "SELECT * FROM files WHERE file_id = %s"
        cursor.execute(query, (src_file_id,))
        original_file = cursor.fetchone()

        # Create a new file entry with the same information in the new parent directory
        query = "INSERT INTO files (file_name, size, parent_dir_id) VALUES (%s, %s, %s)"
        cursor.execute(query, (original_file[1], original_file[4], new_parent))
        new_file_id = cursor.lastrowid
        
        # Retrieve and copy blocks associated with the source file
        query = "SELECT * FROM blocks WHERE file_id = %s"
        cursor.execute(query, (src_file_id,))
        blocks_to_copy = cursor.fetchall()

        for block in blocks_to_copy:
            query = "INSERT INTO blocks (file_id, block_id, datanode_id) VALUES (%s, %s, %s)"
            cursor.execute(query, (new_file_id, block[1], block[2]))

        db.commit()
        #print(f"File with ID {src_file_id} copied to {new_parent} successfully.")
        return [[src_file_id, new_file_id]]
    except Error as e:
        #print(f"Error copying file: {e}")
        db.rollback()
        return -3

def list_files(cursor, db, path):
    print("in namespace.py")
    try:
        print("about to get dir_id")
        ID = child_dir_id(cursor, db, path)
        print("got dir_id:",ID)
        if ID == -1:
            print("Invalid path.")
            return -1

        query = "SELECT file_name, time_of_creation, size FROM files WHERE parent_dir_id = %s"
        cursor.execute(query, (ID,))
        files = cursor.fetchall()
        print("got files:", files)

        query = "SELECT dir_name, time_of_creation FROM directories WHERE parent_dir_id = %s"
        cursor.execute(query, (ID,))
        dirs = cursor.fetchall()
        print("got dirs:", dirs)

        f_list = [i[0] + " " + str(i[1].date()) + "|" + str(i[1].time()) + " " + str(i[2]) for i in files]
        d_list = [i[0] + "/ " + str(i[1].date()) + "|" + str(i[1].time()) for i in dirs]
        
        print("final res to nn:", f_list + d_list)
        return f_list + d_list

        # if not files:
        #     #print("No files found in directory with ID %s" % ID)
        #     return 
        # else:
        #     print("Files in directory with ID %s:" % ID)
        #     for file in files:
        #         print(f"File ID: {file[0]}, Name: {file[1]}, Size: {file[4]} bytes")

    except Error as e:
        #print(f"Error listing files: {e}")
        return -1



def view_hierarchy(cursor, start):
    try:
        #print("Directory Hierarchy starting from ID %s:",start)
        #print(f"Root/")
        tree = ["root/"]
        query2 = "SELECT file_id, file_name FROM files WHERE parent_dir_id = %s"
        cursor.execute(query2, (1,))

        files_within = cursor.fetchall()
        for files_inside in files_within:
            tree.append("  " * (1) + f"{files_inside[1]}")
        print_directory_tree(cursor, start, 1, tree)
        return tree

    except Error as e:
        print(f"Error viewing hierarchy: {e}")
        return -1

def print_directory_tree(cursor, parent_dir_id, indent_level, tree):
    query = "SELECT * FROM directories WHERE parent_dir_id = %s"
    cursor.execute(query, (parent_dir_id,))
    directories = cursor.fetchall()

    for directory in directories:
        tree.append("  " * indent_level + f"{directory[1]}/")

        query2 = "SELECT file_id, file_name FROM files WHERE parent_dir_id = %s"
        cursor.execute(query2, (directory[0],))

        files_within = cursor.fetchall()
        for files_inside in files_within:
            tree.append("  " * (indent_level + 1) + f"{files_inside[1]}")

        print_directory_tree(cursor, directory[0], indent_level + 1, tree)


# def main_menu():
#     print("Menu:")
#     print("1. Create Directory")
#     print("2. Delete Directory")
#     print("3. Delete File")
#     print("4. Move Directory")
#     print("5. Move File")
#     print("6. Copy Directory")
#     print("7. Copy File")
#     print("8. List Files in Directory")
#     print("9. View Hierarchy")
#     print("0. Exit")

# def get_user_choice():
#     return input("Enter your choice: ")

# try:
#     db = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="maitreyi@1304",
#         database="bdproject"
#     )

#     if db.is_connected():
#         print("Connected to MySQL database")

#     cursor = db.cursor()

#     while True:
#         main_menu()
#         choice = get_user_choice()

#         if choice == '0':
#             break
#         elif choice == '1':
#             create_directory(cursor, db, input("Enter path to create directory: "))
#         elif choice == '2':
#             delete_directory(cursor, db, input("Enter path to delete directory: "))
#         elif choice == '3':
#             delete_file(cursor, db, input("Enter path to delete file: "))
#         elif choice == '4':
#             move_directory(cursor, db, input("Enter path to move directory: "))
#         elif choice == '5':
#             move_file(cursor, db, input("Enter path to move file: "))
#         elif choice == '6':
#             copy_directory(cursor, db, input("Enter path to copy directory: "))
#         elif choice == '7':
#             copy_file(cursor, db, input("Enter path to copy file: "))
#         elif choice == '8':
#             list_files(cursor,db , input("Enter path to list files: "))
#         elif choice == '9':
#             view_hierarchy(cursor, 1)
#         else:
#             print("Invalid choice. Please enter a valid option.")

# except Error as e:
#     print(f"Error connecting to MySQL: {e}")

# finally:
#     if db.is_connected():
#         cursor.close()
#         db.close()
#         print("Connection closed")