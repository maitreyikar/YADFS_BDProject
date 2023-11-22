import mysql.connector

# def get_parent_directory_id(cursor, path):
#     if path == "/root":
#         return 1
#     def get_root_directory_id(cursor):
#         cursor.execute("SELECT dir_id FROM directories WHERE parent_dir_id IS NULL")
#         result = cursor.fetchone()
#         if result:
#             return result[0]
#         else:
#             return -1

#     def get_child_directory_id(cursor, parent_dir_id, dir_name):
#         cursor.execute(
#             "SELECT dir_id FROM directories WHERE parent_dir_id = %s AND dir_name = %s",
#             (parent_dir_id, dir_name)
#         )
#         result = cursor.fetchone()
#         if result:
#             return result[0]
#         else:
#             return -1

#     try:
#         dirs = path.split('/')[2:-1]
#         current_dir_id = get_root_directory_id(cursor)

#         for dir_name in dirs:
#             current_dir_id = get_child_directory_id(cursor, current_dir_id, dir_name)
#             if current_dir_id is None:
#                 return -1

#         return current_dir_id


#     except Exception as e:
#         print(f"Error: {e}")
#         return -1


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







def add_file_metadata_and_return_file_id(cursor, db, path, file_name, size):
    try:
        parent_dir_id = child_dir_id(cursor, db, path)
        print(parent_dir_id)
        print(path)
        if parent_dir_id == -1:
            print("Error: Parent directory doesn't exist.")
            return -1  # Return -1 to indicate an error

        #dirs = path.split('/')
        #file_name = dirs[-1]

        # Check if the file already exists in the parent directory
        cursor.execute(
            "SELECT file_id FROM files WHERE parent_dir_id = %s AND file_name = %s",
            (parent_dir_id, file_name)
        )
        result = cursor.fetchone()

        if result:
            print("Error: File with the same name already exists in the parent directory.")
            return -2  
        else:
            # Insert new file metadata
            cursor.execute(
                "INSERT INTO files (file_name, parent_dir_id, size) VALUES (%s, %s, %s)",
                (file_name, parent_dir_id, size)
            )
            db.commit()

            # Get the file_id of the newly inserted file
            cursor.execute(
                "SELECT file_id FROM files WHERE parent_dir_id = %s AND file_name = %s",
                (parent_dir_id, file_name)
            )
            new_file_id = cursor.fetchone()[0]

            print("File metadata added successfully.")
            return new_file_id

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1  

    except Exception as e:
        print(f"Error: {e}")
        return -3  






def get_datanodes_and_blocks(cursor, db, file_id):
    try:
        # Retrieve datanode_id and block_id for the given file_id
        cursor.execute(
            "SELECT datanode_id, block_id FROM blocks WHERE file_id = %s",
            (file_id,)
        )
        results = cursor.fetchall()

        datanodes_and_blocks = {}

        for row in results:
            datanode_id = row[0]
            block_id = row[1]

            # Add block_id to the list for the corresponding datanode_id
            if block_id in datanodes_and_blocks:
                datanodes_and_blocks[block_id].append(datanode_id)
            else:
                datanodes_and_blocks[block_id] = [datanode_id]

        return datanodes_and_blocks

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1
    



def add_block_metadata(cursor, db, file_id, block_id, datanode_id):
    try:
        # Check if the block already exists
        cursor.execute(
            "SELECT * FROM blocks WHERE file_id = %s AND block_id = %s AND datanode_id = %s",
            (file_id, block_id, datanode_id)
        )
        result = cursor.fetchone()

        if result:
            print("Error: Block with the same file_id, block_id, and datanode_id already exists.")
            return -1
        else:
            # Insert new block metadata
            cursor.execute(
                "INSERT INTO blocks (file_id, block_id, datanode_id) VALUES (%s, %s, %s)",
                (file_id, block_id, datanode_id)
            )
            db.commit()
            print("Block metadata added successfully.")
            return 1

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return -1

    except Exception as e:
        print(f"Error: {e}")
        return -1

    
    











# def main():
#     host = "localhost"
#     user = "root"
#     password = "maitreyi@1304"
#     database = "bdproject" 

#     path = "/root/Documents/document4.txt"
#     size = 1024

#     var = add_file_metadata_and_return_file_id(host, user, password, database, path, size)
#     print(var)
# if __name__ == "__main__":
#     main()
