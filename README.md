# EC-Team-45-Yet-Another-Distributed-File-System-YADFS-

1. Start namenode
2. Start client
3. Start datanodes (python3 datanode <datanode_id>)

datanode_id can be 1,2,3,4.


To do:

Data Block Ops:

    1. Upload:
        a. Client gets info of active datanodes from namenode
        b. Client contacts and sends blocks as many of them as required, receives ack.
        d. Each datanode that received a block in step b. will forward it to 2 more datanodes (replication) and inform namenode accordingly to update metadata

    2. Download:
        a. Client gets metadata from namenode
        b. Client contacts any of the datanodes
        c. Client initiates download from datanodes
        d. Reassembly once done
        e. save locally

    3. Read:
        a. Client gets metadata from namenode
        b. Client invokes read api with corresponding datanodes
        c. Reassembly and display

    4. Write:
        a. Get changes first
        b. Get metadata
        c. Overwrite changes in all blocks including replicas

Namespace Hierarchy Ops:

    1. Create directory:
        a. create entry in directory table

    2. Delete directory/file:
        a. delete entry from directory/file table, cascading down to files and directories within
    
    3. Move directory/file:
        a. change parent_d_id
    
    4. Copy directory/file:
        a. new entry, same file/directory name in a new location
    
    5. List files in directories

    6. View hierarchy as a tree



Database Schema:


    directories:

        dir_id (auto_increment), dir_name, time_of_creation, parent_dir_id

        PK: dir_id 

        FK: parent_dir_id -> directories.dir_id 



    files:

        file_id (auto_increment), file_name, time_of_creation, time_of_modification, size, parent_dir_id

        PK: file_id 

        FK: parent_dir_id -> directories.dir_id 



    blocks:

        file_id, block_id, datanode_id

        PK: file_id, block_id, datanode_id (rack awareness)

        FK: file_id -> files.file_id


    trigger check_dir_name on insert in table directories:

        <checks if all the directories in the parent dir have different names compared to the new one being inserted>

    trigger check_file_name on insert in table files:
    
        <checks if all the files in the parent dir have different names compared to the new one being inserted>
        
    trigger check_dir_name_update on update in table directories:
    
    	<checks if all the directories in the same parent dir have different names compared to new one being inserted on updation>
    	
    trigger check_file_name_update on update in table files:
    
    	<checks if all the files in the parent dir have different names compared to the new one being inserted on updation>
