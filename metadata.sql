CREATE DATABASE bdproject;

USE bdproject;

CREATE TABLE directories (
    dir_id INT AUTO_INCREMENT PRIMARY KEY,
    dir_name VARCHAR(255) NOT NULL,
    time_of_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parent_dir_id INT,
    FOREIGN KEY (parent_dir_id) REFERENCES directories(dir_id) ON DELETE CASCADE
);

CREATE TABLE files (
    file_id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    time_of_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    time_of_modification TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    size INT NOT NULL,
    parent_dir_id INT,
    FOREIGN KEY (parent_dir_id) REFERENCES directories(dir_id) ON DELETE CASCADE
);

CREATE TABLE blocks (
    file_id INT,
    block_id INT,
    datanode_id INT,
    PRIMARY KEY (file_id, block_id, datanode_id),
    FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

DELIMITER //
CREATE TRIGGER check_dir_name
BEFORE INSERT ON directories
FOR EACH ROW
BEGIN
    IF EXISTS (SELECT 1 FROM directories WHERE dir_name = NEW.dir_name AND parent_dir_id = NEW.parent_dir_id) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Directory with the same name already exists in the parent directory.';
    END IF;
END;
//
DELIMITER ;

DELIMITER //
CREATE TRIGGER check_file_name
BEFORE INSERT ON files
FOR EACH ROW
BEGIN
    IF EXISTS (SELECT 1 FROM files WHERE file_name = NEW.file_name AND parent_dir_id = NEW.parent_dir_id) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'File with the same name already exists in the parent directory.';
    END IF;
END;
//
DELIMITER ;



DELIMITER //
CREATE TRIGGER check_dir_name_update
BEFORE UPDATE ON directories
FOR EACH ROW
BEGIN
    IF EXISTS (
        SELECT 1
        FROM directories
        WHERE dir_name = NEW.dir_name
          AND parent_dir_id = NEW.parent_dir_id
          AND dir_id != NEW.dir_id
    ) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Directory with the same name already exists in the parent directory.';
    END IF;
END;
//
DELIMITER ;


DELIMITER //
CREATE TRIGGER check_file_name_update
BEFORE UPDATE ON files
FOR EACH ROW
BEGIN
    IF EXISTS (
        SELECT 1
        FROM files
        WHERE file_name = NEW.file_name
          AND parent_dir_id = NEW.parent_dir_id
          AND file_id != NEW.file_id
    ) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'File with the same name already exists in the parent directory.';
    END IF;
END;
//
DELIMITER ;


