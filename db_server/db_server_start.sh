#!/bin/bash

echo "Start mysqld ..."
service mariadb start

cntr=0
until mysql -u root -e "SHOW DATABASES; ALTER USER 'root'@'localhost' IDENTIFIED BY '$SERVER_ID@123';" ; do
    sleep 1
    read -r -p "Can't connect, retrying..."
    # echo "Retrying..."
    cntr=$((cntr+1))
    if [ $cntr -gt 5 ]; then
        echo "Failed to start MySQL server."
        exit 1
    fi
done

exec python3 db_server.py