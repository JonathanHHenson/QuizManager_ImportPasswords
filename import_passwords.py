import mysql.connector
import pandas
import numpy as np
import config as cfg
import sys


def main():
    config_path, user_csv_path = get_args()
    if not config_path:
        print('Usage: import_passwords.py <config> <users_csv>')
        return

    users_df = pandas.read_csv(user_csv_path)
    permissions = set(perm.lower() for perm in users_df.permission)

    config = cfg.Config(config_path)
    db_connection = mysql.connector.connect(
        host=config['host'],
        user=config['username'],
        password=config['password'],
        database=config['database']
    )
    del config

    insert_permissions(permissions, db_connection)
    users = prepare_users(users_df, db_connection)
    insert_users(users, db_connection)

    db_connection.close()


def insert_permissions(permissions, db_connection):
    sql_select_permissions = 'SELECT Label FROM Permission'
    sql_insert_permissions = 'INSERT INTO Permission (Label) VALUES (%s)'

    my_cursor = db_connection.cursor()

    my_cursor.execute(sql_select_permissions)
    registered_perms = set(lbl.lower() for lbl, in my_cursor.fetchall())

    new_perms = [(perm,) for perm in permissions - registered_perms]
    print('Adding permissions:', new_perms)

    if new_perms:
        my_cursor.executemany(sql_insert_permissions, new_perms)
        db_connection.commit()
        print(my_cursor.rowcount, 'permissions added')
    else:
        print('No permissions need to be added')

    my_cursor.close()


def prepare_users(users_df, db_connection):
    sql_select_permissions = 'SELECT ID, Label FROM Permission'

    my_cursor = db_connection.cursor()

    my_cursor.execute(sql_select_permissions)
    perms_lookup = {lbl.lower(): perm_id for perm_id, lbl in my_cursor.fetchall()}

    my_cursor.close()

    user_columns = ['username', 'password', 'permission']
    users = [(user, pswd, perms_lookup[perm.lower()]) for _, user, pswd, perm in users_df[user_columns].itertuples()]

    return users


def insert_users(users, db_connection):
    sql_insert_users = 'INSERT INTO Authentication (Username, Password, PermissionID) VALUES (%s, %s, %s)'

    my_cursor = db_connection.cursor()
    my_cursor.executemany(sql_insert_users, users)

    db_connection.commit()
    print(my_cursor.rowcount, 'users added')

    my_cursor.close()


def get_args():
    if len(sys.argv) < 3:
        return None, None
    return sys.argv[1:3]


if __name__ == '__main__':
    main()