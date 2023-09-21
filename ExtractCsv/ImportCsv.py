# %%
# #1 clean CSV file
# import csv

# # Tên tệp CSV đầu vào
# input_file = '201901010954023928_0.csv'

# # Tên tệp CSV đầu ra (đã lọc)
# output_file = '201901010954023928_1.csv'

# # Số lượng cột bạn muốn giữ lại
# num_columns_to_keep = 52

# # Mở tệp CSV đầu vào để đọc
# with open(input_file, 'r', newline='') as csvfile:
#     reader = csv.reader(csvfile)

#     # Đọc dòng đầu tiên của tệp CSV để lấy tiêu đề (header)
#     header = next(reader)

#     # Chỉ giữ lại 52 cột đầu tiên và loại bỏ các dòng rỗng
#     filtered_data = [row[:num_columns_to_keep] for row in reader if any(row)]

# # Mở tệp CSV đầu ra để viết dữ liệu đã lọc
# with open(output_file, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)

#     # Ghi tiêu đề vào tệp CSV đầu ra
#     writer.writerow(header[:num_columns_to_keep])

#     # Ghi dữ liệu đã lọc vào tệp CSV đầu ra
#     writer.writerows(filtered_data)


# %%
import mysql.connector
import pandas as pd
import numpy as np

# Define your MySQL connection parameters
mysql_config = {
    'host': "localhost",
    'port': 3306,
    'user': "root",
    'password': "a",
    'database': "student_database",
}

table_name = "atp_huawei_smppgw"

# %%
# 1 Create table

try:
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()

    # Define the SQL queries to delete and recreate the table
    delete_table_query = f"DROP TABLE IF EXISTS {table_name}"
    recreate_table_query = f"""
    CREATE TABLE {table_name} (
        serial_number CHAR(18),
        message_type INT UNSIGNED,
        send_service_type INT UNSIGNED,
        receive_service_type INT UNSIGNED,
        originating_address VARCHAR(20),
        ton_of_the_source_address TINYINT UNSIGNED,
        npi_of_the_originating_address TINYINT UNSIGNED,
        destination_address_after_number_conversion VARCHAR(20),
        ton_of_the_destination_address TINYINT UNSIGNED,
        npi_of_the_destination_address TINYINT UNSIGNED,
        final_time DATETIME,
        opposite_user_type TINYINT UNSIGNED,
        pid TINYINT UNSIGNED,
        dcs TINYINT UNSIGNED,
        message_length INT UNSIGNED,
        message_status INT UNSIGNED,
        ip_address_of_message_sender VARCHAR(15),
        ip_address_of_message_receiver VARCHAR(15),
        sm_id_smsc INT UNSIGNED,
        sm_status INT UNSIGNED,
        sm_error_code INT UNSIGNED,
        charge_rate INT,
        submission_time DATETIME,
        service_sub_type VARCHAR(11),
        time_field_in_msg_id_cmpp INT UNSIGNED,
        gateway_id_or_smc_number_in_msg_id_cmpp INT UNSIGNED,
        sequence_number_in_msg_id_cmpp INT UNSIGNED,
        number_of_messages_with_the_same_msg_id_cmpp TINYINT UNSIGNED,
        sequence_number_of_messages_with_the_same_msg_id_cmpp TINYINT UNSIGNED,
        message_level TINYINT UNSIGNED,
        charged_subscriber_type_cmpp TINYINT UNSIGNED,
        charged_subscriber_number_cmpp VARCHAR(20),
        source_of_information_content_cmpp VARCHAR(10),
        charging_type_cmpp VARCHAR(2),
        charging_code VARCHAR(10),
        service_code VARCHAR(10),
        whether_to_write_a_cmpp_bill TINYINT UNSIGNED,
        bill_confirmation_type TINYINT UNSIGNED,
        pps_subscriber_flag TINYINT UNSIGNED,
        local_gateway_code VARCHAR(6),
        handling_time INT UNSIGNED,
        code_of_the_originating_gateway VARCHAR(6),
        forwarding_gateway_code VARCHAR(6),
        protocol_data_unit_type_of_message INT UNSIGNED,
        sm_content VARCHAR(254),
        opid_of_the_originating_address VARCHAR(20),
        opid_of_the_destination_address VARCHAR(20),
        incoming_sequence_number INT UNSIGNED,
        outgoing_sequence_number INT UNSIGNED,
        sm_id_smc VARCHAR(64),
        sm_sending_account VARCHAR(20),
        sm_receiving_account VARCHAR(20)
    );
    """

    cursor.execute(delete_table_query)
    print(f"Table {table_name} deleted successfully")
    cursor.execute(recreate_table_query)
    print(f"Table {table_name} recreated successfully")

    connection.commit()

except mysql.connector.Error as error:
    print(f"Error: {error}")
finally:
    if "cursor" in locals():
        cursor.close()
    if "connection" in locals() and connection.is_connected():
        connection.close()

# %%
# #1 Describe table

# try:
#     # Connect to the MySQL server
#     connection = mysql.connector.connect(**mysql_config)

#     # Create a cursor object to interact with the database
#     cursor = connection.cursor()

#     # Define the SQL query to retrieve the table schema
#     describe_query = f"DESCRIBE {table_name}"

#     # Execute the query to retrieve the table schema
#     cursor.execute(describe_query)

#     # Fetch and print the results
#     table_schema = cursor.fetchall()
#     for column in table_schema:
#         print(column)

# except mysql.connector.Error as error:
#     print(f"Error: {error}")
# finally:
#     # Close the cursor and connection
#     if 'cursor' in locals():
#         cursor.close()
#     if 'connection' in locals() and connection.is_connected():
#         connection.close()

# %%
# #1 import CSV file with pandas
# import sqlalchemy
# Define the path to your CSV file
csv_file_path = "201901010954023928_1.csv"

try:
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()

    df = pd.read_csv(
        csv_file_path,
        dtype={
            "Serial number": str,
            "Message type": "UInt32",
            "Send service type": "UInt32",
            "Receive service type": "UInt32",
            "Originating address": str,
            "TON of the source address": "UInt8",
            "NPI of the originating address": "UInt8",
            "Destination address after number conversion": str,
            "TON of the destination address": "UInt8",
            "NPI of the destination address": "UInt8",
            "Final time": str,
            "Opposite user type": "UInt8",
            "PID": "UInt8",
            "DCS": "UInt8",
            "Message length": "UInt32",
            "Message status": "UInt32",
            "IP address of the message sender": str,
            "IP address of the message receiver": str,
            "SM ID SMSC": "UInt32",
            "SM status": "UInt32",
            "SM error code": "UInt16",
            "Charge rate": "Int32",
            "Submission time": str,
            "Service sub-type": str,
            "Time field in MsgID (CMPP)": "UInt32",
            "Gateway ID or SMC number in MsgID (CMPP)": "UInt32",
            "Sequence number in MsgID (CMPP)": "UInt32",
            "Number of messages with the same Msg_id (CMPP)": "UInt8",
            "Sequence number of messages with the same Msg_id (CMPP)": "UInt8",
            "Message level": "UInt8",
            "Charged Subscriber type (CMPP)": "UInt8",
            "Charged subscriber number (CMPP)": str,
            "Source of information content (CMPP)": str,
            "Charging type (CMPP)": str,
            "Charging code": str,
            "Service code": str,
            "Whether to write a CMPP bill": "UInt8",
            "Bill confirmation type": "UInt16",
            "PPS subscriber flag": "UInt8",
            "Local gateway code": str,
            "Handling time": "UInt32",
            "Code of the originating gateway": str,
            "Forwarding gateway code": str,
            "Protocol data unit (PDU) type of message": "UInt16",
            "SM content": str,
            "OPID of the originating address": str,
            "OPID of the destination address": str,
            "Incoming sequence number": "UInt32",
            "Outgoing sequence number": "UInt32",
            "SM ID": str,
            "SM sending account": str,
            "SM receiving account": str,
        },
    )

    # Replace NaN values with None (NULL) in the DataFrame
    df = df.replace({np.nan: None})
    # df.fillna(value = 'None', inplace=True)
    # df = df.replace(np.nan, None)

    placeholders = ", ".join(["%s" for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    values = [tuple(row) for _, row in df.iterrows()]
    cursor.executemany(insert_query, values)

    # df.to_sql(name=table_name, con=connection, if_exists='append', index=False)

    # # Iterate through each row in the DataFrame and insert into the MySQL table
    # for index, row in df.iterrows():
    #     try:
    #         # Replace NaN values with None (NULL) in the DataFrame
    #         # row = row.where(pd.notna(row), None)
    #         placeholders = ", ".join(["%s" for _ in row])
    #         insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    #         cursor.execute(insert_query, tuple(row))
    #     except mysql.connector.Error as mysql_error:
    #         print(f"MySQL Error: {mysql_error}")
    #         print(f"Row {index + 1}: {row}")
    #         break
    #     except Exception as e:
    #         print(f"An error occurred: {e}")
    #         print(f"Row {index + 1}: {row}")

    connection.commit()
    print("Data imported successfully!")

except mysql.connector.Error as error:
    print(f"MySQL Error: {error}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the cursor and connection
    if "cursor" in locals():
        cursor.close()
    if "connection" in locals() and connection.is_connected():
        connection.close()

# %%
# # 1 print table

# try:
#     # Connect to the MySQL server
#     connection = mysql.connector.connect(**mysql_config)

#     # Create a cursor object to interact with the database
#     cursor = connection.cursor()

#     # Define the SQL query to select all rows from the table
#     select_query = f"SELECT * FROM {table_name}"

#     # Execute the SQL query
#     cursor.execute(select_query)

#     # Fetch all rows from the result set
#     rows = cursor.fetchall()

#     # Get the column names
#     column_names = [desc[0] for desc in cursor.description]

#     # Calculate the maximum width for each column
#     column_widths = [max(len(str(row[i])) for row in rows + [column_names]) for i in range(len(column_names))]

#     # Print column headers
#     for i, column_name in enumerate(column_names):
#         print(f"{column_name:<{column_widths[i]}}", end="\t")
#     print()  # Print a newline after headers

#     # Print table content
#     for row in rows:
#         for i, cell in enumerate(row):
#             print(f"{cell:<{column_widths[i]}}", end="\t")
#         print()  # Print a newline after each row

# except mysql.connector.Error as error:
#     print(f"Error: {error}")

# finally:
#     # Close the cursor and connection
#     if 'cursor' in locals():
#         cursor.close()
#     if 'connection' in locals() and connection.is_connected():
#         connection.close()


# %%
# #1 export to CSV file

# try:
#     output_csv_file = "exported.csv"

#     # Connect to the MySQL server
#     connection = mysql.connector.connect(**mysql_config)

#     # Create a cursor object to interact with the database
#     cursor = connection.cursor()

#     # Define the SQL query to select all rows from the table
#     select_query = f"SELECT * FROM {table_name}"

#     # Execute the SQL query
#     cursor.execute(select_query)

#     # Fetch all rows from the result set
#     rows = cursor.fetchall()

#     # Get the column names from the cursor description
#     column_names = [desc[0] for desc in cursor.description]

#     # Create a DataFrame from the fetched data
#     df = pd.DataFrame(rows, columns=column_names)

#     # Export the DataFrame to a CSV file
#     df.to_csv(output_csv_file, index=False)

#     print(f"Table data exported to {output_csv_file}.")

# except mysql.connector.Error as error:
#     print(f"Error: {error}")

# finally:
#     # Close the cursor and connection
#     if "cursor" in locals():
#         cursor.close()
#     if "connection" in locals() and connection.is_connected():
#         connection.close()
