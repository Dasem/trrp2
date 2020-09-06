import pymysql
import constants
import xlsxwriter
import datetime

mysql = pymysql.connect(constants.mysql_ip, constants.mysql_user, constants.mysql_pwd, constants.mysql_db)

mysql_cur = mysql.cursor()

def to_excel():
    mysql_cur.execute("SHOW tables")
    tables = mysql_cur.fetchall()
    # create xlsx file
    workbook = xlsxwriter.Workbook('all_tables.xlsx')
    worksheet = workbook.add_worksheet('sellers')
    date_format = workbook.add_format({'num_format': 'd mmmm yyyy'})
    # create style for xlsx file
    header_cell_format = workbook.add_format({'bold': True, 'border': True, 'bg_color': 'yellow'})
    body_cell_format = workbook.add_format({'border': True})
    row_index = 0
    for tablename_raw in tables:
        tablename = str(tablename_raw)[2:-3]
        mysql_cur.execute(f'select * from {tablename}')
        header = [row[0] for row in mysql_cur.description]
        rows = mysql_cur.fetchall()

        column_index = 0
        row_index += 1
        worksheet.write(row_index, column_index, tablename, header_cell_format)
        row_index += 1

        for column_name in header:
            worksheet.write(row_index, column_index, column_name, header_cell_format)
            column_index += 1

        row_index += 1
        for row in rows:
            column_index = 0
            for cell in row:
                # write
                if (type(cell) == datetime.datetime):
                    worksheet.write_datetime(row_index, column_index, cell, date_format)
                else:
                    worksheet.write(row_index, column_index, cell, body_cell_format)
                column_index += 1
            row_index += 1

    workbook.close()

if __name__ == '__main__':
    to_excel()

    mysql.close()
    sqlite.close()
