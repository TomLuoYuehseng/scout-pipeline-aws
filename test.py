from sqlalchemy.sql import text
table_name = 'nice'
sql = f'''SELECT * FROM {table_name};'''
text(sql)
print(sql)