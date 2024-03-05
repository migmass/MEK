#!/usr/bin/python3
# агрегируем данные отчет по действющим урокам 
import psycopg2


conn_dwh = psycopg2.connect(database = "dwh",
                        host =     "localhost",
                        user =     "postgres",
                        password = "postgres_password",
                        port =     "5433")

conn_dwh.autocommit = False
cursor_dwh = conn_dwh.cursor()

cursor_dwh.execute("""  insert into dwh.tream_module_lesson_active (
                            id, 
							title, 
							description,
							stream_module_id							
                        select 
                            id, 
	                        title, 
	                        description,
	                        stream_module_id, 
	                        from dwh.stream_module_lesson_hist tr
                        where deleted_flg!='Y' and extract(year from tgt.end_dt) = 9999 ;""")




conn_dwh.commit()
cursor_dwh.close()
conn_dwh.close()
