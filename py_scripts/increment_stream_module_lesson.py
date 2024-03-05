#!/usr/bin/python3
# Инкрементальная загрузка tream_module_lesson
import psycopg2

conn_main = psycopg2.connect(database = "main",
                        host =     "localhost",
                        user =     "postgres",
                        password = "postgres_password",
                        port =     "5433")

conn_dwh = psycopg2.connect(database = "dwh",
                        host =     "localhost",
                        user =     "postgres",
                        password = "postgres_password",
                        port =     "5433")

conn_main.autocommit = False
cursor_main = conn_main.cursor()
conn_dwh.autocommit = False
cursor_dwh = conn_dwh.cursor()

# 1 Очистка стейджинговых таблиц
cursor_main.execute("""
delete from dwh.stream_module_lesson_stg;""")
cursor_main.execute("""
delete from dwh.stream_module_lesson_stg_del;""")

# перенос данных из main в dwh
cursor_main.execute("select max_update_dt from dwh.turn_meta where schema_name='dwh' and table_name='stream_module_lesson'")
meta_acc_date = cursor_main.fetchone()
cursor_dwh.execute(f"""    select 
								id, 
								title, 
								description,
								start_at, 
								end_at, 
								teacher_id, 
								stream_module_id, 
								update_dt
                            from main.stream_module_lesson
                            where (update_dt is not null and update_dt > to_timestamp('{meta_acc_date[-1]}', 'YYYY-MM-DD HH24:MI:SS'))
                            or (update_dt is null and create_dt > to_timestamp('{meta_acc_date[-1]}', 'YYYY-MM-DD HH24:MI:SS'))
                            """)
row = cursor_dwh.fetchone()
while row is not None:
    cursor_main.execute(""" INSERT INTO dwh.stream_module_lesson_stg( 
                                id, 
								title, 
								description,
								start_at, 
								end_at, 
								teacher_id, 
								stream_module_id, 
								update_dt
                            ) 
                            VALUES( %s, %s, %s, %s, %s, %s, %s, %s ) """, row)
    row = cursor_dwh.fetchone()

# 3 Захват в стейджинг ключей из источника полным срезом для вычисления удалений
cursor_dwh.execute("select id from main.stream_module_lesson")
row_id = cursor_dwh.fetchone()
while row_id is not None:
    cursor_main.execute("""
    insert into dwh.stream_module_lesson_stg_del(id)
    values(%s);""", row_id)
    row_id = cursor_dwh.fetchone()

# 4 Вставка новых данных в target
cursor_main.execute("""
insert into dwh.stream_module_lesson_hist (
	id, 
	title, 
	description,
	start_at, 
	end_at, 
	teacher_id, 
	stream_module_id, 
	start_dt,
	end_dt
)
select 
	stg.id, 
	stg.title, 
	stg.description,
	stg.start_at, 
	stg.end_at, 
	stg.teacher_id, 
	stg.stream_module_id, 
	stg.update_dt, 
	to_date( '9999-12-31', 'YYYY-MM-DD' )
from dwh.stream_module_lesson_stg stg
left join dwh.stream_module_lesson_hist tgt 
on stg.id = tgt.id
	and extract(year from tgt.end_dt) = 9999
	and tgt.deleted_flg = 'N'
where tgt.id is null;
""")

# 5 Обновление данных в target
cursor_main.execute("""
update dwh.stream_module_lesson_hist
set
	end_dt = tmp.update_dt - interval '1 day'
from (
	select
		stg.id,
		stg.update_dt 
	from stream_module_lesson_stg stg
	inner join dwh.stream_module_lesson_hist tgt 
	on stg.id = tgt.id
		and extract(year from tgt.end_dt) = 9999
		and tgt.deleted_flg = 'N'
	where stg.update_dt is not null and
	(stg.title <> tgt.title or (stg.title is null and tgt.title is not null) or (stg.title is not null and tgt.title is null))
	or (stg.description <> tgt.description or (stg.description is null and tgt.description is not null) or (stg.description is not null and tgt.description is null))
    or (stg.teacher_id <> tgt.teacher_id or (stg.teacher_id is null and tgt.teacher_id is not null) or (stg.teacher_id is not null and tgt.teacher_id is null))
    or (stg.stream_module_id <> tgt.stream_module_id or (stg.stream_module_id is null and tgt.stream_module_id is not null) or (stg.stream_module_id is not null and tgt.stream_module_id is null))
	or (stg.start_at <> tgt.start_at or (stg.start_at is null and tgt.start_at is not null) or (stg.start_at is not null and tgt.start_at is null))
	or (stg.end_at <> tgt.end_at or (stg.end_at is null and tgt.end_at is not null) or (stg.end_at is not null and tgt.end_at is null))
) tmp
where dwh.stream_module_lesson_hist.id = tmp.id;
""")

cursor_main.execute("""
insert into dwh.stream_module_lesson_hist (
	id, 
	title, 
	description,
	start_at, 
	end_at, 
	teacher_id, 
	stream_module_id, 
	start_dt, 
	end_dt
)
select 
	stg.id, 
	stg.title, 
	stg.description,
	stg.start_at, 
	stg.end_at, 
	stg.teacher_id, 
	stg.stream_module_id, 
	stg.update_dt, 
	to_date( '9999-12-31', 'YYYY-MM-DD' )
from stream_module_lesson_stg stg
inner join dwh.stream_module_lesson_hist tgt 
on stg.id = tgt.id
	and tgt.end_dt = stg.update_dt - interval '1 second'
    and tgt.deleted_flg = 'N'
where stg.update_dt is not null and
(stg.title <> tgt.title or (stg.title is null and tgt.title is not null) or (stg.title is not null and tgt.title is null))
	or (stg.description <> tgt.description or (stg.description is null and tgt.description is not null) or (stg.description is not null and tgt.description is null))
    or (stg.teacher_id <> tgt.teacher_id or (stg.teacher_id is null and tgt.teacher_id is not null) or (stg.teacher_id is not null and tgt.teacher_id is null))
    or (stg.stream_module_id <> tgt.stream_module_id or (stg.stream_module_id is null and tgt.stream_module_id is not null) or (stg.stream_module_id is not null and tgt.stream_module_id is null))
	or (stg.start_at <> tgt.start_at or (stg.start_at is null and tgt.start_at is not null) or (stg.start_at is not null and tgt.start_at is null))
	or (stg.end_at <> tgt.end_at or (stg.end_at is null and tgt.end_at is not null) or (stg.end_at is not null and tgt.end_at is null))
""")

# 6 Удаление данных в target
cursor_main.execute("""
insert into dwh.stream_module_lesson_hist  (
	id, 
	title, 
	description,
	start_at, 
	end_at, 
	teacher_id, 
	stream_module_id, 
	start_dt, 
	end_dt,
	deleted_flg
)
select
	tgt.id, 
	tgt.title, 
	tgt.description,
	tgt.start_at, 
	tgt.end_at, 
	tgt.teacher_id, 
	tgt.stream_module_id, 
	now(),
	to_timestamp('9999-12-31', 'YYYY-MM-DD'),
	'Y' as deleted_flg
from dwh.stream_module_lesson_hist tgt
left join dwh.stream_module_lesson_stg_del stg
on tgt.id = stg.id 
	and extract(year from tgt.end_dt) = 9999
	and tgt.deleted_flg = 'N'
where stg.id is null 
and extract(year from tgt.end_dt) = 9999
and tgt.deleted_flg = 'N';
""")

cursor_main.execute("""
update dwh.stream_module_lesson_hist
set
	end_dt = now() - interval '1 minute'
where stream_module_lesson_hist.id in (
	select 
		tgt.id
	from dwh.stream_module_lesson_hist tgt
	left join dwh.stream_module_lesson_stg_del stg 
	on tgt.id = stg.id
	where stg.id is null
		and extract(year from tgt.end_dt) = 9999
		and tgt.deleted_flg = 'N'
)
and extract(year from end_dt) = 9999
and deleted_flg = 'N';
""")

# 7 Обновление meta
cursor_main.execute("""
update dwh.turn_meta
set 
	max_update_dt = coalesce(
		(
		select 
			coalesce(max(update_dt), max(create_dt))
		from dwh.stream_module_lesson_stg
		), 
		(
		select 
			max_update_dt 
		from dwh.turn_meta
		where schema_name = 'dwh' and table_name = 'stream_module_lesson_stg'
		)
	)
where schema_name = 'dwh' and table_name = 'stream_module_lesson_stg';
""")
conn_main.commit()
cursor_main.close()
conn_main.close()
cursor_dwh.close()
conn_dwh.close()