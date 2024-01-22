--Задача 1.1:	Написать прототип реализующий заполнение Target из Source по примеру											
--В источнике данные хранятся в средном виде, а целевой таблице в интервальном. Суть задачи в том чтобы перенести интервальное хранение в срезное											
--Предполагается что прототип запускается каждый день в инкрементальном режиме и переносит данные только за дату запуска											

DROP TABLE IF EXISTS source1_1;
CREATE TABLE source1_1 (
	id int,
	attr1 int,
	attr2 int,
	gregor_dt varchar
);


INSERT INTO source1_1 VALUES
(1,	11,	111,	'01.01.2023'),
(2,	22,	222,	'01.01.2023'),
(3,	33,	333,	'01.01.2023'),
(5,	55,	555,	'01.01.2023'),
(6,	66,	666,	'01.01.2023'),
(1,	11,	111,	'01.02.2023'),
(2,	22,	222,	'01.02.2023'),
(3,	33,	333,	'01.02.2023'),
(4,	44,	444,	'01.02.2023'),
(5,	55,	5555,	'01.02.2023'),
(1,	11,	111,	'01.08.2023'),
(2,	222,222,	'01.08.2023'),
(3,	33,	333,	'01.08.2023'),
(5,	55,	5555,	'01.08.2023');


DROP TABLE IF EXISTS  target1_1;
CREATE TABLE target1_1 (
	id int,
	attr1 int,
	attr2 int,
	start_dt date,
	end_dt date,
	ctl_action char(1),
	ctl_datechange date
);

DROP PROCEDURE IF EXISTS etl1_1(etl_date date);
CREATE PROCEDURE etl1_1(etl_date date) AS
$$
	BEGIN
		
		UPDATE target1_1 t
		SET end_dt = etl_date - INTERVAL '1 day',
			ctl_action = 'U',
			ctl_datechange = etl_date
		FROM source1_1 s
		WHERE 
			s.id = t.id
			AND to_date(s.gregor_dt, 'dd.MM.YYYY') = etl_date
			AND t.end_dt = '9999-12-31'
			AND (s.attr1 <> t.attr1 OR s.attr2 <> t.attr2)
			AND etl_date > t.start_dt;
		
		INSERT INTO target1_1
		WITH
		s AS(
			SELECT
				id,
				attr1,
				attr2,
				gregor_dt	
			FROM source1_1 s
			WHERE to_date(s.gregor_dt, 'dd.MM.YYYY') = etl_date
		),
		t AS(
			SELECT
				id,
				attr1,
				attr2,
				start_dt,
				end_dt,
				ctl_action,
				ctl_datechange
			FROM target1_1
			WHERE etl_date BETWEEN start_dt AND end_dt
		)
		SELECT
			s.id,
			s.attr1,
			s.attr2,
			to_date(s.gregor_dt, 'dd.MM.YYYY'),
			'9999-12-31',
			'I',
			etl_date
		FROM s
		LEFT JOIN t
			ON s.id = t.id
			AND (s.attr1 = t.attr1 AND s.attr2 = t.attr2)
		WHERE t.id IS NULL;
	
		WITH
		ids AS(
			SELECT DISTINCT id
			FROM source1_1 s 
			WHERE to_date(s.gregor_dt, 'dd.MM.YYYY') = etl_date
		)
		UPDATE target1_1 t
		SET end_dt = etl_date - INTERVAL '1 day',
			ctl_action = 'D',
			ctl_datechange = etl_date
		WHERE t.end_dt = '9999-12-31'
			AND etl_date > t.start_dt 
			AND id NOT IN (SELECT * FROM ids);
		
		COMMIT;
	
	END;
$$
LANGUAGE PLPGSQL;

CALL etl1_1('2023-01-01');

CALL etl1_1('2023-02-01');

CALL etl1_1('2023-08-01');


--Задача 1.2*:	Написать прототип забирающий из Target актуальную (последнюю) запись по каждому уникальному значению ID. 
--При этом не использовать  select * from target where end_dt='9999-12-31'	
									
SELECT
	id,
	attr1,
	attr2,
	start_dt,
	end_dt,
	ctl_action,
	ctl_datechange
FROM 
	(SELECT
		*
		,ROW_NUMBER() OVER(PARTITION BY id ORDER BY end_dt DESC) r
	FROM target1_1 t) sq
WHERE r = 1;


--Задача 2:	Написать  прототип формирующий инкремент из Source витрины на дату запуска потока для дальнейшей выгрузки в стороннюю систему			
--	Инкремени это разница между текущим и предыдущим состояникм по примеру ниже			
--	Прототип не предполагает архивного или инкрементального режима.			
--	Текущее и предыдущее состоянии нужно забирать из Source без использования view или промежуточных таблиц			
--	Инкремент нужно записывать в Target	


DROP TABLE IF EXISTS  source2;
CREATE TABLE source2 (
	id int,
	attr1 int,
	gregor_dt varchar,
	ctl_action char(1),
	ctl_datechange varchar
);

INSERT INTO source2 VALUES
(1,	11,		'20.07.2023',	'I',	'20.07.2023'),
(2,	22,		'20.07.2023',	'I',	'20.07.2023'),
(3,	33,		'20.07.2023',	'I',	'20.07.2023'),
(1,	11,		'21.07.2023',	'I',	'21.07.2023'),
(3,	333,	'21.07.2023',	'I',	'21.07.2023'),
(4,	44,		'21.07.2023',	'I',	'21.07.2023'),
(1,	11,		'22.07.2023',	'I',	'22.07.2023'),
(3,	333,	'22.07.2023',	'I',	'22.07.2023'),
(4,	444,	'22.07.2023',	'I',	'22.07.2023');


DROP TABLE IF EXISTS  target2;
CREATE TABLE target2 (
	id int,
	attr1 int,
	gregor_dt date,
	ctl_action char(1),
	ctl_datechange varchar,
	
	UNIQUE(id, attr1, gregor_dt, ctl_action)
);


DROP PROCEDURE IF EXISTS etl2(etl_date date);
CREATE PROCEDURE etl2(etl_date date) AS
$$
	
	DECLARE 
		start_date date := etl_date - INTERVAL '1 day';
		end_date date := etl_date;

	BEGIN
		
		INSERT INTO target2
		WITH 
		s AS(
			SELECT
				id,
				attr1,
				to_date(gregor_dt, 'dd.MM.YYYY')						   gregor_dt,
				ctl_action,
				to_date(ctl_datechange, 'dd.MM.YYYY')                      ctl_datechange,
				lag (attr1) OVER (PARTITION BY id ORDER BY ctl_datechange) lg,
				lead(attr1) OVER (PARTITION BY id ORDER BY ctl_datechange) ld
			FROM source2
			WHERE to_date(ctl_datechange, 'dd.MM.YYYY') BETWEEN start_date AND end_date
		),
		c AS(
			SELECT
				id,
				attr1,
				gregor_dt,
				CASE 
					WHEN ctl_datechange = end_date
						AND lg <> attr1
							THEN 'U'
					WHEN ctl_datechange = end_date
						AND lg IS NULL 
							THEN 'I'
					WHEN ctl_datechange = start_date
						AND ld IS NULL
							THEN 'D'
				END							ctl_action,
				end_date
			FROM s
		)
		SELECT
			c.id,
			c.attr1,
			c.gregor_dt,
			c.ctl_action,
			c.end_date
		FROM c
		WHERE ctl_action IS NOT NULL
		ON CONFLICT DO NOTHING;
	
	END;
	
$$
LANGUAGE PLPGSQL;

CALL etl2('2023-07-20');
CALL etl2('2023-07-21');
CALL etl2('2023-07-22');


--Задача 3:	Написать прототип который может сформировать данные в архивном и инкрементальном режиме										
--	В каждом из источников образуется новый интервал при изменении значения бизнес аттрибута 										
--	Jkmu										
--	В целевой таблице находятся бизнес аттрибуты из обоих источников. Таким образом интервалы в целевой таблице должны создаваться при изменении любого из аттрибутов										
--											
--Задача 3.1:	Сформировать данные в Target в интервальном виде при запуске потока в архивном режиме (на одну дату, например 01.08.2023)	


DROP TABLE IF EXISTS  source3_1;
CREATE TABLE source3_1 (
	id int,
	attr1 int,
	start_dt date,
	end_dt date,
	ACTION char(1),
	ts date
);


INSERT INTO source3_1 VALUES
(1,	11,    '2023-01-01', '2023-01-15', 'U', '2023-01-16'),
(1,	111,   '2023-01-16', '2023-03-05', 'U', '2023-03-06'),
(1,	1111,  '2023-03-06', '9999-12-31', 'I', '2023-03-06'),
(2,	22,    '2023-01-05', '2023-02-05', 'U', '2023-02-06'),
(2,	222,   '2023-02-06', '9999-12-31', 'I', '2023-02-06');


DROP TABLE IF EXISTS  source3_2;
CREATE TABLE source3_2 (
	id int,
	attr2 char(3),
	start_dt date,
	end_dt date,
	ACTION char(1),
	ts date
);


INSERT INTO public.source3_2 VALUES
(1,	'aaa',	    '2023-01-10', '2023-02-05', 'U', '2023-01-06'),
(1,	'bbb',	 	'2023-02-06', '9999-12-31', 'I', '2023-01-06'),
(2,	'xxx', 		'2023-01-10', '2023-02-25', 'D', '2023-02-26');


DROP TABLE IF EXISTS  target3;
CREATE TABLE target3 (
	id int,
	attr1 int,
	attr2 char(3),
	start_dt date,
	end_dt date,
	ctl_action char(1),
	ctl_datechange date
);

DROP PROCEDURE IF EXISTS etl3_1(etl_date date);
CREATE PROCEDURE etl3_1(etl_date date) AS
$$
	INSERT INTO target3
	WITH 
	sq AS(
		SELECT
			s1.id,
			s1.attr1,
			s2.attr2,
			GREATEST(s1.start_dt, s2.start_dt)  AS start_dt,
			least(s1.end_dt, s2.end_dt)  		AS end_dt,
			'I',
			etl_date
		FROM source3_1 s1
		JOIN source3_2 s2
			ON s1.id = s2.id
			AND s1.start_dt < s2.end_dt AND s2.start_dt < s1.end_dt
		WHERE s1.ts <= etl_date AND s2.ts <= etl_date
	),
	sq1 AS(
		SELECT
			*,
			LAG(end_dt, 1, '0001-01-01'::date) OVER my_window,
			LEAD(start_dt, 1, '100000-12-31'::date) OVER my_window,
			start_dt - LAG(end_dt, 1, '0001-01-01'::date) OVER my_window AS lag_delta
		FROM sq
		WINDOW my_window AS (PARTITION BY id ORDER BY end_dt)
		ORDER BY id, end_dt
	),
	sq2 AS(
		SELECT 
			id,
			NULL								AS attr1,
			NULL								AS attr2,
			lag  								AS start_dt,
			(start_dt - INTERVAL '1 DAY')::date AS end_dt
		FROM sq1
		WHERE lag_delta <> 1
		
		UNION
		
		SELECT 
			id,
			NULL								AS attr1,
			NULL								AS attr2,
			(end_dt + INTERVAL '1 DAY')::date  	AS start_dt,
			LEAD::date							AS end_dt
		FROM sq1
		WHERE LEAD = '100000-12-31' AND end_dt <> '9999-12-31'
	)
	SELECT
		sq2.id,
		coalesce(sq2.attr1::int, s1.attr1),
		coalesce(sq2.attr1, s2.attr2),
		CASE 
			WHEN sq2.start_dt = '0001-01-01' THEN COALESCE(s1.start_dt, s2.start_dt)
			ELSE sq2.start_dt
		END,
		CASE 
			WHEN sq2.end_dt = '100000-12-31' THEN COALESCE(s1.end_dt, s2.end_dt)
			ELSE sq2.end_dt
		END,
		'I',
		etl_date
	FROM sq2
	LEFT JOIN source3_1 s1
		ON sq2.id = s1.id
		AND sq2.start_dt < s1.end_dt AND s1.start_dt < sq2.end_dt
	LEFT JOIN source3_2 s2
		ON sq2.id = s2.id
		AND sq2.start_dt < s2.end_dt AND s2.start_dt < sq2.end_dt
	UNION 
	SELECT * FROM sq
$$
LANGUAGE SQL;

CALL etl3_1('2023-08-01');