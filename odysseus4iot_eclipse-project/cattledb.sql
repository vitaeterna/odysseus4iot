--1 - Create CattleDB
CREATE DATABASE "CattleDB"
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

--2 - Install postgres extension postgres_fdw
CREATE EXTENSION postgres_fdw;

--3 - Create foreign server
CREATE SERVER fdw_rawdb
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '141.13.162.179', port '5432', dbname 'rawdb');

CREATE SERVER fdw_procdb
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '141.13.162.179', port '5432', dbname 'procdb');

--4 - Create user mapping
CREATE USER MAPPING FOR postgres
SERVER fdw_rawdb
OPTIONS (user 'fdw_postgres', password 'fordatwin');

CREATE USER MAPPING FOR postgres
SERVER fdw_procdb
OPTIONS (user 'fdw_postgres', password 'fordatwin');

--5 - Create foreign tables
CREATE FOREIGN TABLE public.fdw_cattle
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    round text COLLATE pg_catalog."default" NOT NULL,
    sensor_id integer NOT NULL,
    label_id integer NOT NULL,
    farm_id text COLLATE pg_catalog."default",
    labeled_by text COLLATE pg_catalog."default",
    earmark_id text COLLATE pg_catalog."default" NOT NULL,
    collar_id integer,
    age integer,
    year_of_birth integer,
    lactation integer,
    age_of_first_fecundity double precision,
    interval_between_calvings integer,
    has_horns text COLLATE pg_catalog."default",
    has_nose_ring text COLLATE pg_catalog."default",
    avg_lifetime_production double precision,
    avg_daily_lifetime_production double precision,
    last_calving date,
    planned_calving date,
    bcs double precision,
    bcs_measurement_date date,
    lameness_score integer,
    lameness_score_measurement_date date
)
SERVER fdw_rawdb
OPTIONS (schema_name 'public', table_name 'cattle');

CREATE FOREIGN TABLE public.fdw_label_data
(
    cattle_id integer NOT NULL,
    label_id integer NOT NULL,
    begin_time bigint NOT NULL,
    end_time bigint NOT NULL,
    id integer NOT NULL
)
SERVER fdw_rawdb
OPTIONS (schema_name 'public', table_name 'label_data');

CREATE FOREIGN TABLE public.fdw_label_data_definition
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    abbreviation text COLLATE pg_catalog."default" NOT NULL,
    explanation text COLLATE pg_catalog."default" NOT NULL
)
SERVER fdw_rawdb
OPTIONS (schema_name 'public', table_name 'label_data_definition');

CREATE FOREIGN TABLE public.fdw_sensor_data_acc
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    x double precision NOT NULL,
    y double precision NOT NULL,
    z double precision NOT NULL
)
SERVER fdw_rawdb
OPTIONS (schema_name 'public', table_name 'sensor_data_acc');

CREATE FOREIGN TABLE public.fdw_sensor_data_ims
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    x double precision NOT NULL,
    y double precision NOT NULL,
    z double precision NOT NULL
)
SERVER fdw_rawdb
OPTIONS (schema_name 'public', table_name 'sensor_data_ims');

CREATE FOREIGN TABLE public.fdw_experiment_result
(
    model_title text COLLATE pg_catalog."default",
    model_init_name text COLLATE pg_catalog."default",
    model_binary_content bytea,
    features_json_content json,
    model_comments text COLLATE pg_catalog."default",
    train_table text COLLATE pg_catalog."default",
    monitor_table text COLLATE pg_catalog."default",
    no_of_predicted_classes integer,
    list_of_predicted_classes text COLLATE pg_catalog."default",
    original_sample_rate_in_hz integer,
    no_of_original_train_data_points integer,
    resampled_rate_in_hz integer,
    no_of_resampled_train_data_points integer,
    no_of_instances_for_each_class_in_resampled_train_table integer,
    algorithm text COLLATE pg_catalog."default",
    no_of_functions integer,
    list_of_functions text COLLATE pg_catalog."default",
    no_of_axes integer,
    list_of_axes text COLLATE pg_catalog."default",
    window_size integer,
    window_stride text COLLATE pg_catalog."default",
    k_fold integer,
    accuracy_train_valid real,
    precision_train_valid real,
    recall_train_valid real,
    specificity_train_valid real,
    f1_train_valid real,
    accuracy_test real,
    precision_test real,
    recall_test real,
    specificity_test real,
    f1_test real,
    monitoring_window_stride text COLLATE pg_catalog."default",
    accuracy_monitor real,
    precision_monitor real,
    recall_monitor real,
    specificity_monitor real,
    f1_monitor real,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    running_time_in_minutes text COLLATE pg_catalog."default"
)
SERVER fdw_procdb
OPTIONS (schema_name 'public', table_name 'experiment_result');

--6 - Create tables
CREATE TABLE IF NOT EXISTS public.cattle
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    round text COLLATE pg_catalog."default" NOT NULL,
    sensor_id integer NOT NULL,
    label_id integer NOT NULL,
    farm_id text COLLATE pg_catalog."default",
    labeled_by text COLLATE pg_catalog."default",
    earmark_id text COLLATE pg_catalog."default" NOT NULL,
    collar_id integer,
    age integer,
    year_of_birth integer,
    lactation integer,
    age_of_first_fecundity double precision,
    interval_between_calvings integer,
    has_horns text COLLATE pg_catalog."default",
    has_nose_ring text COLLATE pg_catalog."default",
    avg_lifetime_production double precision,
    avg_daily_lifetime_production double precision,
    last_calving date,
    planned_calving date,
    bcs double precision,
    bcs_measurement_date date,
    lameness_score integer,
    lameness_score_measurement_date date,
    CONSTRAINT cattle_pkey PRIMARY KEY (id)
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.label_data
(
    cattle_id integer NOT NULL,
    label_id integer NOT NULL,
    begin_time bigint NOT NULL,
    end_time bigint NOT NULL
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.label_data_definition
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    abbreviation text COLLATE pg_catalog."default" NOT NULL,
    explanation text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT label_data_definition_pkey PRIMARY KEY (id)
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.sensor_data_acc
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    x double precision NOT NULL,
    y double precision NOT NULL,
    z double precision NOT NULL
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.sensor_data_ims
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    x double precision NOT NULL,
    y double precision NOT NULL,
    z double precision NOT NULL
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.experiment_result
(
    model_title text COLLATE pg_catalog."default",
    model_init_name text COLLATE pg_catalog."default",
    model_binary_content bytea,
    features_json_content json,
    model_comments text COLLATE pg_catalog."default",
    train_table text COLLATE pg_catalog."default",
    monitor_table text COLLATE pg_catalog."default",
    no_of_predicted_classes integer,
    list_of_predicted_classes text COLLATE pg_catalog."default",
    original_sample_rate_in_hz integer,
    no_of_original_train_data_points integer,
    resampled_rate_in_hz integer,
    no_of_resampled_train_data_points integer,
    no_of_instances_for_each_class_in_resampled_train_table integer,
    algorithm text COLLATE pg_catalog."default",
    no_of_functions integer,
    list_of_functions text COLLATE pg_catalog."default",
    no_of_axes integer,
    list_of_axes text COLLATE pg_catalog."default",
    window_size integer,
    window_stride text COLLATE pg_catalog."default",
    k_fold integer,
    accuracy_train_valid real,
    precision_train_valid real,
    recall_train_valid real,
    specificity_train_valid real,
    f1_train_valid real,
    accuracy_test real,
    precision_test real,
    recall_test real,
    specificity_test real,
    f1_test real,
    monitoring_window_stride text COLLATE pg_catalog."default",
    accuracy_monitor real,
    precision_monitor real,
    recall_monitor real,
    specificity_monitor real,
    f1_monitor real,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    running_time_in_minutes text COLLATE pg_catalog."default"
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

--7 - Insert data from foreign tables
INSERT INTO public.cattle(id, name, round, sensor_id, label_id, farm_id, labeled_by, earmark_id, collar_id, age, year_of_birth, lactation, age_of_first_fecundity, interval_between_calvings, has_horns, has_nose_ring, avg_lifetime_production, avg_daily_lifetime_production, last_calving, planned_calving, bcs, bcs_measurement_date, lameness_score, lameness_score_measurement_date)
SELECT id, name, round, sensor_id, label_id, farm_id, labeled_by, earmark_id, collar_id, age, year_of_birth, lactation, age_of_first_fecundity, interval_between_calvings, has_horns, has_nose_ring, avg_lifetime_production, avg_daily_lifetime_production, last_calving, planned_calving, bcs, bcs_measurement_date, lameness_score, lameness_score_measurement_date
FROM public.fdw_cattle
ORDER BY id ASC;

INSERT INTO public.label_data(cattle_id, label_id, begin_time, end_time)
SELECT cattle_id, label_id, begin_time, end_time
FROM public.fdw_label_data
ORDER BY begin_time ASC;

INSERT INTO public.label_data_definition(id, name, abbreviation, explanation)
SELECT id, name, abbreviation, explanation
FROM public.fdw_label_data_definition
ORDER BY id ASC;

INSERT INTO public.sensor_data_acc(cattle_id, "timestamp", x, y, z)
SELECT cattle_id, "timestamp", x, y, z
FROM public.fdw_sensor_data_acc
ORDER BY "timestamp" ASC;

INSERT INTO public.sensor_data_ims(cattle_id, "timestamp", x, y, z)
SELECT cattle_id, "timestamp", x, y, z
FROM public.fdw_sensor_data_ims
ORDER BY "timestamp" ASC;

INSERT INTO public.experiment_result(model_title, model_init_name, model_binary_content, features_json_content, model_comments, train_table, monitor_table, no_of_predicted_classes, list_of_predicted_classes, original_sample_rate_in_hz, no_of_original_train_data_points, resampled_rate_in_hz, no_of_resampled_train_data_points, no_of_instances_for_each_class_in_resampled_train_table, algorithm, no_of_functions, list_of_functions, no_of_axes, list_of_axes, window_size, window_stride, k_fold, accuracy_train_valid, precision_train_valid, recall_train_valid, specificity_train_valid, f1_train_valid, accuracy_test, precision_test, recall_test, specificity_test, f1_test, monitoring_window_stride, accuracy_monitor, precision_monitor, recall_monitor, specificity_monitor, f1_monitor, start_time, end_time, running_time_in_minutes)
SELECT model_title, model_init_name, model_binary_content, features_json_content, model_comments, train_table, monitor_table, no_of_predicted_classes, list_of_predicted_classes, original_sample_rate_in_hz, no_of_original_train_data_points, resampled_rate_in_hz, no_of_resampled_train_data_points, no_of_instances_for_each_class_in_resampled_train_table, algorithm, no_of_functions, list_of_functions, no_of_axes, list_of_axes, window_size, window_stride, k_fold, accuracy_train_valid, precision_train_valid, recall_train_valid, specificity_train_valid, f1_train_valid, accuracy_test, precision_test, recall_test, specificity_test, f1_test, monitoring_window_stride, accuracy_monitor, precision_monitor, recall_monitor, specificity_monitor, f1_monitor, start_time, end_time, running_time_in_minutes
FROM public.fdw_experiment_result
ORDER BY start_time ASC;

--8 - Create custom tables
CREATE TABLE IF NOT EXISTS public.sensor_data
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    ax double precision NOT NULL,
    ay double precision NOT NULL,
    az double precision NOT NULL,
    ox double precision NOT NULL,
    oy double precision NOT NULL,
    oz double precision NOT NULL
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

INSERT INTO public.sensor_data(cattle_id, "timestamp", ax, ay, az, ox, oy, oz)
SELECT sensor_data_acc.cattle_id, sensor_data_acc."timestamp", sensor_data_acc.x, sensor_data_acc.y, sensor_data_acc.z, sensor_data_ims.x, sensor_data_ims.y, sensor_data_ims.z
FROM sensor_data_acc, sensor_data_ims
WHERE sensor_data_acc.cattle_id = sensor_data_ims.cattle_id
AND sensor_data_acc."timestamp" = sensor_data_ims."timestamp"
ORDER BY sensor_data_acc."timestamp" ASC;

CREATE TABLE IF NOT EXISTS public.sensor_data_19
(
    cattle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    ax double precision NOT NULL,
    ay double precision NOT NULL,
    az double precision NOT NULL,
    ox double precision NOT NULL,
    oy double precision NOT NULL,
    oz double precision NOT NULL
)
WITH
(
    OIDS = FALSE
)
TABLESPACE pg_default;

INSERT INTO public.sensor_data_19(cattle_id, "timestamp", ax, ay, az, ox, oy, oz)
SELECT cattle_id, "timestamp", ax, ay, az, ox, oy, oz
FROM sensor_data
WHERE cattle_id = 19
ORDER BY "timestamp" ASC;

--Sensor Tuple Count per Cattle
SELECT sensor_data.cattle_id, cattle.name, cattle.round, COUNT(*)
FROM sensor_data, cattle
WHERE sensor_data.cattle_id = cattle.id
GROUP BY sensor_data.cattle_id, cattle.name, cattle.round
ORDER BY COUNT(*) DESC;

--Duplicate Check
SELECT cattle_id, "timestamp", x, y, z, COUNT(*) AS counter
FROM public.sensor_data_acc
GROUP BY cattle_id, "timestamp", x, y, z
HAVING COUNT(*) > 1
ORDER BY counter DESC;

--Min Max Timestamp
SELECT fdw_cattle.earmark_id, fdw_cattle.round, min(fdw_sensor_data_ims.timestamp), max(fdw_sensor_data_ims.timestamp)
FROM fdw_cattle, fdw_sensor_data_ims
WHERE fdw_cattle.id = fdw_sensor_data_ims.cattle_id
GROUP BY fdw_cattle.round, fdw_cattle.earmark_id;