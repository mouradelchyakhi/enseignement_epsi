--
-- PostgreSQL database cluster dump
--

\restrict YvEpWfDE8qJQ2waT3dew3vm7zc4VlFNTxNbyz7ONZX8pi8qVzdXLu4LydlSjEsy

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

ALTER ROLE postgres WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:qXWtMHCjDKXoLBp7S3TfhQ==$St3vANCOayKw2EIx14lSO0P87dJ067Jg+Di7tWjihwc=:nGiTaxF8k5t/NrviHuQ12LLQtn3Lwxv9SgoFhF/R4Ys=';

--
-- User Configurations
--








\unrestrict YvEpWfDE8qJQ2waT3dew3vm7zc4VlFNTxNbyz7ONZX8pi8qVzdXLu4LydlSjEsy

--
-- Databases
--

--
-- Database "template1" dump
--

\connect template1

--
-- PostgreSQL database dump
--

\restrict rkcHHmsjjft782wG1VnhtoRa0kXaZWx0Or9HaboGcqIVUsp7OdBm5pXwLIZI4rc

-- Dumped from database version 18.0 (Debian 18.0-1.pgdg13+3)
-- Dumped by pg_dump version 18.0 (Debian 18.0-1.pgdg13+3)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- PostgreSQL database dump complete
--

\unrestrict rkcHHmsjjft782wG1VnhtoRa0kXaZWx0Or9HaboGcqIVUsp7OdBm5pXwLIZI4rc

--
-- Database "postgres" dump
--

\connect postgres

--
-- PostgreSQL database dump
--

\restrict 4IJhCeuFWTmIKKu4iLJ1xk9At0OZ0lQM6NuiYcQtI75YiOeZjCRlhIU7hT4d9zz

-- Dumped from database version 18.0 (Debian 18.0-1.pgdg13+3)
-- Dumped by pg_dump version 18.0 (Debian 18.0-1.pgdg13+3)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA raw;
DROP schema public;


ALTER SCHEMA raw OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: api_request_logs; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.api_request_logs (
    request_id uuid DEFAULT gen_random_uuid() NOT NULL,
    endpoint character varying(100),
    response_code integer,
    request_time timestamp without time zone
);


ALTER TABLE raw.api_request_logs OWNER TO postgres;

--
-- Name: bike_maintenance_logs; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.bike_maintenance_logs (
    log_id integer NOT NULL,
    bike_id integer,
    report_date timestamp without time zone,
    issue_description text,
    cost_eur numeric(6,2)
);


ALTER TABLE raw.bike_maintenance_logs OWNER TO postgres;

--
-- Name: bike_maintenance_logs_log_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.bike_maintenance_logs_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.bike_maintenance_logs_log_id_seq OWNER TO postgres;

--
-- Name: bike_maintenance_logs_log_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.bike_maintenance_logs_log_id_seq OWNED BY raw.bike_maintenance_logs.log_id;


--
-- Name: bike_part_orders; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.bike_part_orders (
    order_id integer NOT NULL,
    part_sku character varying(50),
    quantity integer,
    order_date date,
    supplier_name character varying(100)
);


ALTER TABLE raw.bike_part_orders OWNER TO postgres;

--
-- Name: bike_part_orders_order_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.bike_part_orders_order_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.bike_part_orders_order_id_seq OWNER TO postgres;

--
-- Name: bike_part_orders_order_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.bike_part_orders_order_id_seq OWNED BY raw.bike_part_orders.order_id;


--
-- Name: bike_rentals; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.bike_rentals (
    rental_id bigint NOT NULL,
    bike_id integer,
    user_id uuid,
    start_station_id character varying(10),
    end_station_id character varying(10),
    start_t text,
    end_t text
);


ALTER TABLE raw.bike_rentals OWNER TO postgres;

--
-- Name: bike_stations; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.bike_stations (
    station_id character varying(10) NOT NULL,
    station_name character varying(255),
    latitude text,
    longitude text,
    capacity integer,
    city_id integer
);


ALTER TABLE raw.bike_stations OWNER TO postgres;

--
-- Name: bikes; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.bikes (
    bike_id integer NOT NULL,
    bike_type character varying(50),
    model_name character varying(100),
    commissioning_date date,
    status character varying(50)
);


ALTER TABLE raw.bikes OWNER TO postgres;

--
-- Name: billing_invoices; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.billing_invoices (
    invoice_id character varying(50) NOT NULL,
    user_id uuid,
    subscription_id character varying(50),
    issue_date date,
    amount_eur numeric(5,2),
    status character varying(20)
);


ALTER TABLE raw.billing_invoices OWNER TO postgres;

--
-- Name: cities; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.cities (
    city_id integer NOT NULL,
    city_name character varying(100),
    region character varying(100)
);


ALTER TABLE raw.cities OWNER TO postgres;

--
-- Name: customer_support_tickets; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.customer_support_tickets (
    ticket_id integer NOT NULL,
    user_id uuid,
    created_at timestamp without time zone,
    subject character varying(255),
    status character varying(20)
);


ALTER TABLE raw.customer_support_tickets OWNER TO postgres;

--
-- Name: customer_support_tickets_ticket_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.customer_support_tickets_ticket_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.customer_support_tickets_ticket_id_seq OWNER TO postgres;

--
-- Name: customer_support_tickets_ticket_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.customer_support_tickets_ticket_id_seq OWNED BY raw.customer_support_tickets.ticket_id;


--
-- Name: daily_activity_summary_old; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.daily_activity_summary_old (
    summary_date date NOT NULL,
    total_rentals integer,
    total_revenue_eur numeric(10,2)
);


ALTER TABLE raw.daily_activity_summary_old OWNER TO postgres;

--
-- Name: email_sends_log; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.email_sends_log (
    email_log_id integer NOT NULL,
    user_id uuid,
    campaign_id character varying(20),
    sent_at timestamp without time zone,
    status character varying(20)
);


ALTER TABLE raw.email_sends_log OWNER TO postgres;

--
-- Name: email_sends_log_email_log_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.email_sends_log_email_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.email_sends_log_email_log_id_seq OWNER TO postgres;

--
-- Name: email_sends_log_email_log_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.email_sends_log_email_log_id_seq OWNED BY raw.email_sends_log.email_log_id;


--
-- Name: employee_payrolls; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.employee_payrolls (
    payroll_id integer NOT NULL,
    employee_id integer,
    pay_date date,
    monthly_salary_eur integer
);


ALTER TABLE raw.employee_payrolls OWNER TO postgres;

--
-- Name: employee_payrolls_payroll_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.employee_payrolls_payroll_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.employee_payrolls_payroll_id_seq OWNER TO postgres;

--
-- Name: employee_payrolls_payroll_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.employee_payrolls_payroll_id_seq OWNED BY raw.employee_payrolls.payroll_id;


--
-- Name: employees; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.employees (
    employee_id integer NOT NULL,
    first_name character varying(100),
    last_name character varying(100),
    job_title character varying(100),
    office_city_id integer
);


ALTER TABLE raw.employees OWNER TO postgres;

--
-- Name: employees_employee_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.employees_employee_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.employees_employee_id_seq OWNER TO postgres;

--
-- Name: employees_employee_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.employees_employee_id_seq OWNED BY raw.employees.employee_id;


--
-- Name: logistics_fleet_vehicles; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.logistics_fleet_vehicles (
    vehicle_id character varying(20) NOT NULL,
    vehicle_type character varying(50),
    license_plate character varying(10),
    current_city_id integer
);


ALTER TABLE raw.logistics_fleet_vehicles OWNER TO postgres;

--
-- Name: marketing_campaigns; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.marketing_campaigns (
    campaign_id character varying(20) NOT NULL,
    campaign_name character varying(100),
    start_date date,
    end_date date,
    budget_eur integer
);


ALTER TABLE raw.marketing_campaigns OWNER TO postgres;

--
-- Name: rental_transactions; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.rental_transactions (
    transaction_id character varying(100) NOT NULL,
    rental_id bigint,
    payment_provider character varying(50),
    processed_at timestamp without time zone,
    status character varying(20)
);


ALTER TABLE raw.rental_transactions OWNER TO postgres;

--
-- Name: rentals_archive_2022; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.rentals_archive_2022 (
    rental_id bigint NOT NULL,
    bike_id integer,
    user_id uuid,
    start_t timestamp without time zone,
    end_t timestamp without time zone
);


ALTER TABLE raw.rentals_archive_2022 OWNER TO postgres;

--
-- Name: station_hardware_inventory; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.station_hardware_inventory (
    hardware_id integer NOT NULL,
    station_id character varying(10),
    terminal_model character varying(50),
    install_date date
);


ALTER TABLE raw.station_hardware_inventory OWNER TO postgres;

--
-- Name: station_hardware_inventory_hardware_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.station_hardware_inventory_hardware_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.station_hardware_inventory_hardware_id_seq OWNER TO postgres;

--
-- Name: station_hardware_inventory_hardware_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.station_hardware_inventory_hardware_id_seq OWNED BY raw.station_hardware_inventory.hardware_id;


--
-- Name: station_maintenance_schedule; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.station_maintenance_schedule (
    maintenance_id integer NOT NULL,
    station_id character varying(10),
    planned_date date,
    technician_name character varying(100),
    status character varying(20)
);


ALTER TABLE raw.station_maintenance_schedule OWNER TO postgres;

--
-- Name: station_maintenance_schedule_maintenance_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.station_maintenance_schedule_maintenance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.station_maintenance_schedule_maintenance_id_seq OWNER TO postgres;

--
-- Name: station_maintenance_schedule_maintenance_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.station_maintenance_schedule_maintenance_id_seq OWNED BY raw.station_maintenance_schedule.maintenance_id;


--
-- Name: subscriptions; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.subscriptions (
    subscription_id character varying(50) NOT NULL,
    subscription_type character varying(100),
    price_eur numeric(5,2)
);


ALTER TABLE raw.subscriptions OWNER TO postgres;

--
-- Name: supplier_invoices; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.supplier_invoices (
    supplier_invoice_id character varying(50) NOT NULL,
    supplier_name character varying(100),
    item_description character varying(255),
    total_eur numeric(8,2)
);


ALTER TABLE raw.supplier_invoices OWNER TO postgres;

--
-- Name: user_accounts; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.user_accounts (
    user_id uuid NOT NULL,
    first_name character varying(100),
    last_name character varying(100),
    email character varying(150),
    birthdate character varying(50),
    registration_date text,
    subscription_id character varying(50)
);


ALTER TABLE raw.user_accounts OWNER TO postgres;

--
-- Name: user_accounts_deprecated; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.user_accounts_deprecated (
    user_id uuid NOT NULL,
    email character varying(150),
    birthdate character varying(50),
    registration_date text
);


ALTER TABLE raw.user_accounts_deprecated OWNER TO postgres;

--
-- Name: user_session_logs; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.user_session_logs (
    session_id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid,
    login_time timestamp without time zone,
    duration_seconds integer,
    device_type character varying(50)
);


ALTER TABLE raw.user_session_logs OWNER TO postgres;

--
-- Name: vehicle_gps_tracking; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.vehicle_gps_tracking (
    ping_id bigint NOT NULL,
    vehicle_id character varying(20),
    ping_time timestamp without time zone,
    latitude text,
    longitude text
);


ALTER TABLE raw.vehicle_gps_tracking OWNER TO postgres;

--
-- Name: vehicle_gps_tracking_ping_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.vehicle_gps_tracking_ping_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.vehicle_gps_tracking_ping_id_seq OWNER TO postgres;

--
-- Name: vehicle_gps_tracking_ping_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.vehicle_gps_tracking_ping_id_seq OWNED BY raw.vehicle_gps_tracking.ping_id;


--
-- Name: weather_forecast_hourly; Type: TABLE; Schema: raw; Owner: postgres
--

CREATE TABLE raw.weather_forecast_hourly (
    forecast_id integer NOT NULL,
    city_id integer,
    forecast_time timestamp without time zone,
    temperature_celsius numeric(4,1),
    precipitation_mm numeric(4,1)
);


ALTER TABLE raw.weather_forecast_hourly OWNER TO postgres;

--
-- Name: weather_forecast_hourly_forecast_id_seq; Type: SEQUENCE; Schema: raw; Owner: postgres
--

CREATE SEQUENCE raw.weather_forecast_hourly_forecast_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE raw.weather_forecast_hourly_forecast_id_seq OWNER TO postgres;

--
-- Name: weather_forecast_hourly_forecast_id_seq; Type: SEQUENCE OWNED BY; Schema: raw; Owner: postgres
--

ALTER SEQUENCE raw.weather_forecast_hourly_forecast_id_seq OWNED BY raw.weather_forecast_hourly.forecast_id;


--
-- Name: bike_maintenance_logs log_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_maintenance_logs ALTER COLUMN log_id SET DEFAULT nextval('raw.bike_maintenance_logs_log_id_seq'::regclass);


--
-- Name: bike_part_orders order_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_part_orders ALTER COLUMN order_id SET DEFAULT nextval('raw.bike_part_orders_order_id_seq'::regclass);


--
-- Name: customer_support_tickets ticket_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.customer_support_tickets ALTER COLUMN ticket_id SET DEFAULT nextval('raw.customer_support_tickets_ticket_id_seq'::regclass);


--
-- Name: email_sends_log email_log_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.email_sends_log ALTER COLUMN email_log_id SET DEFAULT nextval('raw.email_sends_log_email_log_id_seq'::regclass);


--
-- Name: employee_payrolls payroll_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.employee_payrolls ALTER COLUMN payroll_id SET DEFAULT nextval('raw.employee_payrolls_payroll_id_seq'::regclass);


--
-- Name: employees employee_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.employees ALTER COLUMN employee_id SET DEFAULT nextval('raw.employees_employee_id_seq'::regclass);


--
-- Name: station_hardware_inventory hardware_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.station_hardware_inventory ALTER COLUMN hardware_id SET DEFAULT nextval('raw.station_hardware_inventory_hardware_id_seq'::regclass);


--
-- Name: station_maintenance_schedule maintenance_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.station_maintenance_schedule ALTER COLUMN maintenance_id SET DEFAULT nextval('raw.station_maintenance_schedule_maintenance_id_seq'::regclass);


--
-- Name: vehicle_gps_tracking ping_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.vehicle_gps_tracking ALTER COLUMN ping_id SET DEFAULT nextval('raw.vehicle_gps_tracking_ping_id_seq'::regclass);


--
-- Name: weather_forecast_hourly forecast_id; Type: DEFAULT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.weather_forecast_hourly ALTER COLUMN forecast_id SET DEFAULT nextval('raw.weather_forecast_hourly_forecast_id_seq'::regclass);


--
-- Data for Name: api_request_logs; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.api_request_logs (request_id, endpoint, response_code, request_time) FROM stdin;
0da9ca52-3f9d-42c9-a1bc-9dedcf3ed700	/api/v1/user	200	2025-11-11 17:15:51.044677
290c46d4-6e42-401b-8ab3-bcef155e8fdd	/api/v1/user	200	2025-11-11 17:23:04.173337
e55a86fb-9890-4d63-a1ec-340786f4db0a	/api/v1/user	200	2025-11-11 17:40:59.119495
59cdbfd8-afdf-48df-be8f-a75261718ff6	/api/v1/user	200	2025-11-11 17:52:25.497279
564c7c74-97fc-4b0d-b663-ebc46a116217	/api/v1/stations	404	2025-11-11 17:19:04.645874
2081cbbc-ea6a-4add-a48f-730c972a0760	/api/v1/stations	200	2025-11-11 17:34:46.594132
0ab5d5c9-962f-459b-b05d-24d21297b8d4	/api/v1/rent	404	2025-11-11 17:34:39.086533
d3411cb0-5354-44ac-bfdd-c4ab9a0c2592	/api/v1/rent	200	2025-11-11 17:00:30.234361
711b78d0-0797-45c3-874e-5ed058873b20	/api/v1/rent	200	2025-11-11 17:25:34.005106
42676915-cd60-49e2-8139-e4f28a19d14c	/api/v1/user	200	2025-11-11 16:59:54.815863
532facf0-28fb-4e0f-baaa-c1d82fadb313	/api/v1/stations	200	2025-11-11 17:34:50.363467
e4a200b9-3332-42fa-9fbc-6cb56a296bd7	/api/v1/user	200	2025-11-11 17:22:24.766141
7f66abf0-8c84-4e70-9f86-3bb29d7b849c	/api/v1/stations	200	2025-11-11 17:00:42.786559
b436a241-704f-4971-9247-8a089043e7f3	/api/v1/user	404	2025-11-11 17:06:37.110836
56b00427-e7c2-4f9f-ae5b-250228ad3c0d	/api/v1/stations	404	2025-11-11 17:04:04.135545
dcaf238c-f495-4b7a-90f7-a097cfcbc82a	/api/v1/user	500	2025-11-11 17:23:38.497361
297779c8-7123-41ef-bacb-10557ad7ecc2	/api/v1/user	404	2025-11-11 17:39:01.149729
906d94cc-202c-44b2-8180-f84440666f12	/api/v1/user	404	2025-11-11 17:03:29.665702
16c814fb-ace1-47b7-83dd-1a779c29fed1	/api/v1/rent	200	2025-11-11 17:12:35.134766
f7ea6f7b-9b77-484e-be56-809666eec6c1	/api/v1/user	200	2025-11-11 17:24:40.53766
fb5f480d-4b3c-435a-a701-26dcce35cd6c	/api/v1/stations	404	2025-11-11 17:31:31.130231
e4a9fd83-5588-46f0-9c39-21333ca47cb9	/api/v1/rent	500	2025-11-11 17:35:12.954664
850372d3-d0d5-4678-aaac-e1b41d0f529c	/api/v1/stations	404	2025-11-11 17:02:51.575691
cc308368-8f0a-41ab-8f99-933636599aee	/api/v1/stations	200	2025-11-11 17:04:33.540691
93799cad-70b9-410c-8a62-48f7e7a1bddf	/api/v1/stations	200	2025-11-11 17:28:28.08232
4f3f9682-6847-4344-8b2d-ac189be93be8	/api/v1/user	404	2025-11-11 17:31:26.90824
35c973ed-4687-4b58-904a-03b7179f891d	/api/v1/stations	404	2025-11-11 17:05:22.955844
d55fa2b9-b35d-445f-a3c1-3b2e9fea06bc	/api/v1/stations	200	2025-11-11 17:27:01.704907
4efdd970-e4ed-4a22-9ab6-988b75bf5a61	/api/v1/rent	404	2025-11-11 17:34:01.091194
99f5f78b-fab9-40bf-876d-4b9249aeff4a	/api/v1/rent	200	2025-11-11 17:53:00.403974
5f3f3673-ae30-4fcc-a313-118a6961d703	/api/v1/user	500	2025-11-11 17:33:16.735543
35f8a279-bbd7-4b28-bff4-c097f250221f	/api/v1/user	200	2025-11-11 17:34:11.474191
0367afe7-1ae7-4b8c-b8fb-2f0462456b54	/api/v1/stations	500	2025-11-11 17:13:28.380671
f751f792-5aea-4d77-9bdb-48f230aecc0e	/api/v1/rent	200	2025-11-11 17:30:37.024354
b9ae4c07-ed8d-4ddb-afb6-16c24f17c438	/api/v1/user	404	2025-11-11 17:47:13.546347
cf8b420f-68b4-4bbe-9c5a-cc37001cd3cc	/api/v1/rent	500	2025-11-11 17:28:33.619516
f4663725-ef1b-4897-8bc8-e1d74a3341b8	/api/v1/user	200	2025-11-11 17:30:42.891032
2b4bdabe-fabf-48d8-8ef7-a1ba881cf16c	/api/v1/user	200	2025-11-11 17:35:17.048633
1e1b5871-b0e8-45f8-89ff-559c3f78648f	/api/v1/rent	200	2025-11-11 17:04:37.778283
a6c2c333-61bb-4a23-963a-34db9964d734	/api/v1/stations	200	2025-11-11 17:27:13.626132
ac179d72-b57c-405b-8231-241536ca8864	/api/v1/rent	500	2025-11-11 17:43:08.784819
0426dcc3-8edb-4722-9a9e-017da57599b0	/api/v1/stations	200	2025-11-11 17:20:49.559145
3de55ade-f36a-4654-b2fa-702df105f845	/api/v1/rent	500	2025-11-11 17:28:37.420227
729f1761-ea68-4ddb-ba10-1c8e360c4937	/api/v1/user	200	2025-11-11 17:10:44.754954
8d786698-7b40-4026-ba89-78431e78c3bd	/api/v1/stations	200	2025-11-11 17:25:49.77235
1e591ab6-9155-417d-bbd3-0bcef7bef05c	/api/v1/stations	200	2025-11-11 16:55:28.57952
f0da0fcb-74aa-4866-b9ae-f252cf56c8aa	/api/v1/stations	200	2025-11-11 16:56:43.715708
b2d3bec8-7399-4703-b230-3e28fd13628e	/api/v1/stations	200	2025-11-11 17:13:32.496074
0f85e4ed-c5a9-497c-98fa-12989a2df68f	/api/v1/stations	200	2025-11-11 17:35:35.072822
df8ae404-b50e-4729-80f2-3e76308b2d75	/api/v1/user	404	2025-11-11 17:47:10.286169
ce814fd4-b3cc-4fbe-82de-6235254f0a74	/api/v1/user	200	2025-11-11 17:27:38.133641
60af3331-f592-4a2c-8280-6b592990ca2d	/api/v1/stations	200	2025-11-11 17:28:37.14505
4e623282-757f-44b8-b20e-3f2489b6581a	/api/v1/user	404	2025-11-11 17:39:47.130719
260e8522-f474-487c-bc44-c122d934f683	/api/v1/user	200	2025-11-11 17:00:59.822217
08f0eaa8-ccff-4ab6-9410-63a34fb6087d	/api/v1/user	200	2025-11-11 16:55:25.306309
66e493a5-7369-4489-a990-63882b5ff2cc	/api/v1/rent	500	2025-11-11 17:41:14.70424
e79af5e9-d001-4547-912c-e13ec445bc0a	/api/v1/user	404	2025-11-11 17:38:19.629437
f62dfc2f-d6cf-46a6-8ad1-fb040d4589ef	/api/v1/user	200	2025-11-11 17:16:21.905954
8a1dcf09-fcff-44f9-8e13-40a44cd6a0aa	/api/v1/user	404	2025-11-11 17:30:44.019015
56be385b-39f4-4535-87d0-d91d812d7133	/api/v1/user	500	2025-11-11 17:37:34.165606
58483a3a-12ce-43ef-b2d9-eee3c62cc4d0	/api/v1/user	200	2025-11-11 17:11:15.150642
171db7fd-958e-4133-9ecc-9387885df284	/api/v1/user	404	2025-11-11 17:00:30.673645
c08ef5af-cddf-4abc-9cce-e1cb847dfe40	/api/v1/user	200	2025-11-11 17:26:42.042851
8b8599cd-a429-4aa4-bbaf-de8c0e0a5d8f	/api/v1/user	500	2025-11-11 17:12:39.122103
30e2af9b-a96c-4208-85d9-5d956e18b127	/api/v1/stations	200	2025-11-11 17:43:13.455632
70446e68-c312-46ff-a4e5-2c293de2b075	/api/v1/stations	404	2025-11-11 17:23:28.255528
79dd722b-c642-45f4-acd5-99b0ad22d289	/api/v1/stations	200	2025-11-11 17:19:53.863339
f539ba95-35af-4f4c-a31f-23576f368fcd	/api/v1/user	200	2025-11-11 17:23:47.13531
41341953-51bf-4343-babe-7791621073ab	/api/v1/rent	404	2025-11-11 16:56:17.437815
b05101b4-f536-4730-aff4-6ff0afe8fb18	/api/v1/stations	200	2025-11-11 16:56:38.744998
a86b014e-6c0a-44cc-8b8a-0946b26d81a6	/api/v1/stations	200	2025-11-11 17:11:29.717796
5e57040a-9ccc-4629-a34f-86d5126cbaa9	/api/v1/user	200	2025-11-11 17:25:27.283764
6bfe08ed-8266-4c7f-843a-32ccfa5e3182	/api/v1/user	200	2025-11-11 17:42:16.800413
507fa225-689e-4049-9919-c05c1cd651cf	/api/v1/user	404	2025-11-11 17:22:01.789661
a2e63261-708e-4334-a44d-622392a71e2d	/api/v1/user	404	2025-11-11 17:41:44.555396
0fe6bf71-4e88-4f2c-9ef1-5528ee3abb92	/api/v1/rent	404	2025-11-11 17:18:26.732664
ca20826e-4ee2-46df-a8c5-775dff4d4383	/api/v1/user	200	2025-11-11 17:23:48.114906
93060426-8f19-401f-a18b-3e2408281754	/api/v1/user	404	2025-11-11 17:27:22.164511
3b92047c-121c-4dd6-8877-92bb7f260380	/api/v1/user	200	2025-11-11 17:40:08.052822
e9e7478a-2125-4412-b37a-0318bbfd3c0e	/api/v1/user	200	2025-11-11 17:13:24.618164
abfdad6f-25f2-42f7-99de-f416425bb412	/api/v1/rent	500	2025-11-11 17:49:24.597188
d4f7347c-d4fa-4154-b19a-6b472f231909	/api/v1/user	404	2025-11-11 17:30:54.476327
4b17d468-8397-4642-bdf1-ea5e1e75c9c2	/api/v1/stations	200	2025-11-11 17:39:30.735489
6c9b4966-03cc-4b8d-94fb-1085e3cf33c0	/api/v1/stations	404	2025-11-11 17:21:33.606855
6ac0d1f8-d299-4485-a130-227e0a4ad79b	/api/v1/stations	200	2025-11-11 16:55:29.850413
6695dba6-b0be-417d-8c57-3e1abc3e2250	/api/v1/user	200	2025-11-11 16:56:41.203903
46129a92-d383-46ae-a58b-7ff80fa5bc09	/api/v1/rent	200	2025-11-11 17:27:17.944434
e4160ba9-ee58-45e5-a45a-2bbc5f57e47f	/api/v1/user	500	2025-11-11 17:25:28.377111
c12d758a-b1d6-43fa-81b4-658657800f85	/api/v1/user	404	2025-11-11 17:48:18.528699
73c2ed46-7c3c-4ab8-b20d-c525b23ca859	/api/v1/stations	200	2025-11-11 16:56:08.600493
34dc2e4b-2398-4ae6-8520-0b2b4d9d2e9a	/api/v1/stations	200	2025-11-11 17:17:11.238269
acd5d2bb-9100-4c3e-b185-95e629594c72	/api/v1/user	500	2025-11-11 17:07:54.820603
7f85f8f5-320c-48df-9f26-391c4d91843e	/api/v1/rent	200	2025-11-11 17:11:08.545646
a80efd34-5ec3-47cb-bd84-2fe490663a82	/api/v1/stations	404	2025-11-11 16:57:17.789199
2e62d863-f634-40ff-b207-fbc9549588b5	/api/v1/rent	200	2025-11-11 16:58:17.458209
361358f9-8f30-46cd-82d7-81dcf1e39f33	/api/v1/rent	404	2025-11-11 17:28:36.843713
f206951d-1c8f-44e8-8f32-d629603f01c6	/api/v1/user	200	2025-11-11 17:00:29.35804
9c93513c-98a1-4645-be58-f63e09c4c895	/api/v1/stations	200	2025-11-11 17:05:40.520217
c7beb502-b092-468f-8cf3-0d5275825e48	/api/v1/rent	200	2025-11-11 17:44:34.119167
4ed7e20b-fe3c-4d37-a687-ee8a5f9dd201	/api/v1/user	404	2025-11-11 17:20:32.854491
\.


--
-- Data for Name: bike_maintenance_logs; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.bike_maintenance_logs (log_id, bike_id, report_date, issue_description, cost_eur) FROM stdin;
1	1001	2025-10-17 05:42:40.404331	Batterie faible	50.31
2	1003	2025-09-13 01:26:43.762059	Frein usé	60.19
3	1007	2025-11-03 02:19:24.997253	Batterie faible	45.87
4	1003	2025-10-10 02:02:59.278845	Batterie faible	35.58
5	1003	2025-10-08 19:23:37.821312	Batterie faible	7.77
6	1009	2025-11-08 04:56:11.031732	Batterie faible	37.74
7	1008	2025-11-09 17:17:38.276345	Chaîne rouillée	56.76
8	1008	2025-08-28 11:47:32.781196	Batterie faible	91.66
9	1007	2025-09-01 08:09:43.36112	Batterie faible	97.00
10	1007	2025-11-08 12:24:59.569079	Batterie faible	68.16
11	1004	2025-11-07 18:59:10.068479	Batterie faible	74.71
12	1007	2025-10-21 15:38:23.765775	Batterie faible	30.99
13	1003	2025-10-12 20:41:28.37888	Batterie faible	1.90
14	1006	2025-10-10 00:29:16.86003	Chaîne rouillée	24.27
15	1004	2025-08-20 22:35:05.125957	Pneu crevé	60.92
16	1007	2025-10-09 00:57:10.298643	Pneu crevé	1.89
17	1002	2025-08-15 12:17:30.31178	Chaîne rouillée	37.10
18	1005	2025-10-14 08:05:16.735658	Batterie faible	67.03
19	1009	2025-09-23 05:27:31.422042	Batterie faible	4.30
20	1001	2025-08-23 20:48:18.691405	Chaîne rouillée	92.19
21	1006	2025-09-06 08:24:05.872077	Pneu crevé	70.02
22	1005	2025-10-26 04:41:02.498606	Pneu crevé	21.63
23	1007	2025-09-12 20:25:29.735183	Batterie faible	13.89
24	1009	2025-09-10 00:15:13.205515	Frein usé	99.69
25	1002	2025-11-08 07:43:25.294956	Pneu crevé	88.11
26	1003	2025-09-18 04:43:52.558819	Batterie faible	58.97
27	1005	2025-10-26 21:41:03.098051	Pneu crevé	8.81
28	1001	2025-08-30 20:43:33.220354	Pneu crevé	19.04
29	1008	2025-11-03 22:59:30.287646	Batterie faible	48.11
30	1003	2025-09-19 23:48:22.217082	Batterie faible	24.11
31	1002	2025-08-30 18:16:08.122881	Batterie faible	42.45
32	1004	2025-10-01 00:18:00.866395	Batterie faible	93.29
33	1010	2025-09-24 23:10:59.568071	Pneu crevé	78.04
34	1002	2025-09-29 02:52:41.881808	Batterie faible	95.23
35	1008	2025-10-03 20:22:51.980291	Batterie faible	46.64
36	1007	2025-11-09 05:32:17.479702	Chaîne rouillée	87.78
37	1001	2025-11-06 12:29:40.715512	Frein usé	38.25
38	1008	2025-09-02 09:34:49.210348	Pneu crevé	80.46
39	1000	2025-08-19 11:32:00.662466	Frein usé	29.11
40	1008	2025-08-17 17:27:15.066722	Pneu crevé	74.10
41	1005	2025-08-24 02:50:07.082801	Chaîne rouillée	38.66
42	1002	2025-10-11 06:23:05.385803	Pneu crevé	71.50
43	1005	2025-11-03 02:32:15.336263	Batterie faible	26.57
44	1000	2025-09-18 13:56:18.434156	Batterie faible	89.22
45	1009	2025-08-28 14:46:03.389176	Batterie faible	49.99
46	1008	2025-10-13 21:09:05.948775	Batterie faible	28.98
47	1006	2025-08-19 05:40:37.366659	Pneu crevé	19.45
48	1006	2025-08-24 21:04:59.062761	Chaîne rouillée	90.23
49	1009	2025-09-04 20:02:32.015443	Pneu crevé	37.34
50	1001	2025-09-29 11:22:50.0814	Chaîne rouillée	7.42
51	1000	2025-08-13 18:02:17.305991	Frein usé	28.85
52	1007	2025-09-02 21:21:42.856041	Chaîne rouillée	70.51
53	1010	2025-09-08 07:53:41.958927	Pneu crevé	61.08
54	1004	2025-10-25 12:06:33.0865	Chaîne rouillée	65.16
55	1008	2025-09-25 14:51:17.078348	Chaîne rouillée	6.71
56	1003	2025-10-29 08:46:23.032345	Batterie faible	23.44
57	1007	2025-11-06 22:29:39.372753	Chaîne rouillée	92.07
58	1000	2025-10-12 15:26:26.623058	Batterie faible	34.82
59	1004	2025-09-08 13:12:04.310968	Pneu crevé	66.04
60	1002	2025-10-30 06:03:43.824829	Pneu crevé	4.43
61	1005	2025-09-12 15:25:00.643612	Chaîne rouillée	10.21
62	1002	2025-09-28 16:26:34.790483	Pneu crevé	10.82
63	1005	2025-09-03 22:39:29.352312	Batterie faible	37.05
64	1005	2025-09-05 11:24:51.632456	Batterie faible	12.89
65	1010	2025-10-17 05:11:32.68483	Pneu crevé	61.20
66	1001	2025-08-25 08:05:28.709747	Pneu crevé	74.15
67	1007	2025-09-16 03:16:54.060477	Pneu crevé	61.18
68	1008	2025-10-23 22:06:55.222448	Pneu crevé	96.68
69	1004	2025-09-27 01:42:49.082908	Pneu crevé	46.25
70	1004	2025-10-20 03:51:03.023374	Frein usé	20.45
71	1002	2025-10-14 14:25:30.943606	Frein usé	61.81
72	1004	2025-10-28 10:32:47.405984	Pneu crevé	8.83
73	1008	2025-10-19 06:59:39.73467	Frein usé	57.99
74	1003	2025-10-20 08:10:29.218004	Batterie faible	66.38
75	1005	2025-11-03 07:25:51.18343	Chaîne rouillée	34.86
76	1005	2025-08-31 04:22:17.66854	Pneu crevé	94.96
77	1010	2025-09-16 16:32:58.945263	Pneu crevé	19.95
78	1006	2025-08-13 23:49:37.381266	Batterie faible	50.30
79	1006	2025-10-24 13:41:15.818146	Pneu crevé	14.51
80	1009	2025-10-12 16:51:52.663895	Batterie faible	49.38
81	1005	2025-09-14 11:46:24.386598	Frein usé	95.28
82	1005	2025-08-21 08:20:11.007356	Pneu crevé	88.60
83	1009	2025-09-10 08:56:27.557913	Frein usé	48.91
84	1003	2025-08-25 23:43:38.716636	Pneu crevé	0.25
85	1008	2025-10-08 23:57:59.388174	Chaîne rouillée	40.10
86	1008	2025-09-29 03:37:44.119286	Frein usé	77.18
87	1002	2025-10-28 10:34:06.224305	Chaîne rouillée	22.37
88	1001	2025-08-18 04:26:20.581244	Frein usé	96.06
89	1004	2025-09-23 01:05:52.075543	Pneu crevé	45.20
90	1003	2025-09-19 15:04:05.639946	Pneu crevé	19.61
91	1000	2025-11-04 02:54:36.349696	Batterie faible	3.48
92	1004	2025-09-08 12:33:14.881925	Frein usé	40.08
93	1004	2025-10-09 02:58:48.302562	Pneu crevé	89.02
94	1008	2025-09-11 20:21:20.305293	Batterie faible	52.63
95	1002	2025-10-15 16:11:57.11973	Batterie faible	28.05
96	1007	2025-09-08 12:08:43.222138	Batterie faible	47.46
97	1008	2025-10-03 03:03:52.806079	Batterie faible	4.35
98	1003	2025-10-24 12:18:09.839074	Pneu crevé	77.74
99	1005	2025-08-28 07:28:48.23361	Batterie faible	40.32
100	1006	2025-08-22 21:21:35.035003	Batterie faible	76.13
\.


--
-- Data for Name: bike_part_orders; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.bike_part_orders (order_id, part_sku, quantity, order_date, supplier_name) FROM stdin;
1	SKU-1044	83	2025-10-23	BikeParts Inc.
2	SKU-1009	94	2025-08-18	BikeParts Inc.
3	SKU-1000	74	2025-08-26	BikeParts Inc.
4	SKU-1049	67	2025-09-27	BikeParts Inc.
5	SKU-1029	78	2025-09-29	BikeParts Inc.
6	SKU-1045	64	2025-10-03	BikeParts Inc.
7	SKU-1039	73	2025-08-19	BikeParts Inc.
8	SKU-1021	59	2025-10-18	BikeParts Inc.
9	SKU-1035	22	2025-10-24	BikeParts Inc.
10	SKU-1029	58	2025-10-16	BikeParts Inc.
11	SKU-1043	64	2025-08-08	BikeParts Inc.
12	SKU-1042	59	2025-11-08	BikeParts Inc.
13	SKU-1026	84	2025-10-16	BikeParts Inc.
14	SKU-1005	104	2025-09-05	BikeParts Inc.
15	SKU-1010	27	2025-08-19	BikeParts Inc.
16	SKU-1006	25	2025-09-09	BikeParts Inc.
17	SKU-1036	24	2025-09-17	BikeParts Inc.
18	SKU-1041	99	2025-10-06	BikeParts Inc.
19	SKU-1030	16	2025-10-16	BikeParts Inc.
20	SKU-1005	86	2025-09-12	BikeParts Inc.
21	SKU-1049	110	2025-08-31	BikeParts Inc.
22	SKU-1009	31	2025-10-01	BikeParts Inc.
23	SKU-1023	94	2025-10-04	BikeParts Inc.
24	SKU-1014	65	2025-10-26	BikeParts Inc.
25	SKU-1030	13	2025-09-19	BikeParts Inc.
26	SKU-1040	62	2025-08-14	BikeParts Inc.
27	SKU-1019	43	2025-09-28	BikeParts Inc.
28	SKU-1049	19	2025-11-10	BikeParts Inc.
29	SKU-1033	21	2025-10-28	BikeParts Inc.
30	SKU-1043	37	2025-08-20	BikeParts Inc.
31	SKU-1045	55	2025-09-08	BikeParts Inc.
32	SKU-1043	32	2025-08-09	BikeParts Inc.
33	SKU-1044	35	2025-10-03	BikeParts Inc.
34	SKU-1027	55	2025-10-04	BikeParts Inc.
35	SKU-1035	38	2025-10-21	BikeParts Inc.
36	SKU-1048	11	2025-09-02	BikeParts Inc.
37	SKU-1043	48	2025-10-31	BikeParts Inc.
38	SKU-1011	49	2025-09-14	BikeParts Inc.
39	SKU-1004	69	2025-09-03	BikeParts Inc.
40	SKU-1026	37	2025-10-25	BikeParts Inc.
41	SKU-1025	88	2025-11-10	BikeParts Inc.
42	SKU-1033	103	2025-10-10	BikeParts Inc.
43	SKU-1034	45	2025-08-26	BikeParts Inc.
44	SKU-1050	74	2025-10-20	BikeParts Inc.
45	SKU-1034	37	2025-09-07	BikeParts Inc.
46	SKU-1034	95	2025-09-30	BikeParts Inc.
47	SKU-1017	54	2025-08-19	BikeParts Inc.
48	SKU-1023	22	2025-11-02	BikeParts Inc.
49	SKU-1042	45	2025-10-30	BikeParts Inc.
50	SKU-1037	66	2025-09-23	BikeParts Inc.
51	SKU-1031	81	2025-08-19	BikeParts Inc.
52	SKU-1048	23	2025-10-14	BikeParts Inc.
53	SKU-1007	47	2025-10-08	BikeParts Inc.
54	SKU-1030	110	2025-10-23	BikeParts Inc.
55	SKU-1032	54	2025-09-18	BikeParts Inc.
56	SKU-1027	75	2025-11-07	BikeParts Inc.
57	SKU-1021	41	2025-08-21	BikeParts Inc.
58	SKU-1005	60	2025-09-04	BikeParts Inc.
59	SKU-1010	30	2025-09-26	BikeParts Inc.
60	SKU-1016	32	2025-10-05	BikeParts Inc.
61	SKU-1011	105	2025-10-20	BikeParts Inc.
62	SKU-1035	78	2025-08-15	BikeParts Inc.
63	SKU-1041	90	2025-11-02	BikeParts Inc.
64	SKU-1040	27	2025-09-10	BikeParts Inc.
65	SKU-1029	79	2025-08-23	BikeParts Inc.
66	SKU-1014	88	2025-09-19	BikeParts Inc.
67	SKU-1039	30	2025-08-19	BikeParts Inc.
68	SKU-1023	40	2025-09-03	BikeParts Inc.
69	SKU-1013	100	2025-10-28	BikeParts Inc.
70	SKU-1009	75	2025-10-08	BikeParts Inc.
71	SKU-1002	18	2025-09-17	BikeParts Inc.
72	SKU-1000	106	2025-08-08	BikeParts Inc.
73	SKU-1028	81	2025-08-19	BikeParts Inc.
74	SKU-1028	38	2025-09-01	BikeParts Inc.
75	SKU-1029	71	2025-10-12	BikeParts Inc.
76	SKU-1011	45	2025-11-06	BikeParts Inc.
77	SKU-1039	97	2025-10-04	BikeParts Inc.
78	SKU-1037	18	2025-10-04	BikeParts Inc.
79	SKU-1014	72	2025-09-25	BikeParts Inc.
80	SKU-1005	92	2025-09-01	BikeParts Inc.
81	SKU-1024	54	2025-08-13	BikeParts Inc.
82	SKU-1023	74	2025-09-28	BikeParts Inc.
83	SKU-1042	71	2025-08-07	BikeParts Inc.
84	SKU-1007	95	2025-10-24	BikeParts Inc.
85	SKU-1003	81	2025-09-23	BikeParts Inc.
86	SKU-1017	50	2025-08-29	BikeParts Inc.
87	SKU-1043	63	2025-09-19	BikeParts Inc.
88	SKU-1035	29	2025-10-05	BikeParts Inc.
89	SKU-1046	34	2025-09-10	BikeParts Inc.
90	SKU-1023	67	2025-11-08	BikeParts Inc.
91	SKU-1048	87	2025-10-23	BikeParts Inc.
92	SKU-1019	33	2025-10-16	BikeParts Inc.
93	SKU-1049	25	2025-10-31	BikeParts Inc.
94	SKU-1007	107	2025-08-23	BikeParts Inc.
95	SKU-1030	105	2025-08-24	BikeParts Inc.
96	SKU-1005	61	2025-08-09	BikeParts Inc.
97	SKU-1024	36	2025-09-12	BikeParts Inc.
98	SKU-1022	22	2025-10-10	BikeParts Inc.
99	SKU-1015	69	2025-09-23	BikeParts Inc.
100	SKU-1007	48	2025-09-07	BikeParts Inc.
\.


--
-- Data for Name: bike_rentals; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.bike_rentals (rental_id, bike_id, user_id, start_station_id, end_station_id, start_t, end_t) FROM stdin;
1	1001	a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890	sta_1001	sta_1002	2024-10-01 08:30:15	2024-10-01 08:45:22
2	1002	b2c3d4e5-f6a7-8901-b2c3-d4e5f6a78901	sta_1004	sta_1005	2024-10-01 09:10:05	2024-10-01 09:35:10
3	1005	c3d4e5f6-a7b8-9012-c3d4-e5f6a7b89012	sta_1006	sta_1007	2024-10-01 12:15:00	2024-10-01 12:45:30
4	1007	07b8c9d0-e1f2-3456-07b8-c9d0e1f23456	sta_1001	sta_1003	2024-10-02 07:45:10	2024-10-02 08:10:15
5	1004	18c9d0e1-f2a3-4567-18c9-d0e1f2a34567	sta_1008	sta_1009	2024-10-02 10:00:00	2024-10-02 10:22:00
6	1001	a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890	sta_1002	sta_1001	2024-10-02 18:05:00	2024-10-02 18:25:12
7	1002	b2c3d4e5-f6a7-8901-b2c3-d4e5f6a78901	sta_1005	sta_1004	2024-10-03 09:00:15	2024-10-03 09:28:00
8	1006	29d0e1f2-a3b4-5678-29d0-e1f2a3b45678	sta_1010	sta_1010	2024-10-03 15:30:00	2024-10-03 16:00:00
9	1003	e5f6a7b8-c9d0-1234-e5f6-a7b8c9d01234	sta_1001	sta_1001	2024-10-04 10:00:00	2024-10-04 10:00:30
10	1001	d4e5f6a7-b8c9-0123-d4e5-f6a7b8c90123	sta_1003	sta_1002	2024-10-04 11:00:00	2024-10-04 10:50:00
11	1008	f6a7b8c9-d0e1-2345-f6a7-b8c9d0e12345	sta_1004	\N	2024-10-04 13:00:00	\N
12	9999	a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890	sta_1006	sta_1006	2024-10-05 08:00:00	2024-10-05 08:30:00
13	1002	\N	sta_1008	sta_1008	2024-10-05 14:20:00	2024-10-05 14:40:00
14	1004	c3d4e5f6-a7b8-9012-c3d4-e5f6a7b89012	sta_XXXX	sta_1007	2024-10-05 16:00:00	2024-10-05 16:30:00
15	1005	b2c3d4e5-f6a7-8901-b2c3-d4e5f6a78901	sta_1005	sta_1004	06/10/2024 10:00:00	06/10/2024 10:30:00
16	1007	a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890	sta_1001	sta_1002	2024-10-01 08:30:15	2024-10-01 08:45:22
\.


--
-- Data for Name: bike_stations; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.bike_stations (station_id, station_name, latitude, longitude, capacity, city_id) FROM stdin;
sta_1001	Hôtel de Ville Paris	48.8566	2.3522	30	1
sta_1002	Louvre-Rivoli	48.8606	2.3400	25	1
sta_1003	Gare de Lyon	48.8444	2.3744	40	1
sta_1004	Place Bellecour	45.7578	4.8320	35	2
sta_1005	Vieux Lyon	45.7618	4.8256	20	2
sta_1006	Vieux-Port	43.2965	5.3700	50	3
sta_1007	Gare St-Charles	43.3030	5.3807	30	3
sta_1008	Gare Lille Flandres	50.6366	3.0694	45	4
sta_1009	\N	50.6292	3.0573	25	4
sta_1010	Capitole	43.6045	1.4442	30	5
sta_1011	Place Bellecour	45.7578	4.8320	35	2
sta_1012	Notre Dame	48.8529	2.3500	20	1
sta_1013	Station Invalide	coord_lat	coord_lon	15	1
sta_1014	Station Orpheline	48.8500	2.3500	10	99
\.


--
-- Data for Name: bikes; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.bikes (bike_id, bike_type, model_name, commissioning_date, status) FROM stdin;
1001	mecanique	CityBike V1	2022-05-10	active
1002	electrique	E-City V2	2022-06-15	active
1003	mecanique	CityBike V1	2022-05-10	in_maintenance
1004	Mecanique	CityBike V2	2023-01-20	active
1005	electrique	E-City V2	2022-06-15	active
1006	E-bike	E-City V3	2023-03-30	active
1007	mecanique	CityBike V2	2023-01-20	active
1008	electrique	E-City V3	2023-03-30	retired
1009	\N	CityBike V2	2023-01-20	active
\.


--
-- Data for Name: billing_invoices; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.billing_invoices (invoice_id, user_id, subscription_id, issue_date, amount_eur, status) FROM stdin;
inv_0d94b52c0325b6a4818496c05fd769dd	2ad23963-7cb9-448e-9ce3-dbcbcc64055b	sub_2	2025-10-21	14.91	Pending
inv_08834f0695518d989bf6957d023b5333	4a9ac214-29cb-4c67-af3b-394cc30cc395	sub_4	2025-10-10	19.52	Failed
inv_31b174485e03b58d6c5ca2d772d7c1ad	486d9f02-c027-451e-8c07-3b210066a4ee	sub_6	2025-09-15	11.36	Pending
inv_5b36c83cdef006acfb434cbd7810fefe	ad1f1b10-b21a-41a2-bc3e-5a55f8e46770	sub_2	2025-10-23	24.94	Pending
inv_f3048a209ab8080d98f33ee7fae237d9	dc008927-14cf-400d-b15d-08fca6be16c3	sub_4	2025-09-24	28.95	Pending
inv_710267eafed6f707ac1b7e037bdfba0b	826b226d-e330-4341-b0df-08d5a47601bd	sub_5	2025-09-18	34.21	Failed
inv_631208f9cb32faf5ede3b79ff8fd344b	e8683ccf-9aa5-4179-b281-eba2ddd4db93	sub_5	2025-10-31	34.67	Pending
inv_14a459fae6f82e8593eb4aafbccc7754	28af2d52-bee3-4aa3-9d2d-2320661e66ac	sub_6	2025-10-18	37.71	Failed
inv_104f9c0b77cb37cbcbacf8deae2a31f8	be7117a2-3aa1-4995-a913-041da2f72710	sub_4	2025-09-26	27.45	Paid
inv_1cd24a20235e27825bbd29b028f6f968	5f419b7f-30e5-4193-b7b3-a041b71a1b9e	sub_3	2025-10-18	37.12	Pending
inv_5d918f695a2978b047c24434e3b2f7e0	e94bd557-9639-4ef6-a09f-2bd71c208438	sub_5	2025-09-20	30.92	Pending
inv_dfe864ab87ea2a4d4394fa139c363f88	8fe8a1ce-fe82-4720-a15d-9872d4be4625	sub_4	2025-11-06	31.30	Failed
inv_447b6d9d61f6db978abd9a091c9abfb8	532c9012-c30d-48e9-87d3-2bf09c386ef9	sub_2	2025-11-09	30.86	Pending
inv_0c2f85a147095d12e9332b3089a9f610	8e243156-4423-443a-b44c-40832c45ed07	sub_6	2025-09-17	28.96	Failed
inv_4f4f7d808e5e68168e1f92dfebbcfca7	0680ab1b-9925-422c-b0b4-d61f2ae83b51	sub_6	2025-10-24	37.18	Pending
inv_d76778bc4518c822379ab3ebbf5af9d7	b1809101-aece-4693-8129-eaf3aa891515	sub_5	2025-10-03	39.48	Failed
inv_a1359ea917571f6cc4f654297c1f4e34	93a37c49-03e4-4fe5-866d-8dcd6b0088f5	sub_2	2025-10-26	11.50	Paid
inv_c52cd3c2b9d2d5a05673e935da931028	082b014c-7775-4c32-98bb-13766e7a089c	sub_5	2025-10-28	14.98	Pending
inv_d236a0dc001b6127e0bc791dc054efda	2c460003-273f-4869-abe2-45a9822e54e7	sub_3	2025-10-25	28.44	Paid
inv_6a7958d464186e3575aa353b558f8abd	a7300a95-0b13-43f9-b228-b0ee220630b2	sub_3	2025-10-26	12.81	Paid
inv_4289e5f8ea918129addbb67ca3baf89c	ca80cd8e-60e6-415c-8299-6568cc4c9482	sub_5	2025-11-06	35.25	Paid
inv_59c9a214a6d23281aa673b523f2e54b3	36d63e04-a8e2-4ce0-81c8-9f2a650986cc	sub_4	2025-09-20	33.21	Pending
inv_60b3506455951769ea3648f1bdb09b7b	f5e0ee49-ddaa-4c14-a533-f1a0e05534af	sub_3	2025-10-21	20.23	Failed
inv_c72bec326322f871e60cf3669100d015	948f8883-cd87-44ab-9df6-b4582804d881	sub_6	2025-11-10	34.51	Failed
inv_8ceaa548ac3e74db261ef7cb0835263e	f3c722c1-e958-4715-aabf-7c02113e3920	sub_5	2025-10-13	22.52	Failed
inv_223111420284d5f3a16cd9ddb79628a4	92702a2a-15ec-4938-a64a-870398fe88f4	sub_3	2025-11-08	17.47	Pending
inv_df991671cdfd0b0b23462b7b4f6676ca	0022b00e-af33-4d58-8367-5df7af8f9569	sub_4	2025-09-24	39.56	Failed
inv_40251dc898938a171364411dc01d6ef0	37213769-9d5d-42ea-beff-0cef14ed7203	sub_6	2025-10-26	33.14	Pending
inv_9aa999e02e4569a0b59d19514f3b7164	3e5d856d-4559-427c-9cc8-deedeb036de1	sub_3	2025-10-27	18.97	Pending
inv_253c3e0ec23f46920e5e12d924fdad68	c3efbe06-fab6-4db1-b947-72e8efdb93e0	sub_4	2025-11-02	35.51	Failed
inv_be1a1c36ddf4857f6c59ea538214c145	a1c4e0da-22f9-4661-9f49-d35339af2f20	sub_4	2025-10-31	17.35	Paid
inv_2983e35578d0257360a8ad9394bee450	0f7d690f-de19-4559-a520-2cf65485a767	sub_6	2025-11-07	22.69	Failed
inv_ed3fa31f571087b13dc3e32a0bceccd8	0e3bc1b0-059c-4689-a8be-e09cf4fc524b	sub_6	2025-10-20	30.46	Pending
inv_6ca3593f3fbfa9ca66d74a2f727007d5	abdde4aa-b947-4dd6-8652-4ad633a6353d	sub_5	2025-10-09	31.08	Paid
inv_76762679b3b0d759860703a6f43306f1	cb27059d-e1e4-4a50-b1d2-9daccd4e330c	sub_1	2025-10-09	19.32	Pending
inv_9d5b2623e001ce6e996cfe00e6006524	8fa6a51a-2e66-4165-8ac7-b70ecf8f5201	sub_3	2025-09-30	10.40	Pending
inv_0626df5e21c9f0bb5a86ace64cf02c05	c12020f5-99d1-4196-89ae-51612e3d2809	sub_4	2025-11-05	38.52	Failed
inv_af1fb4947be9e5c3378343767fc1d58d	8938a616-66fb-46f7-8be9-72593c1cb615	sub_2	2025-10-11	19.58	Paid
inv_a1ef2923f10134e9a8db19dd6d3709ea	698feae7-023b-4fd8-a95a-c316bd4cce77	sub_3	2025-10-15	13.04	Paid
inv_e4ba502935fec065a041d8f1b8fc1bf1	db90a662-765d-4224-ab8e-bee13cb1dc9d	sub_5	2025-09-23	28.30	Pending
inv_fb5fd311b9d67650bf68712525cfa444	813cfa42-95ca-4e62-b7ec-01884617c04b	sub_2	2025-10-14	21.34	Failed
inv_9e91a14770aabc9af7cd3a0c896d0120	4c725c96-644f-4c43-bb7a-fae114ea3c51	sub_2	2025-11-01	21.56	Failed
inv_074197744762397c87abd6a0676f811e	397a458e-2969-445b-bfea-608c3c92fa05	sub_4	2025-11-01	21.31	Pending
inv_26972c82b80df3d8c3167873499d2d31	235c3e89-d734-4be1-8560-fe48ddfd615b	sub_5	2025-11-05	37.75	Pending
inv_92499d2ef0e8e484abe45c917c0e99fb	2dea4677-7887-4cab-8c0a-c069282e18ef	sub_3	2025-10-28	33.86	Failed
inv_149b4813b52ee7e234a39843f0e31130	712a39e3-a8e8-4d71-9b0a-7d894075e0e0	sub_2	2025-10-26	24.70	Failed
inv_a22b9923b332425b1ab5162bdffa9307	cf460684-4d1d-42e4-81f1-5ddb60ca5b4b	sub_2	2025-09-14	24.99	Failed
inv_96bcf182cdc64077a511284b08e8b3c3	08bb06ef-de4f-4690-9b81-7c10dc6d1562	sub_3	2025-10-16	36.20	Paid
inv_d69ff51b1685173c5a24c64baa6ae4e8	2058994e-098d-48b2-bacc-35cc6a3e6c0d	sub_2	2025-10-31	17.77	Pending
inv_ae74292388ee6239eefc046c106f3927	8f28bdea-8676-41f4-b06b-1cc10d2f6348	sub_4	2025-10-16	25.74	Failed
inv_67b51cc06c2c302436188397c64d6e8b	6ca454c7-79a5-411a-b7af-8b592c0cacd9	sub_5	2025-09-19	31.16	Paid
inv_2a6ec0b90c977d41fb25ba251f70a6b8	e32c8988-415d-4c20-8fd1-7d6301af36fd	sub_5	2025-10-30	19.78	Failed
inv_864dd9753585c3917f675454926bf442	3a29e6a6-7421-413d-b577-6231bfb9222b	sub_2	2025-10-01	15.36	Pending
inv_e8a3f7d7a7d32bcfee22d31b96da5c3a	0e458d13-d442-47e7-85a3-82241759e915	sub_5	2025-11-01	14.30	Failed
inv_47981adf7b246e46ed8fe03574de2f7a	b30fc2bf-2f12-4d8c-866a-4dd941c803f3	sub_4	2025-10-11	11.04	Pending
inv_7e892e125b55291ca3e0059019416248	348a6f91-15ae-4e9c-957c-62ddba7254ec	sub_4	2025-10-02	36.42	Pending
inv_7a2c42d2d606eae4b1dde1788291c0b5	7170879b-5476-478e-97ee-c74c9d8e1fc3	sub_5	2025-10-25	26.86	Failed
inv_692317ea0c0c8f170aec0f311c22755c	ef40a113-9e79-4432-a2cd-a2795c7e43db	sub_2	2025-09-15	27.61	Failed
inv_4c01f37719adfb5645e82a250481ee49	05ae9b35-e524-4c63-8fe4-a4005a7da7e1	sub_2	2025-10-10	10.92	Paid
inv_72d9c1135cc7d26d09544b18879231ee	6bd13b2f-bcf2-4c0c-ac7b-d742162fa3e7	sub_5	2025-10-25	25.88	Pending
inv_a7ba68518275161e17b0bdec54457c11	08cd31ff-bb20-4dee-bc8f-e6d4e0bbd829	sub_3	2025-10-27	28.48	Pending
inv_9593bde6f3f12ea4f690ce51c4205d5c	efd99ba2-b363-45ef-affd-017bafa115c5	sub_3	2025-09-22	31.59	Pending
inv_c03c74af186d62c6d2c9280c50e82a94	7f4e0e25-e84a-440e-b55c-fb025997a8ff	sub_4	2025-10-05	14.39	Pending
inv_3bf330c75cc0597f3c7ab675520d3dc4	4f50026f-cce9-4ce5-90f4-baab74617124	sub_2	2025-10-11	37.38	Pending
inv_8af2feaaad19cde96faff4fb33cff461	b15e1ddf-f5ca-46da-8060-13d2a48f5e79	sub_3	2025-10-09	20.70	Failed
inv_3c110589687d8a2b9a774947e972a99b	dff76a27-68be-4610-b84d-f6c22c6f8acf	sub_3	2025-09-23	20.41	Failed
inv_e09191d58d3f674b0e26e93f051614bf	9d0eae10-f0dc-42e4-b04a-5e1073f8be99	sub_3	2025-10-07	11.44	Pending
inv_39bfa5baf1b01a7229232ed2c570d24f	bb107e38-07a0-4359-96e9-f9d53a7842a3	sub_2	2025-09-21	27.84	Failed
inv_b7fb88e8ad397fe9c3454ee7729165c6	bc69e499-b128-4370-826b-daa5f2bf86fe	sub_4	2025-10-09	24.39	Paid
inv_6b8fe81eec3ea02082561153de0718b9	8de02e22-fc94-451a-8fd5-74113d9500ce	sub_4	2025-10-31	25.13	Paid
inv_45e388b85760161440c8c3a596b20055	07822c16-9bf4-4e42-bdfb-b342ceef4e7e	sub_6	2025-10-27	34.87	Pending
inv_26d08e7a50d13f465bf2f6636b3db607	04b33a91-386b-48c8-b7d8-6ea7d4a9c09b	sub_4	2025-10-12	11.00	Pending
inv_c68786027897da932de1ed4598d7903e	515dea39-e0ba-4ce5-ac1d-b008a2b9fef8	sub_6	2025-11-10	33.99	Pending
inv_434ff820fa0489becf62a8863467c039	fbd166dc-32ff-44f1-89c1-4bc310fed96a	sub_1	2025-10-21	37.49	Failed
inv_e7cb3b1e5a9d343328cc206d83dbafb8	21da00ec-f49c-464d-80a1-c1b52a82bcfc	sub_2	2025-09-27	15.35	Paid
inv_63a7197c25de26255a97b74766c78507	d1b9d26a-eab3-4fa0-a010-7d34ce94e155	sub_4	2025-10-30	23.61	Pending
inv_dd1ad6fa7787dba51b4c836232e01121	85182051-4199-484a-b24f-205569e67ca8	sub_5	2025-09-26	23.16	Pending
inv_05298cdadb9df799d7a11112d6481b6d	1dcdb018-8b68-4e6f-909a-76590e9feef1	sub_3	2025-10-10	36.44	Pending
inv_cc9035acf56433847ca9e7336606e07c	534c715c-bb24-4d0d-b381-d8980e31e757	sub_4	2025-10-29	16.80	Pending
inv_b101893c5fac9a64607d89fda96f7fee	6295c5fd-4880-449c-852b-dc205858cc1d	sub_5	2025-10-24	11.22	Failed
inv_d15c2da47c5fd0135dc1ffc2c9447971	00390afa-544a-4308-a628-47398b872921	sub_1	2025-11-03	29.35	Failed
inv_cd6770f354d756041e2877fa622c4855	80d6653b-a801-4fa7-b593-bdaba976abd0	sub_1	2025-10-18	29.87	Pending
inv_ccbdab4b0c469d7cd064bce71df657e0	0882c70f-2093-4256-8756-cfc18572d7b6	sub_3	2025-10-19	19.40	Pending
inv_fc74c856711dd13dc4458b5748d67916	cedfcf8a-a37b-463e-9307-cbd78c7b053a	sub_3	2025-10-14	16.73	Failed
inv_9ce90a785c66af759748125b87700513	ebb02553-f081-4a46-a52f-973616065367	sub_5	2025-10-28	17.06	Paid
inv_fed52d2aab46d3700bb5f4880b22ca22	28928770-9d55-4963-ac1e-90227f31dc26	sub_4	2025-09-21	19.74	Failed
inv_bc82c9b15ea38979fc9f864e98164cbe	723cef70-beee-4011-94b8-28f2c76ccf64	sub_4	2025-11-05	30.05	Paid
inv_199b6a08af6a086f274522abbab6dc31	c2803173-4be6-430b-97d4-a9bd899fd04c	sub_2	2025-11-01	34.65	Pending
inv_5d5a326576fb0019636561fde2ffc0a9	e0e64e4c-cda7-47f5-be21-bfd3f0f0f534	sub_4	2025-10-02	12.49	Paid
inv_e7e80ea8f3461f5bba7f60cf93341e15	ab5e8c7a-a7aa-4973-b2b9-45f643d77cd3	sub_1	2025-11-11	36.03	Pending
inv_90f694bf34f4d0b4583cf8a02bf2810b	2b36a5a2-09b3-4bb1-aaae-87b420632caa	sub_4	2025-09-17	13.41	Failed
inv_c43a246742c6df31480498d48dd91635	ee958dba-7f68-4c9e-bd9c-8312f0e91543	sub_4	2025-09-30	18.51	Failed
inv_966778f647761e6678fe10b6a634cdb9	aead3ad9-ad86-4c72-94f1-7fe5d1d2a8bb	sub_6	2025-09-28	24.32	Pending
inv_40e5f90f1a726c709ca33f1ecf24cc90	1e979962-2ac3-4763-8ba5-19f6c1d66e7b	sub_3	2025-09-25	21.56	Paid
inv_a75ebdc4a45a808fe45f4260c6d2d50c	cbab4d54-7479-45d1-baa0-d0673e988fa0	sub_6	2025-09-13	13.21	Pending
inv_ebba7eb71e41b11b9b8ecade80f966df	e3f956d9-e28c-4415-b9f1-3d6e784fd9ae	sub_6	2025-10-03	28.99	Pending
inv_252a19f89099a093bda7e94ca90f6449	488338a2-a467-4b73-a136-861ae8d447a3	sub_2	2025-11-09	37.27	Pending
inv_92825d40dc587fcfdc65ac9d796174b2	2843311d-3766-4c74-97be-bc721ea5efca	sub_4	2025-10-20	33.38	Pending
inv_1b672ce120085ac50c092e482bbb7f22	49cae4c7-8bb9-42be-bcbb-8208766ca6c9	sub_2	2025-09-27	39.75	Paid
inv_df7ede19c10854d9359c4ee82e6dbbfe	b1392939-aaa1-41d0-a707-04ff7b1b56c9	sub_3	2025-10-06	12.42	Pending
\.


--
-- Data for Name: cities; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.cities (city_id, city_name, region) FROM stdin;
1	Paris	Île-de-France
2	Lyon	Auvergne-Rhône-Alpes
3	Marseille	Provence-Alpes-Côte d'Azur
4	Lille	Hauts-de-France
5	Toulouse	Occitanie
\.


--
-- Data for Name: customer_support_tickets; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.customer_support_tickets (ticket_id, user_id, created_at, subject, status) FROM stdin;
1	b202a8db-8b57-420c-81e8-807c65eb1655	2025-10-31 09:18:04.967676	Autre	Closed
2	399a0e4f-d7b0-4033-acae-613747c369cd	2025-11-05 07:45:35.523655	Autre	Closed
3	2591a88a-e0fe-44d9-ab0b-c36a5221fc35	2025-10-30 22:26:30.516653	Problème paiement	Pending
4	dc126316-7214-4b68-8026-d13ab456aae3	2025-11-06 10:44:58.826292	Abonnement	Pending
5	558f1a69-6e3a-442d-a549-facc1c8ece95	2025-11-11 15:10:25.944536	Vélo non déverrouillé	Pending
6	db6b898a-0afa-4802-907a-7af0a60213c8	2025-11-06 01:19:07.963645	Abonnement	Pending
7	62cc0a76-4619-4fd3-b2f0-aa0ea6e6f1e1	2025-11-07 03:37:57.96953	Autre	Open
8	36fded37-79bc-4045-825b-11e2702ffa81	2025-10-31 12:39:36.084105	Problème paiement	Open
9	542ed6d4-b62d-4720-b387-8e155b249da8	2025-11-08 21:00:46.009007	Abonnement	Pending
10	68dd7807-a634-487c-8993-061c15fc9eb6	2025-11-03 05:17:14.935394	Abonnement	Pending
11	c9fefec2-3711-4af8-8de3-b2765de169ae	2025-11-09 00:21:30.755935	Problème paiement	Open
12	d6899aae-9a70-48cc-b350-d4e62d213243	2025-10-31 16:30:02.507352	Autre	Closed
13	21a5e3d8-0eac-4967-ad9f-864704e27e9c	2025-10-31 01:10:43.667601	Vélo non déverrouillé	Closed
14	f95b83cb-b450-49dc-9773-b48b43e8ec27	2025-11-07 11:12:09.445028	Vélo non déverrouillé	Pending
15	372ae2ac-b9ca-4a20-97fc-07b83cf992a8	2025-11-03 05:25:57.581441	Vélo non déverrouillé	Open
16	20c1de99-7d1b-4fd1-af05-a46f0911d8a3	2025-10-29 23:21:10.769821	Vélo non déverrouillé	Closed
17	d5d4afaf-0d34-4efa-b427-c323d4bc7ee8	2025-11-09 21:25:20.16742	Problème paiement	Closed
18	ac5f3fe2-54cb-41eb-b478-59f5ebe21341	2025-11-05 21:45:05.356904	Abonnement	Closed
19	14a5e324-f2a1-4f2e-9828-6bfe53f35eae	2025-10-28 23:47:40.235049	Vélo non déverrouillé	Closed
20	a80a17d4-eba4-4ebf-96ed-179e4d964ea7	2025-11-05 22:12:40.611629	Problème paiement	Pending
21	b6ef40c7-7926-4cac-8fa0-74176bfc19e4	2025-11-11 08:10:10.001172	Problème paiement	Pending
22	69874774-02a8-49ab-8cd1-cf0664472a17	2025-10-31 13:02:05.464482	Vélo non déverrouillé	Closed
23	2820b689-11b7-415e-8d8c-fbe62d6a1b26	2025-11-06 13:39:52.046338	Vélo non déverrouillé	Pending
24	13760b45-86bf-4a84-83ec-e06b453a94ab	2025-11-07 06:18:40.31891	Abonnement	Open
25	ade2d283-edcc-4777-9e8d-d62b5ec748aa	2025-10-31 15:15:15.544007	Vélo non déverrouillé	Open
26	9072c8f6-11f2-4027-a28b-79b02959ba55	2025-11-11 00:22:56.002427	Problème paiement	Open
27	52ccba14-5b1c-4c4c-bb39-f12cae8f877c	2025-11-09 20:02:12.757799	Vélo non déverrouillé	Open
28	ff86d4d2-0def-4040-9789-7f1444aa2069	2025-11-10 15:04:28.679622	Vélo non déverrouillé	Pending
29	eba3d527-2422-4e2a-9866-56bb90752ff8	2025-11-07 20:53:12.229668	Vélo non déverrouillé	Open
30	7e72af81-9445-4c8e-8e16-b96607ccb5a6	2025-11-11 05:21:21.818187	Abonnement	Closed
31	76e5f10b-8fbc-4735-af66-8fae2b05bf00	2025-11-08 02:59:42.102678	Abonnement	Open
32	07699835-f243-4e39-a394-83259ba8ff3f	2025-11-07 21:03:34.949202	Abonnement	Open
33	7029192c-e91d-44ac-aa79-c2d84ef8f0a9	2025-11-04 20:03:51.351764	Abonnement	Open
34	a8f5f502-fdfa-4f4a-acca-798857f62f9f	2025-10-29 15:45:23.327889	Autre	Closed
35	59ffd58f-3e0a-4903-9a62-22037f91b403	2025-10-31 04:03:50.415022	Vélo non déverrouillé	Closed
36	b0a9d98e-e5c7-434a-b193-2d8fafdf0899	2025-10-29 09:27:11.180966	Vélo non déverrouillé	Closed
37	653cda27-e21f-4da0-8453-3ac31e47a189	2025-11-07 00:48:34.531821	Abonnement	Closed
38	18d702e3-150d-4e20-be7b-769ea120ece7	2025-11-07 09:44:23.817756	Autre	Closed
39	13ab246f-13dc-4dfe-b619-8b4cbb297f29	2025-10-29 02:54:46.500363	Abonnement	Closed
40	7bc2e726-e0bc-475e-b0ce-abf2f4caa7b4	2025-11-06 17:29:32.933785	Vélo non déverrouillé	Open
41	9b107150-2e23-48b8-a23d-25d6082bc5e5	2025-11-05 01:45:06.608469	Vélo non déverrouillé	Open
42	ec8c5f07-541c-4189-840f-045bbb7371c7	2025-11-11 01:47:31.174405	Vélo non déverrouillé	Closed
43	88bff37d-3b02-4fec-9382-4de2fbd0b39a	2025-10-31 13:16:01.120361	Vélo non déverrouillé	Closed
44	317f8924-f248-45ef-ad38-391143d3021e	2025-10-30 18:21:09.174031	Autre	Closed
45	9e38c719-781f-4412-a9fe-f166c633c087	2025-11-08 05:38:49.107562	Abonnement	Closed
46	69e20dcf-7492-43b0-944e-7b8bc4253ec5	2025-11-11 05:06:12.817297	Vélo non déverrouillé	Closed
47	32da2ed8-207f-474d-a055-d76f204239c7	2025-11-06 00:29:02.427807	Vélo non déverrouillé	Closed
48	78d03776-6525-4673-808e-1d0c25791360	2025-11-04 04:48:24.426693	Abonnement	Closed
49	cfc6d870-07f1-4e28-b9f0-b600d90b9848	2025-11-05 18:33:03.796228	Autre	Pending
50	a21108bb-a32a-4c2c-af01-a3223171c347	2025-11-10 06:18:05.559751	Autre	Open
51	89541c9a-fc18-47f4-ac00-ff72df64c5ba	2025-11-05 07:47:36.621446	Problème paiement	Open
52	d7ef7a8e-ce5a-4988-b854-f51e71bd35a7	2025-11-10 06:36:27.421965	Abonnement	Open
53	389cc877-9665-4edc-9090-edbd15f2a9a3	2025-11-10 20:10:29.406888	Abonnement	Pending
54	e7ef6ff7-b1c7-4932-9539-c3f83fee0fe3	2025-11-10 14:32:34.498324	Abonnement	Closed
55	a1e45c38-ab2c-4e21-b2a9-2c5217193b72	2025-11-10 21:43:06.974928	Abonnement	Open
56	3efb46d8-1949-4d80-82f2-ed891e755d5e	2025-11-05 14:42:18.952442	Autre	Closed
57	e81d2228-ed25-4f67-adb0-b48061c837cd	2025-11-04 05:09:47.859981	Autre	Open
58	e1c3af11-1729-4935-bbbf-4e1db82efb2e	2025-11-09 19:22:59.103727	Vélo non déverrouillé	Pending
59	55379800-0262-46f9-9795-66eb898d6d60	2025-10-29 17:48:40.598859	Problème paiement	Open
60	46e3b070-7a8c-40de-ae1e-b1ab0f8db7df	2025-10-28 18:32:03.127542	Abonnement	Closed
61	a715fd82-941c-49fa-8a1b-2e3ac3de5d0c	2025-11-10 21:57:35.170731	Abonnement	Pending
62	b81f199a-dc63-4b88-a5f1-8e2775c323bb	2025-10-29 10:23:24.521661	Abonnement	Pending
63	23a8a230-ea86-4669-ba9d-b322b15de10d	2025-11-02 19:00:13.403712	Abonnement	Pending
64	c58adcfa-b713-4a24-82bf-9bac80c37429	2025-11-03 09:17:25.821033	Abonnement	Open
65	eabcf017-d2a7-43bf-99cf-31b472432fdf	2025-11-01 08:48:49.103441	Autre	Closed
66	f743d2f0-1556-44af-be25-7d3fd808ee97	2025-11-04 23:06:08.477688	Abonnement	Pending
67	36074999-1c06-4224-b354-12e36f4da595	2025-10-30 12:02:59.866898	Vélo non déverrouillé	Closed
68	615ca454-e49d-4d63-8543-8636812ee040	2025-11-08 10:42:10.106467	Vélo non déverrouillé	Closed
69	436e3c06-6cd4-4059-9905-3592a8eb501e	2025-11-01 15:00:25.088513	Autre	Closed
70	c9d4936e-873d-4d1d-98eb-52f971e945ae	2025-10-31 09:15:02.850275	Abonnement	Pending
71	f4693986-77cf-4a15-a04f-c5219396e828	2025-11-04 11:57:59.273929	Vélo non déverrouillé	Open
72	a93d6b2e-18de-4db8-b9ad-f0e17333e54a	2025-10-30 19:03:38.583423	Vélo non déverrouillé	Open
73	4d42de84-7f0f-49d1-90e9-a050600729cb	2025-11-06 04:32:21.885538	Abonnement	Open
74	dfbf55e2-8d1f-4198-9dd4-91c45d88f61f	2025-11-02 12:40:34.227906	Vélo non déverrouillé	Open
75	972336d9-54b8-47c5-824a-e607afb2bc79	2025-11-11 10:41:23.760414	Abonnement	Open
76	09da4dba-885b-4420-9d92-dc6d3145137b	2025-11-09 03:28:47.418719	Abonnement	Closed
77	8da1a851-ca77-47e7-a7a3-e6a16f977b18	2025-11-11 02:46:44.054582	Vélo non déverrouillé	Open
78	294e0a13-6b9e-42a7-8af1-aad7733f9a57	2025-11-02 21:01:03.11937	Problème paiement	Closed
79	bb6f5481-94fb-4bf8-9cd0-63018f90fce3	2025-11-09 19:52:27.144891	Abonnement	Closed
80	7465e7cc-76dc-408f-9834-4a42e2d06e82	2025-11-03 03:18:54.84339	Problème paiement	Closed
81	0e982a9c-b295-42fe-a348-eb27223354f4	2025-11-10 04:36:04.350492	Problème paiement	Pending
82	7b2d268d-571a-4bf8-bb80-29e0c8640441	2025-11-01 10:09:18.522347	Vélo non déverrouillé	Closed
83	8b9d4fc7-ccca-4afc-95e6-d33a5cebc194	2025-11-03 09:46:29.205361	Abonnement	Open
84	96de8c9f-3ed7-45c1-aa0a-3c85f6bc413f	2025-11-08 07:50:39.055362	Problème paiement	Closed
85	b67cd171-1cfe-4dec-8f25-6be47f9400e4	2025-10-29 05:02:02.551353	Vélo non déverrouillé	Pending
86	0dd5dbc9-ccbe-40ed-9537-def159724c1f	2025-11-07 00:21:14.036607	Autre	Closed
87	44237ddd-8f49-463d-9dbd-79a2318cce1e	2025-11-08 18:33:30.437771	Autre	Closed
88	da684c86-51e9-40bd-bcc6-b38fa6eeab2c	2025-11-08 22:41:30.082195	Vélo non déverrouillé	Closed
89	24e35612-24c5-42b1-b6b3-a361c781e8b8	2025-11-09 03:27:38.415908	Autre	Open
90	69b7aa65-f8eb-4dd1-b84f-4558e4004daa	2025-10-29 14:18:20.162667	Autre	Closed
91	38873b6b-6715-4662-83f3-82a46cc67d5c	2025-11-08 20:20:23.665007	Autre	Closed
92	8f138764-9102-49c4-9cc7-87eb7c2c9598	2025-11-10 04:30:57.359737	Problème paiement	Pending
93	60bf16ce-58dd-4e5f-99c6-5f9dc3920a09	2025-11-06 15:46:10.390635	Problème paiement	Pending
94	373622f5-3f24-4175-bff3-58301b903c0a	2025-10-29 07:55:58.218951	Abonnement	Open
95	2c43ba64-5aef-42d9-9610-96161ee61a9b	2025-10-30 11:15:55.484099	Abonnement	Closed
96	79cff30c-844e-428b-b5b1-607eaedb58da	2025-11-04 14:39:17.911435	Problème paiement	Closed
97	5f93683c-a22e-41b4-a1e2-a57160d8e1ed	2025-11-01 12:39:43.780302	Abonnement	Closed
98	61a650bc-9210-42b5-9e8e-31028d3fb019	2025-11-09 14:09:50.217478	Abonnement	Closed
99	69a86739-7aec-4aaf-8b1a-0ad714182f74	2025-11-08 21:05:51.725089	Autre	Pending
100	abe2e561-83a9-4282-900f-774e2a0eeb21	2025-11-04 21:35:34.593294	Abonnement	Pending
\.


--
-- Data for Name: daily_activity_summary_old; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.daily_activity_summary_old (summary_date, total_rentals, total_revenue_eur) FROM stdin;
2023-01-02	262	1693.12
2023-01-03	593	1601.71
2023-01-04	263	2188.31
2023-01-05	371	1344.51
2023-01-06	495	782.75
2023-01-07	135	537.17
2023-01-08	100	1967.61
2023-01-09	339	663.32
2023-01-10	209	1682.08
2023-01-11	537	1202.45
2023-01-12	522	1134.75
2023-01-13	583	1022.30
2023-01-14	533	1476.84
2023-01-15	380	2214.41
2023-01-16	179	2373.18
2023-01-17	102	1139.62
2023-01-18	547	1186.30
2023-01-19	464	2335.97
2023-01-20	322	2306.49
2023-01-21	159	565.02
2023-01-22	412	1670.99
2023-01-23	275	559.24
2023-01-24	521	1004.07
2023-01-25	202	1519.72
2023-01-26	128	1845.26
2023-01-27	358	823.13
2023-01-28	353	2214.67
2023-01-29	214	2185.22
2023-01-30	595	1643.14
2023-01-31	317	2153.58
2023-02-01	456	1120.82
2023-02-02	360	2364.01
2023-02-03	145	2408.20
2023-02-04	291	1535.68
2023-02-05	171	2228.87
2023-02-06	398	2243.30
2023-02-07	258	2454.86
2023-02-08	167	2054.04
2023-02-09	537	1106.32
2023-02-10	184	2427.60
2023-02-11	403	695.85
2023-02-12	572	2223.97
2023-02-13	383	643.97
2023-02-14	162	2383.26
2023-02-15	321	1882.31
2023-02-16	277	1998.43
2023-02-17	405	2381.35
2023-02-18	207	775.15
2023-02-19	454	1191.17
2023-02-20	491	1602.93
2023-02-21	144	2133.95
2023-02-22	514	2353.08
2023-02-23	312	1435.32
2023-02-24	185	1085.20
2023-02-25	584	818.53
2023-02-26	571	1240.44
2023-02-27	142	2399.76
2023-02-28	513	1872.71
2023-03-01	270	1618.28
2023-03-02	313	2301.18
2023-03-03	502	2445.13
2023-03-04	114	674.61
2023-03-05	114	1458.18
2023-03-06	321	1724.06
2023-03-07	369	1600.93
2023-03-08	110	993.04
2023-03-09	252	500.84
2023-03-10	288	1500.55
2023-03-11	599	711.95
2023-03-12	388	523.88
2023-03-13	256	2441.04
2023-03-14	295	2067.35
2023-03-15	243	931.11
2023-03-16	435	692.69
2023-03-17	361	996.63
2023-03-18	342	2424.79
2023-03-19	383	2253.12
2023-03-20	162	2241.24
2023-03-21	363	2347.52
2023-03-22	180	552.53
2023-03-23	105	801.23
2023-03-24	277	827.03
2023-03-25	520	2220.26
2023-03-26	363	1381.07
2023-03-27	562	2326.38
2023-03-28	395	2260.82
2023-03-29	320	1541.58
2023-03-30	374	683.93
2023-03-31	262	508.60
2023-04-01	457	2118.67
2023-04-02	552	1798.28
2023-04-03	553	1600.42
2023-04-04	436	918.50
2023-04-05	406	1811.37
2023-04-06	256	2088.89
2023-04-07	406	677.26
2023-04-08	146	2395.74
2023-04-09	395	2314.79
2023-04-10	450	865.65
2023-04-11	225	1402.52
\.


--
-- Data for Name: email_sends_log; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.email_sends_log (email_log_id, user_id, campaign_id, sent_at, status) FROM stdin;
1	89b963ef-e5bd-402c-8e9f-a5fd541fb111	CAMP_123	2025-11-09 22:34:34.634561	Sent
2	b273f93c-4ced-4409-85db-2ec553cdcbec	CAMP_149	2025-11-10 19:57:13.581986	Opened
3	c347a270-37cc-4d43-a166-d17beb698cd8	CAMP_120	2025-11-11 01:07:31.954667	Sent
4	ffc42285-5f5a-4093-a87d-f334981f60f1	CAMP_158	2025-11-10 06:47:52.902827	Opened
5	34cf7a10-814e-464c-b6a5-98d9073717aa	CAMP_188	2025-11-10 19:26:36.404919	Opened
6	20c0b884-1272-4598-8531-b5036f7e761b	CAMP_168	2025-11-10 21:43:19.229086	Opened
7	ca830e0a-395b-4b8d-ab63-158491b792db	CAMP_159	2025-11-11 14:56:24.527164	Sent
8	40ee8608-d3b0-4fd9-a0bb-5b4a3a6352cb	CAMP_102	2025-11-09 19:31:38.461077	Opened
9	655c5966-369c-4057-b588-d0114080be8e	CAMP_179	2025-11-10 22:24:57.975863	Sent
10	127a4ca6-927e-48d3-b577-1822c6d5d2ca	CAMP_104	2025-11-10 18:24:15.596625	Clicked
11	a8cae3a0-ae79-4650-b5fb-8cc9c2b70cb9	CAMP_111	2025-11-11 08:58:24.305358	Opened
12	332b6cd4-a9af-4f94-a9a9-882e647cf854	CAMP_167	2025-11-09 21:56:55.838799	Sent
13	dd901fae-764e-40b6-957e-38cd35647cb6	CAMP_158	2025-11-10 01:34:07.806711	Opened
14	c5c1e4ab-39a9-4bfb-abb1-9230d3c677b1	CAMP_186	2025-11-11 13:20:48.788119	Opened
15	857387f5-c6e9-4be1-9b9c-d53cc25f0a1a	CAMP_192	2025-11-10 23:10:32.268468	Opened
16	4cf6b624-5510-4611-96ee-c4915f05f82f	CAMP_145	2025-11-11 06:22:25.699325	Opened
17	e5ab61ff-588d-4972-8c29-34d544392ace	CAMP_137	2025-11-11 06:44:01.765132	Opened
18	56920ebc-d6e5-4f10-be59-71915e820645	CAMP_125	2025-11-10 05:29:45.525509	Clicked
19	d8802fba-2f57-4717-af85-d858bb1f5c38	CAMP_104	2025-11-11 11:39:00.683256	Opened
20	80406038-207f-4029-812a-e87bdd1584d1	CAMP_141	2025-11-10 05:36:54.952251	Opened
21	d41aa0f2-8c45-4bba-90c3-b0fb5d745210	CAMP_167	2025-11-10 16:02:32.337818	Opened
22	3a7ec5ad-9fb8-4218-b1ff-010ef1210bdb	CAMP_173	2025-11-10 20:49:27.945585	Opened
23	126067f3-eb1c-4c60-a7c7-713cad5a4fe1	CAMP_194	2025-11-10 11:19:30.130059	Opened
24	218b5b7a-1cde-4822-bf5f-3c0ae1325364	CAMP_181	2025-11-09 19:57:37.635767	Sent
25	42d8cb73-95c5-46a2-885b-ca57ab71fc48	CAMP_112	2025-11-09 20:10:27.734881	Clicked
26	c52409b5-ce8f-4b9b-a30f-7750e4cb7680	CAMP_178	2025-11-10 18:09:54.758122	Opened
27	046bc638-b631-49c8-91de-2f536a2ecc9d	CAMP_134	2025-11-11 12:11:18.533277	Sent
28	2857d13b-9376-4d5d-afa8-38c33a56eece	CAMP_110	2025-11-11 17:26:32.390485	Sent
29	af10c0ea-95b1-4a85-8caa-91e3295cb2ec	CAMP_147	2025-11-10 23:58:52.134281	Opened
30	b4803abd-fd67-4e60-a989-a74e3740f401	CAMP_172	2025-11-10 16:31:29.020947	Opened
31	5326716b-5094-4bb2-a003-d9e8b62c47b1	CAMP_184	2025-11-10 04:37:19.368014	Opened
32	9cf3a39c-4100-4db6-8d12-c2313092dee2	CAMP_189	2025-11-10 03:51:03.546245	Clicked
33	efbb3328-45fc-4d0a-af5f-1be37a7edf46	CAMP_173	2025-11-10 03:22:18.45736	Clicked
34	c1e8c1f2-23c1-4d47-a02d-a796c6d67d5a	CAMP_136	2025-11-11 01:29:26.069932	Sent
35	05faded8-7c1c-40a4-8336-a87aea629b3e	CAMP_102	2025-11-10 15:51:56.513198	Opened
36	6345d196-3b52-45ef-8fc7-ae571e6c722d	CAMP_105	2025-11-10 23:38:42.79529	Opened
37	cded989e-7a7f-42cc-8f5e-f0c560acdc52	CAMP_116	2025-11-11 13:15:33.541524	Clicked
38	3263d521-2f10-447d-8ce5-57f54702b1d0	CAMP_198	2025-11-09 21:46:32.013388	Clicked
39	eb847bf2-f0f6-4fca-b39d-0bd46c65684d	CAMP_176	2025-11-11 01:11:48.385412	Clicked
40	1ee7321a-4455-44bf-97ed-c9a541d04d95	CAMP_118	2025-11-10 15:51:06.733482	Sent
41	d783722c-1371-48e2-a5d5-6e29e3cc709e	CAMP_194	2025-11-09 18:15:38.191443	Opened
42	a2ff5393-bdec-4ad0-bd11-ce7af0747243	CAMP_161	2025-11-10 18:55:18.886836	Clicked
43	7226baec-cd8e-4b8a-93e4-25e12af5d797	CAMP_117	2025-11-11 12:37:42.720172	Clicked
44	0eab8e09-a5aa-4aa9-a722-dc764d295351	CAMP_187	2025-11-10 19:46:44.341093	Sent
45	1d4a0f8c-101a-464b-a52c-7f928dd0d0a4	CAMP_102	2025-11-11 17:12:51.844593	Opened
46	2819876d-5a6b-40ad-8e34-070d5e325685	CAMP_141	2025-11-11 02:51:06.008252	Sent
47	a3b1c6f8-f159-4e92-8ac3-acea7e7007fb	CAMP_102	2025-11-10 11:11:22.867819	Clicked
48	9aa746a8-8667-499e-b952-95aad6b177b6	CAMP_179	2025-11-10 08:34:18.54881	Opened
49	f26ec986-c4cd-4fff-9ad6-fe95fcfa49a8	CAMP_114	2025-11-11 02:31:37.774837	Opened
50	577cae11-d439-4357-a788-f669e8431a45	CAMP_158	2025-11-11 15:12:25.388632	Sent
51	340316d0-22fb-4432-ac52-f24e4e909331	CAMP_168	2025-11-10 11:31:06.348749	Clicked
52	94f995a1-afb7-4ee7-a4b9-bd4e956de698	CAMP_135	2025-11-10 05:33:13.66554	Opened
53	11ec31ec-5471-4faf-aa3d-badc4ca9f580	CAMP_164	2025-11-10 05:18:53.259743	Opened
54	a2f7f4ea-8549-4845-ad63-ea45f08ec238	CAMP_145	2025-11-09 21:59:15.629862	Sent
55	5a250fe6-2d00-4151-b00a-8299bb97226e	CAMP_172	2025-11-09 23:15:37.420063	Clicked
56	059c887e-e79c-41a5-9734-a7d1b0cbea3d	CAMP_163	2025-11-10 01:55:27.854802	Clicked
57	0b8047df-caf7-4203-95a2-7f4cb3847320	CAMP_181	2025-11-10 12:09:59.65653	Clicked
58	586783dc-125b-4d20-ae7c-04f1798d82b4	CAMP_197	2025-11-11 15:09:56.312568	Opened
59	e8fe098b-3a14-45f3-af7f-50565763f76c	CAMP_126	2025-11-10 07:52:48.468013	Opened
60	a5b12e8d-01a8-4775-9115-bdb2811e77ce	CAMP_155	2025-11-10 18:10:18.181088	Clicked
61	e4e9f4d3-9863-48a4-abc8-4cd314c432b9	CAMP_191	2025-11-11 02:31:08.638435	Clicked
62	8d6730b6-65da-4c06-9a57-707c565c6dd5	CAMP_189	2025-11-10 09:58:42.011037	Opened
63	b02ff5f3-3437-4d5e-8036-4af1ff6e9ffd	CAMP_188	2025-11-10 21:03:52.125277	Clicked
64	dd7613ee-404c-40aa-a2cd-f971a576072a	CAMP_127	2025-11-11 08:15:14.05858	Opened
65	53fae3c3-1ec2-4246-968c-3d1b2fce0f2b	CAMP_101	2025-11-10 00:55:10.108595	Opened
66	d82c8c36-0861-4260-9d28-57324dcac75c	CAMP_171	2025-11-09 21:14:36.158038	Clicked
67	39591117-7ad2-4d07-a7aa-6e49da561c88	CAMP_164	2025-11-11 06:06:54.318161	Clicked
68	e8eb219f-f1bf-4da7-b99e-0911c8b0176e	CAMP_142	2025-11-11 08:24:38.74605	Opened
69	7e497252-e770-4ed2-93dd-4f4d9e53b78b	CAMP_107	2025-11-10 10:53:39.00466	Clicked
70	1f0c96fd-e09b-47e1-85f9-7a408f713f78	CAMP_154	2025-11-09 20:34:49.8337	Opened
71	e4aec77b-4904-42db-9508-1d2a2c063b93	CAMP_144	2025-11-10 23:13:01.172108	Clicked
72	3009fc21-3969-4af1-a3ee-bf4713a7785a	CAMP_102	2025-11-10 08:35:00.202238	Opened
73	ec9f45cd-c41a-428b-8e4c-ef211cbe7ed9	CAMP_101	2025-11-09 21:05:29.44867	Opened
74	02e2900b-5a6a-46a3-8f21-a563a5b0d49b	CAMP_150	2025-11-11 01:51:06.911612	Opened
75	282485b3-79f1-4d0c-90fc-e001689b2026	CAMP_171	2025-11-11 09:50:39.938605	Sent
76	1c1d29b8-cd77-4988-80dc-765214fefd7e	CAMP_156	2025-11-10 06:06:55.288071	Opened
77	91e3d4e4-2778-4ccd-99a9-13f5240b94e5	CAMP_164	2025-11-11 07:49:34.827584	Sent
78	762b8656-ea3d-4c83-a23d-5993f3611283	CAMP_185	2025-11-11 04:15:59.463901	Opened
79	7aebea63-7c0b-48b1-a015-4843eba9be12	CAMP_116	2025-11-10 16:12:00.622909	Sent
80	edeb6583-c96b-48c6-8d0c-7dcb3715523b	CAMP_148	2025-11-10 23:58:48.820492	Opened
81	9107a856-067d-4346-a8f3-1d2fb2f9e714	CAMP_192	2025-11-10 23:52:50.784174	Opened
82	407c303c-3ce2-43eb-a73a-1cdbd3c711e6	CAMP_151	2025-11-11 08:37:41.823949	Opened
83	255dbe26-5d85-4ea6-91d6-0aec665ec449	CAMP_147	2025-11-10 03:53:39.815647	Clicked
84	fd31fc23-adb7-46c6-8d3e-c10d862d90a0	CAMP_198	2025-11-10 02:53:38.783944	Clicked
85	83404da4-cdab-48b8-b9c2-6c535d2a593f	CAMP_127	2025-11-11 09:32:15.652494	Clicked
86	ca7ad116-7d99-475a-9a94-a52a9bffce63	CAMP_185	2025-11-11 01:00:31.776147	Opened
87	09530af6-bf20-4a1f-baac-8caa18adcec4	CAMP_179	2025-11-11 13:45:21.10348	Sent
88	82df41b2-26c0-4feb-8b1f-3d249961c96f	CAMP_167	2025-11-09 23:50:26.458428	Sent
89	232d8a7a-1c94-436a-b931-80f2e6157c0d	CAMP_149	2025-11-10 03:43:25.553064	Sent
90	def06ab7-519f-4b97-be2d-1a8852cb60bf	CAMP_129	2025-11-11 13:26:17.43428	Clicked
91	fadff742-8edb-46ac-9a55-b0dd006b9bed	CAMP_106	2025-11-10 04:31:11.745821	Sent
92	0a76c614-ccaf-4dae-955e-d27f53797ebb	CAMP_193	2025-11-11 16:40:52.734616	Opened
93	7683aa6c-a7f1-4167-986b-1fa01552a981	CAMP_197	2025-11-09 21:41:41.345017	Clicked
94	56fcdc94-1791-4d7f-acdd-588aa694d33f	CAMP_166	2025-11-10 05:22:19.482864	Opened
95	01b3dbf0-ed62-42c2-83a4-04fe2de859d4	CAMP_145	2025-11-11 05:09:02.148086	Opened
96	6d1e11f3-0108-4bb7-8e54-dc04cca460ac	CAMP_149	2025-11-11 14:30:24.523011	Clicked
97	8e544b2d-368d-4cd5-b3b5-a0d96cf362f3	CAMP_119	2025-11-11 11:39:02.617107	Clicked
98	08a83396-16e0-4f12-8c92-13874ff7fed0	CAMP_170	2025-11-10 12:58:04.652555	Opened
99	debde70e-6a4b-4a30-8601-9eae170786bf	CAMP_179	2025-11-10 18:13:02.783779	Opened
100	7441f62a-66f0-43f6-a0fb-431f4a9fa5c3	CAMP_120	2025-11-11 11:22:20.716429	Sent
\.


--
-- Data for Name: employee_payrolls; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.employee_payrolls (payroll_id, employee_id, pay_date, monthly_salary_eur) FROM stdin;
1	77	2024-10-01	2251
2	47	2024-10-01	4304
3	12	2024-10-01	4915
4	96	2024-10-01	4033
5	22	2024-10-01	3404
6	99	2024-10-01	3486
7	30	2024-10-01	3665
8	79	2024-10-01	2150
9	98	2024-10-01	4134
10	90	2024-10-01	4468
11	26	2024-10-01	2746
12	31	2024-10-01	3196
13	26	2024-10-01	3200
14	99	2024-10-01	4871
15	13	2024-10-01	3600
16	21	2024-10-01	4416
17	45	2024-10-01	3651
18	25	2024-10-01	2286
19	89	2024-10-01	3927
20	63	2024-10-01	2989
21	47	2024-10-01	3959
22	41	2024-10-01	4123
23	68	2024-10-01	2186
24	82	2024-10-01	3326
25	21	2024-10-01	2340
26	47	2024-10-01	2706
27	39	2024-10-01	2744
28	46	2024-10-01	2337
29	14	2024-10-01	2191
30	14	2024-10-01	3163
31	79	2024-10-01	4096
32	81	2024-10-01	3943
33	91	2024-10-01	3202
34	75	2024-10-01	3695
35	20	2024-10-01	4195
36	15	2024-10-01	3435
37	81	2024-10-01	2359
38	74	2024-10-01	3908
39	49	2024-10-01	4168
40	40	2024-10-01	4809
41	27	2024-10-01	3053
42	64	2024-10-01	4775
43	2	2024-10-01	4827
44	92	2024-10-01	2960
45	36	2024-10-01	3611
46	37	2024-10-01	2545
47	48	2024-10-01	2426
48	60	2024-10-01	3283
49	41	2024-10-01	2517
50	21	2024-10-01	2989
51	21	2024-10-01	3163
52	91	2024-10-01	2452
53	53	2024-10-01	3140
54	51	2024-10-01	4570
55	44	2024-10-01	4819
56	41	2024-10-01	3021
57	63	2024-10-01	4251
58	3	2024-10-01	3617
59	14	2024-10-01	3176
60	58	2024-10-01	2572
61	84	2024-10-01	2908
62	64	2024-10-01	2680
63	84	2024-10-01	4261
64	50	2024-10-01	4177
65	1	2024-10-01	2813
66	93	2024-10-01	3123
67	92	2024-10-01	4851
68	81	2024-10-01	4696
69	67	2024-10-01	2922
70	68	2024-10-01	2442
71	61	2024-10-01	4625
72	80	2024-10-01	4763
73	88	2024-10-01	2528
74	60	2024-10-01	2252
75	26	2024-10-01	4984
76	50	2024-10-01	2052
77	31	2024-10-01	4932
78	7	2024-10-01	2102
79	68	2024-10-01	3936
80	90	2024-10-01	2237
81	85	2024-10-01	2193
82	44	2024-10-01	2443
83	98	2024-10-01	2417
84	35	2024-10-01	3294
85	34	2024-10-01	2753
86	12	2024-10-01	2540
87	26	2024-10-01	3283
88	55	2024-10-01	3814
89	71	2024-10-01	2614
90	20	2024-10-01	2791
91	57	2024-10-01	3755
92	28	2024-10-01	4772
93	35	2024-10-01	2349
94	100	2024-10-01	3242
95	89	2024-10-01	2636
96	26	2024-10-01	3591
97	67	2024-10-01	4704
98	37	2024-10-01	4453
99	100	2024-10-01	4498
100	100	2024-10-01	3380
\.


--
-- Data for Name: employees; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.employees (employee_id, first_name, last_name, job_title, office_city_id) FROM stdin;
1	Léa	Robert	Marketing	3
2	Lucas	Robert	Developer	3
3	Lucas	Morel	Developer	3
4	Léa	Robert	Developer	3
5	Hugo	Morel	Developer	4
6	Léa	Bernard	Technician	5
7	Lucas	Robert	Marketing	2
8	Hugo	Robert	Finance	4
9	Lucas	Bernard	Developer	4
10	Lucas	Girard	Developer	2
11	Hugo	Girard	Developer	5
12	Manon	Robert	Developer	2
13	Lucas	Morel	Marketing	2
14	Lucas	Morel	Finance	2
15	Manon	Morel	Finance	3
16	Lucas	Robert	Finance	4
17	Hugo	Bernard	Technician	4
18	Hugo	Girard	Developer	3
19	Lucas	Morel	Marketing	2
20	Manon	Morel	Developer	2
21	Manon	Robert	Technician	3
22	Lucas	Bernard	Developer	4
23	Lucas	Robert	Finance	4
24	Léa	Morel	Developer	4
25	Hugo	Bernard	Technician	4
26	Lucas	Morel	Technician	5
27	Léa	Robert	Technician	5
28	Léa	Morel	Developer	2
29	Léa	Morel	Finance	3
30	Lucas	Robert	Marketing	4
31	Lucas	Girard	Technician	2
32	Manon	Morel	Marketing	4
33	Léa	Robert	Technician	2
34	Manon	Morel	Marketing	1
35	Hugo	Robert	Finance	4
36	Lucas	Robert	Developer	4
37	Léa	Robert	Marketing	4
38	Léa	Girard	Developer	3
39	Manon	Girard	Marketing	4
40	Hugo	Robert	Marketing	2
41	Lucas	Bernard	Marketing	1
42	Manon	Robert	Marketing	2
43	Manon	Girard	Marketing	3
44	Lucas	Robert	Technician	3
45	Lucas	Bernard	Developer	2
46	Manon	Morel	Marketing	2
47	Manon	Morel	Developer	5
48	Hugo	Morel	Developer	3
49	Léa	Morel	Technician	2
50	Lucas	Morel	Developer	2
51	Hugo	Robert	Developer	4
52	Manon	Robert	Marketing	2
53	Hugo	Girard	Marketing	2
54	Léa	Bernard	Developer	5
55	Manon	Robert	Finance	2
56	Lucas	Robert	Marketing	5
57	Manon	Girard	Marketing	3
58	Hugo	Girard	Finance	4
59	Hugo	Robert	Developer	3
60	Hugo	Robert	Technician	5
61	Léa	Robert	Marketing	1
62	Manon	Robert	Marketing	2
63	Manon	Robert	Marketing	2
64	Lucas	Robert	Technician	2
65	Lucas	Morel	Developer	4
66	Lucas	Robert	Marketing	1
67	Lucas	Robert	Developer	4
68	Hugo	Morel	Developer	1
69	Hugo	Morel	Marketing	5
70	Hugo	Morel	Marketing	2
71	Hugo	Morel	Technician	4
72	Léa	Robert	Technician	4
73	Lucas	Morel	Finance	4
74	Manon	Robert	Finance	2
75	Lucas	Bernard	Developer	4
76	Léa	Bernard	Developer	3
77	Hugo	Morel	Technician	3
78	Hugo	Robert	Technician	1
79	Manon	Robert	Technician	1
80	Léa	Robert	Marketing	3
81	Manon	Morel	Technician	5
82	Lucas	Girard	Marketing	1
83	Lucas	Morel	Finance	4
84	Manon	Morel	Developer	2
85	Lucas	Morel	Marketing	5
86	Léa	Bernard	Technician	1
87	Manon	Bernard	Technician	2
88	Lucas	Morel	Developer	3
89	Manon	Morel	Marketing	4
90	Léa	Girard	Finance	1
91	Lucas	Morel	Technician	5
92	Manon	Girard	Developer	1
93	Manon	Bernard	Technician	3
94	Lucas	Morel	Technician	5
95	Manon	Robert	Technician	4
96	Hugo	Robert	Marketing	3
97	Manon	Bernard	Developer	4
98	Léa	Morel	Developer	2
99	Lucas	Bernard	Marketing	4
100	Manon	Girard	Developer	2
\.


--
-- Data for Name: logistics_fleet_vehicles; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.logistics_fleet_vehicles (vehicle_id, vehicle_type, license_plate, current_city_id) FROM stdin;
V-101	Camionnette	AA-123-BB	2
V-102	Camionnette	AA-123-BB	5
V-103	Camionnette	AA-123-BB	2
V-104	Camionnette	AA-123-BB	4
V-105	Camionnette	AA-123-BB	4
V-106	Camionnette	AA-123-BB	4
V-107	Camionnette	AA-123-BB	4
V-108	Camionnette	AA-123-BB	3
V-109	Camionnette	AA-123-BB	4
V-110	Camionnette	AA-123-BB	3
V-111	Camionnette	AA-123-BB	5
V-112	Camionnette	AA-123-BB	2
V-113	Camionnette	AA-123-BB	3
V-114	Camionnette	AA-123-BB	3
V-115	Camionnette	AA-123-BB	3
V-116	Camionnette	AA-123-BB	3
V-117	Camionnette	AA-123-BB	5
V-118	Camionnette	AA-123-BB	1
V-119	Camionnette	AA-123-BB	3
V-120	Camionnette	AA-123-BB	2
V-121	Camionnette	AA-123-BB	4
V-122	Camionnette	AA-123-BB	2
V-123	Camionnette	AA-123-BB	2
V-124	Camionnette	AA-123-BB	2
V-125	Camionnette	AA-123-BB	3
V-126	Camionnette	AA-123-BB	3
V-127	Camionnette	AA-123-BB	3
V-128	Camionnette	AA-123-BB	3
V-129	Camionnette	AA-123-BB	2
V-130	Camionnette	AA-123-BB	2
V-131	Camionnette	AA-123-BB	2
V-132	Camionnette	AA-123-BB	2
V-133	Camionnette	AA-123-BB	2
V-134	Camionnette	AA-123-BB	2
V-135	Camionnette	AA-123-BB	3
V-136	Camionnette	AA-123-BB	4
V-137	Camionnette	AA-123-BB	2
V-138	Camionnette	AA-123-BB	5
V-139	Camionnette	AA-123-BB	1
V-140	Camionnette	AA-123-BB	3
V-141	Camionnette	AA-123-BB	2
V-142	Camionnette	AA-123-BB	2
V-143	Camionnette	AA-123-BB	1
V-144	Camionnette	AA-123-BB	1
V-145	Camionnette	AA-123-BB	5
V-146	Camionnette	AA-123-BB	3
V-147	Camionnette	AA-123-BB	3
V-148	Camionnette	AA-123-BB	4
V-149	Camionnette	AA-123-BB	3
V-150	Camionnette	AA-123-BB	2
V-151	Camionnette	AA-123-BB	5
V-152	Camionnette	AA-123-BB	3
V-153	Camionnette	AA-123-BB	1
V-154	Camionnette	AA-123-BB	2
V-155	Camionnette	AA-123-BB	3
V-156	Camionnette	AA-123-BB	3
V-157	Camionnette	AA-123-BB	2
V-158	Camionnette	AA-123-BB	3
V-159	Camionnette	AA-123-BB	2
V-160	Camionnette	AA-123-BB	4
V-161	Camionnette	AA-123-BB	4
V-162	Camionnette	AA-123-BB	3
V-163	Camionnette	AA-123-BB	2
V-164	Camionnette	AA-123-BB	2
V-165	Camionnette	AA-123-BB	4
V-166	Camionnette	AA-123-BB	3
V-167	Camionnette	AA-123-BB	5
V-168	Camionnette	AA-123-BB	2
V-169	Camionnette	AA-123-BB	3
V-170	Camionnette	AA-123-BB	2
V-171	Camionnette	AA-123-BB	3
V-172	Camionnette	AA-123-BB	4
V-173	Camionnette	AA-123-BB	5
V-174	Camionnette	AA-123-BB	2
V-175	Camionnette	AA-123-BB	3
V-176	Camionnette	AA-123-BB	1
V-177	Camionnette	AA-123-BB	3
V-178	Camionnette	AA-123-BB	1
V-179	Camionnette	AA-123-BB	5
V-180	Camionnette	AA-123-BB	2
V-181	Camionnette	AA-123-BB	4
V-182	Camionnette	AA-123-BB	4
V-183	Camionnette	AA-123-BB	3
V-184	Camionnette	AA-123-BB	4
V-185	Camionnette	AA-123-BB	2
V-186	Camionnette	AA-123-BB	2
V-187	Camionnette	AA-123-BB	2
V-188	Camionnette	AA-123-BB	3
V-189	Camionnette	AA-123-BB	4
V-190	Camionnette	AA-123-BB	3
V-191	Camionnette	AA-123-BB	3
V-192	Camionnette	AA-123-BB	2
V-193	Camionnette	AA-123-BB	4
V-194	Camionnette	AA-123-BB	5
V-195	Camionnette	AA-123-BB	4
V-196	Camionnette	AA-123-BB	4
V-197	Camionnette	AA-123-BB	1
V-198	Camionnette	AA-123-BB	4
V-199	Camionnette	AA-123-BB	4
V-200	Camionnette	AA-123-BB	3
\.


--
-- Data for Name: marketing_campaigns; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.marketing_campaigns (campaign_id, campaign_name, start_date, end_date, budget_eur) FROM stdin;
CAMP_101	Black Friday	2025-11-21	2026-01-10	1827
CAMP_102	Black Friday	2025-12-01	2026-01-08	3022
CAMP_103	Black Friday	2025-11-27	2026-01-18	4294
CAMP_104	Hiver Vélo	2025-11-13	2026-01-24	1258
CAMP_105	Black Friday	2025-11-22	2026-02-09	2654
CAMP_106	Rentrée Étudiant	2025-11-13	2026-01-31	4429
CAMP_107	Hiver Vélo	2025-12-10	2026-01-14	2328
CAMP_108	Black Friday	2025-11-16	2026-01-13	2980
CAMP_109	Promo Été	2025-11-23	2026-01-18	1466
CAMP_110	Hiver Vélo	2025-11-27	2026-02-01	2591
CAMP_111	Black Friday	2025-11-29	2025-12-15	2500
CAMP_112	Promo Été	2025-12-07	2025-12-21	4127
CAMP_113	Rentrée Étudiant	2025-11-16	2026-01-15	2599
CAMP_114	Promo Été	2025-12-07	2026-01-13	3932
CAMP_115	Black Friday	2025-12-07	2026-01-10	5515
CAMP_116	Hiver Vélo	2025-12-05	2025-12-16	5335
CAMP_117	Promo Été	2025-12-09	2026-01-14	5115
CAMP_118	Rentrée Étudiant	2025-11-15	2026-02-10	5714
CAMP_119	Black Friday	2025-11-29	2026-01-14	5698
CAMP_120	Rentrée Étudiant	2025-11-23	2026-01-24	2788
CAMP_121	Rentrée Étudiant	2025-11-13	2026-01-29	3453
CAMP_122	Promo Été	2025-11-24	2025-12-15	3665
CAMP_123	Rentrée Étudiant	2025-11-24	2025-12-23	3080
CAMP_124	Black Friday	2025-11-29	2026-02-07	5819
CAMP_125	Black Friday	2025-11-17	2026-01-04	2967
CAMP_126	Black Friday	2025-12-04	2026-01-14	5797
CAMP_127	Black Friday	2025-11-28	2026-01-03	4498
CAMP_128	Hiver Vélo	2025-12-10	2025-12-15	5477
CAMP_129	Hiver Vélo	2025-12-10	2026-01-30	4516
CAMP_130	Hiver Vélo	2025-11-22	2026-01-09	1866
CAMP_131	Rentrée Étudiant	2025-11-21	2025-12-31	3722
CAMP_132	Black Friday	2025-12-01	2025-12-22	2059
CAMP_133	Black Friday	2025-12-05	2026-01-30	2594
CAMP_134	Promo Été	2025-12-02	2025-12-22	5688
CAMP_135	Black Friday	2025-11-19	2026-01-04	5449
CAMP_136	Rentrée Étudiant	2025-11-29	2025-12-16	1460
CAMP_137	Black Friday	2025-11-16	2025-12-14	5387
CAMP_138	Promo Été	2025-11-29	2025-12-17	2193
CAMP_139	Rentrée Étudiant	2025-12-11	2026-01-10	5390
CAMP_140	Promo Été	2025-12-03	2026-01-22	2787
CAMP_141	Black Friday	2025-11-30	2026-01-10	1552
CAMP_142	Black Friday	2025-11-18	2025-12-18	2056
CAMP_143	Hiver Vélo	2025-11-26	2026-02-04	1457
CAMP_144	Rentrée Étudiant	2025-11-24	2026-01-17	4902
CAMP_145	Black Friday	2025-11-28	2026-01-28	5710
CAMP_146	Black Friday	2025-12-07	2025-12-12	4265
CAMP_147	Black Friday	2025-12-03	2025-12-28	3506
CAMP_148	Rentrée Étudiant	2025-11-29	2026-01-30	1117
CAMP_149	Black Friday	2025-12-07	2025-12-16	1700
CAMP_150	Black Friday	2025-11-15	2026-01-09	5651
CAMP_151	Rentrée Étudiant	2025-11-23	2026-02-07	5856
CAMP_152	Rentrée Étudiant	2025-11-22	2026-01-11	5363
CAMP_153	Black Friday	2025-11-20	2026-01-27	4547
CAMP_154	Black Friday	2025-11-22	2025-12-15	1815
CAMP_155	Promo Été	2025-11-17	2025-12-26	4644
CAMP_156	Rentrée Étudiant	2025-11-27	2025-12-25	1446
CAMP_157	Rentrée Étudiant	2025-11-19	2026-01-29	3752
CAMP_158	Promo Été	2025-12-04	2026-02-10	1526
CAMP_159	Rentrée Étudiant	2025-12-09	2025-12-13	5248
CAMP_160	Hiver Vélo	2025-12-08	2026-01-01	1511
CAMP_161	Rentrée Étudiant	2025-12-10	2026-01-19	3135
CAMP_162	Black Friday	2025-11-21	2025-12-28	2896
CAMP_163	Black Friday	2025-12-05	2025-12-15	3946
CAMP_164	Black Friday	2025-12-04	2026-01-31	1580
CAMP_165	Hiver Vélo	2025-11-13	2025-12-27	1253
CAMP_166	Rentrée Étudiant	2025-12-09	2026-01-21	2683
CAMP_167	Rentrée Étudiant	2025-12-03	2025-12-23	4097
CAMP_168	Rentrée Étudiant	2025-11-29	2025-12-14	2110
CAMP_169	Black Friday	2025-12-05	2025-12-22	1655
CAMP_170	Hiver Vélo	2025-12-06	2025-12-26	5915
CAMP_171	Rentrée Étudiant	2025-12-09	2026-01-03	2740
CAMP_172	Hiver Vélo	2025-11-25	2026-02-03	5093
CAMP_173	Rentrée Étudiant	2025-12-05	2025-12-27	1142
CAMP_174	Hiver Vélo	2025-11-13	2026-02-05	4401
CAMP_175	Rentrée Étudiant	2025-12-05	2025-12-20	5356
CAMP_176	Rentrée Étudiant	2025-12-04	2025-12-20	5187
CAMP_177	Hiver Vélo	2025-11-14	2026-01-17	3160
CAMP_178	Rentrée Étudiant	2025-11-17	2025-12-29	4445
CAMP_179	Rentrée Étudiant	2025-11-19	2026-01-24	5405
CAMP_180	Black Friday	2025-12-06	2026-01-15	2241
CAMP_181	Black Friday	2025-11-28	2026-01-21	4051
CAMP_182	Hiver Vélo	2025-11-17	2026-01-03	4584
CAMP_183	Rentrée Étudiant	2025-11-25	2025-12-30	2383
CAMP_184	Black Friday	2025-12-11	2025-12-23	4280
CAMP_185	Hiver Vélo	2025-11-13	2026-01-28	5151
CAMP_186	Promo Été	2025-11-22	2026-01-17	3861
CAMP_187	Promo Été	2025-11-23	2026-01-22	4902
CAMP_188	Rentrée Étudiant	2025-12-01	2026-01-05	5685
CAMP_189	Black Friday	2025-12-09	2025-12-31	4406
CAMP_190	Black Friday	2025-12-04	2025-12-17	1863
CAMP_191	Black Friday	2025-12-07	2026-01-22	1147
CAMP_192	Black Friday	2025-11-16	2026-02-07	2050
CAMP_193	Rentrée Étudiant	2025-11-16	2026-02-04	5266
CAMP_194	Rentrée Étudiant	2025-11-28	2025-12-16	4988
CAMP_195	Rentrée Étudiant	2025-12-06	2026-01-04	1460
CAMP_196	Black Friday	2025-11-27	2026-01-11	3199
CAMP_197	Hiver Vélo	2025-11-19	2026-02-09	4663
CAMP_198	Black Friday	2025-11-21	2026-01-19	4348
CAMP_199	Black Friday	2025-11-26	2026-01-12	5358
CAMP_200	Hiver Vélo	2025-11-23	2026-01-11	5374
\.


--
-- Data for Name: rental_transactions; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.rental_transactions (transaction_id, rental_id, payment_provider, processed_at, status) FROM stdin;
txn_b002d5566659495e7ea303e9731837d0	6042	Stripe	2025-11-07 07:13:37.470627	Success
txn_fc3701a490b59c14ba4a23c20b25ca80	8922	Stripe	2025-11-11 03:36:51.538382	Success
txn_7c72dec2072f3a9aa596b231f39c61eb	4099	Stripe	2025-11-06 18:57:27.832468	Success
txn_5181a3936b3ce81c29388a18949d57b7	8310	Stripe	2025-11-07 17:59:16.06233	Success
txn_af244cef7247abb306c3d52f2596e1e1	6445	Stripe	2025-11-09 01:46:05.914835	Success
txn_f932c9fb232f12fdba12ea34402aa05f	1285	Stripe	2025-11-10 04:21:22.676914	Success
txn_7d786dd139c749e4df0cd39d6a2d3154	4005	Stripe	2025-11-07 19:21:15.78195	Success
txn_3156049cb129b17d9eeacefbc0f81366	210	Stripe	2025-11-09 10:04:44.541507	Success
txn_9187b8f0934218f6cc952fc76250477f	1855	Stripe	2025-11-08 02:48:45.953702	Success
txn_af3cd293e2ecd676859c4f82c7510799	3063	Stripe	2025-11-06 19:03:00.029816	Success
txn_5b07b8fdfb78fb4dc69fb4eec2966f6f	4170	Stripe	2025-11-10 23:34:45.505928	Success
txn_825feed2721528f6d664258154249c5a	7957	Stripe	2025-11-11 04:06:32.874158	Success
txn_58cbdb9dcf2216213847beffa05bd9cb	5816	Stripe	2025-11-09 14:24:52.127141	Success
txn_db9748db3f20a83f6b50a9427985a848	4804	Stripe	2025-11-10 05:45:34.345658	Success
txn_d1faa1f64ba012450ca38f87b0e654fb	4443	Stripe	2025-11-11 11:42:22.056595	Success
txn_1ade2a2ac012382e58cab3febd362732	3284	Stripe	2025-11-07 15:57:53.630566	Success
txn_5a80c4b3fa1570c5a646042678513201	6584	Stripe	2025-11-07 08:00:56.864963	Success
txn_27d5eed41f690a2a419b41bc7f4f4fa6	7307	Stripe	2025-11-10 18:05:11.475866	Success
txn_f3d9399842d93efbb1063a2a8c0d3c38	8304	Stripe	2025-11-08 03:07:47.945102	Success
txn_786526fe99458ad2889e67a746e400b6	6883	Stripe	2025-11-09 00:21:11.56234	Success
txn_7096e9241dfdabcafc1c368cf5ba919f	2230	Stripe	2025-11-07 06:21:37.172808	Success
txn_7aeea9b38048ee8f86c01cf23327452a	8238	Stripe	2025-11-09 01:31:05.902973	Success
txn_3c82aff412212c10625ba04c44987a34	6283	Stripe	2025-11-11 02:53:15.702696	Success
txn_dbd24a8e41c7119b197f08ad0f410e22	2353	Stripe	2025-11-08 19:10:10.153582	Success
txn_8463aafbefa9ff0b2cf37a6f00f499fe	4336	Stripe	2025-11-11 00:26:57.450642	Success
txn_640ff8b699d0ed63e4767087bf6a39e0	8206	Stripe	2025-11-06 22:09:58.981677	Success
txn_e34e7dbecd27266f4c38547a13782a5e	5672	Stripe	2025-11-07 20:47:02.199695	Success
txn_bad55a93edf03f6ac6456e527acee9c1	8548	Stripe	2025-11-07 06:35:13.931871	Success
txn_bdcb51125dcd92012e151771294f4a10	6206	Stripe	2025-11-06 21:23:49.069672	Success
txn_374951d9f20c46f3f71296a2b5d49109	8660	Stripe	2025-11-09 22:10:00.463202	Success
txn_68dd150eae2ade1af173db11532e0e45	547	Stripe	2025-11-10 05:51:34.688265	Success
txn_1a91060be88a8a10da604db94411f010	3406	Stripe	2025-11-09 23:29:51.288187	Success
txn_6bb356cb6488afcff13cc98865fd15b9	4118	Stripe	2025-11-11 13:56:55.535215	Success
txn_14b18eb13e5631024eb32d59d5f70731	2844	Stripe	2025-11-10 07:02:09.172912	Success
txn_185756d081d963cbeb172eb9916c49be	8934	Stripe	2025-11-06 20:20:07.792055	Success
txn_3e4fd189b540b3d1b50b1b234d61b93e	393	Stripe	2025-11-07 08:53:06.08978	Success
txn_fddbbd22b344460c911d6c656e628c94	4396	Stripe	2025-11-10 14:04:00.497074	Success
txn_3619eea0c97823d26c0d16abf81ea506	6292	Stripe	2025-11-09 14:13:41.032285	Success
txn_3c6cb20b2a14909123f4dbad3e3139fc	3447	Stripe	2025-11-09 17:50:32.108904	Success
txn_49c9f83f26889b799c3bd05c2d73fb4b	1497	Stripe	2025-11-07 03:21:28.455099	Success
txn_d40259dfc1ff734bbb6387b2e31635b9	8771	Stripe	2025-11-08 02:43:48.161509	Success
txn_224dd5fa531718d48f5cbf49a08064b1	4696	Stripe	2025-11-11 02:39:24.334038	Success
txn_a6762f9b06dbabdb576a332aae8530f3	3689	Stripe	2025-11-10 15:59:10.944293	Success
txn_c8f9b35db0d8a9521f078cda3ce03d9e	9307	Stripe	2025-11-08 18:28:21.738273	Success
txn_c3dbb8f4c9998e9e6e7d550319d2d028	9521	Stripe	2025-11-08 19:13:26.437809	Success
txn_3475ebc99f0a727398c4803a3c6beb7d	9669	Stripe	2025-11-11 08:44:06.043914	Success
txn_b723f21436e8199c8772765519198f98	7852	Stripe	2025-11-11 07:52:23.995491	Success
txn_71d057085603ffb5724358dea650f042	760	Stripe	2025-11-07 21:46:48.271397	Success
txn_89d309f4df645e8c8b424843ea1734b5	7180	Stripe	2025-11-11 13:45:26.679936	Success
txn_92b37802161761cb59d5cc2895b8b90d	5612	Stripe	2025-11-10 08:24:16.059358	Success
txn_054deb23c8977f80952df52a904b3d38	3521	Stripe	2025-11-08 03:21:56.566476	Success
txn_e45260166742e7e3e9a507d31ab6b7dd	1961	Stripe	2025-11-07 10:34:27.667356	Success
txn_1b92f77b7a275d30b624da04f6cf0045	5249	Stripe	2025-11-10 02:44:00.326503	Success
txn_5d9a290b02ab4d78a9abfc71a92ac370	3221	Stripe	2025-11-09 20:19:04.070018	Success
txn_2e4e7ec79fba12ccfc0ec96a6e0350b2	9407	Stripe	2025-11-07 13:49:36.05714	Success
txn_9c17e59831fd24cf2122315c04939bfa	7024	Stripe	2025-11-07 00:56:12.561156	Success
txn_287e61faa8d4d52f6091e6e95f0f9107	8746	Stripe	2025-11-07 18:53:01.739258	Success
txn_4034af8ef0457062a74d595a2958eefc	6583	Stripe	2025-11-08 23:46:48.438931	Success
txn_2d3100513a0d4aee0fea711db4ab4667	4799	Stripe	2025-11-11 04:37:31.400012	Success
txn_47e07c7821e4ae40a28d3a3320fdcdfe	3557	Stripe	2025-11-11 11:10:40.389606	Success
txn_b6435b6dddb9efa7dcf31837261898b5	8265	Stripe	2025-11-07 22:20:40.531549	Success
txn_a3e0bd57944051a9b788b15a3d24154f	9839	Stripe	2025-11-06 21:32:06.730946	Success
txn_8ef87b1f8c40740ffbd25f960e2ef5dd	3929	Stripe	2025-11-10 13:31:15.563845	Success
txn_b23d207d0071c0dc3a0c4f0af30ed764	1809	Stripe	2025-11-08 04:18:35.089947	Success
txn_28cfe6461c95c8a186bc972676eb236c	2331	Stripe	2025-11-07 17:37:45.472202	Success
txn_0f430ed721e103b0677fa0814c9eaec1	4567	Stripe	2025-11-10 08:08:35.438735	Success
txn_61e2c3429d886cc54014b2c34e30d4f0	599	Stripe	2025-11-09 20:06:48.573217	Success
txn_fd305e17f97f4e68a04fbbca21d47e9a	2583	Stripe	2025-11-08 08:58:59.750058	Success
txn_3b54f90eb9d2c60030fbe9353489cafe	5064	Stripe	2025-11-08 19:59:25.673925	Success
txn_ed07379943554651033552939a65b382	6462	Stripe	2025-11-10 02:27:03.713758	Success
txn_1b40fecfcfe016ee7589b00a4b397215	4316	Stripe	2025-11-07 23:39:13.731491	Success
txn_470dd016b26b1ddb14f59b4514911e5b	7950	Stripe	2025-11-10 21:11:07.421283	Success
txn_d6c550b0487591ff3aa8362d8842ab67	6374	Stripe	2025-11-07 03:21:28.672882	Success
txn_a8efb3876c5bdb88eb10eabd0bc86721	7334	Stripe	2025-11-10 12:46:04.643097	Success
txn_cfad33658c6d805a1554eecec6c49344	8493	Stripe	2025-11-07 20:36:42.258496	Success
txn_a16f346499f8500485243028ce733e0c	8845	Stripe	2025-11-09 19:19:38.810531	Success
txn_76203f38aebec03385b699fd97276e53	9570	Stripe	2025-11-09 04:48:23.547528	Success
txn_c7214949432e63b05581ea94c9de9451	9226	Stripe	2025-11-09 22:14:40.640269	Success
txn_45b660fc6ddd6bac26712919b9b16d87	3134	Stripe	2025-11-10 12:40:21.734949	Success
txn_3bfd2830dc40373c1981b8eacc917ed2	9278	Stripe	2025-11-10 19:45:19.713528	Success
txn_7f43caf97c02b815cd1ec03c2d43f796	6606	Stripe	2025-11-09 21:18:51.551716	Success
txn_b4c488150fcd67f9e00267fabc9455ed	8473	Stripe	2025-11-09 22:07:45.808686	Success
txn_cf54fa2730c2f6cf9d43a7d8bd0b4ab7	216	Stripe	2025-11-09 04:06:13.167594	Success
txn_dfdafe5baf45ce7ad3aa5ebdf6145f52	4785	Stripe	2025-11-11 14:29:31.503503	Success
txn_8d613863339c5c8eac635ead12ecdd10	345	Stripe	2025-11-06 21:32:15.672713	Success
txn_ad5ea0aff8ff6353b3adb40042374837	7185	Stripe	2025-11-08 00:06:33.258617	Success
txn_6470b391d8cba4bc2632afc9ca4d8476	6350	Stripe	2025-11-10 08:43:05.7992	Success
txn_9294547f341fdc1fcb1af65a242f0c4d	1142	Stripe	2025-11-08 09:39:51.535968	Success
txn_ce9a5c189912794490cd8ba709774ca7	1599	Stripe	2025-11-10 19:55:56.216451	Success
txn_9cfc09b116d7788cef32158ad39d4362	4418	Stripe	2025-11-07 16:58:38.023087	Success
txn_062e6454cc70278b0d3fbf2ca1392c46	8089	Stripe	2025-11-07 23:09:27.656878	Success
txn_cd0b769b1686cf7f3893d02295c1fbe7	9645	Stripe	2025-11-11 09:33:14.564542	Success
txn_c1333bf1141b3ead5bad6f6f7981c3c2	6795	Stripe	2025-11-10 15:41:33.070333	Success
txn_d287a2c11a46e16d87a96e3d91163d60	9735	Stripe	2025-11-08 20:22:54.770659	Success
txn_6532ed978d054e47d8bf21f1b770d7e3	8699	Stripe	2025-11-09 03:05:45.16639	Success
txn_12a2515749c416af4ea2a1df7ca69a4e	7461	Stripe	2025-11-08 10:00:10.703287	Success
txn_7d45f9e575b328bfc2dbf9c67583c049	425	Stripe	2025-11-11 09:45:41.000486	Success
txn_fe07227fb40bce4a2cd87f40a869c2f2	6391	Stripe	2025-11-07 08:31:02.705437	Success
txn_91e0ecac31b54e2387710a47840e0cab	3112	Stripe	2025-11-08 07:54:31.881506	Success
txn_4aa9a126d7f1c4409a0cd2a84c0ce4db	3130	Stripe	2025-11-07 10:33:32.900387	Success
\.


--
-- Data for Name: rentals_archive_2022; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.rentals_archive_2022 (rental_id, bike_id, user_id, start_t, end_t) FROM stdin;
1	1001	cdba0d9a-26e2-468a-a1ca-1eebb1caec0b	2022-05-18 06:09:20.585977	2022-05-17 08:19:40.05573
2	1003	0717fa4b-e231-4d11-a49b-83b723c3b10c	2022-05-25 07:27:01.317066	2022-05-24 13:24:18.204106
3	1009	8af5f09a-fc6e-4aeb-aaad-56c6b6624ef6	2022-06-06 22:17:16.174502	2022-05-10 13:54:19.328658
4	1004	6fc94d99-6197-4fda-9ef9-e584a29fb076	2022-05-18 17:50:07.32298	2022-05-18 22:55:21.412793
5	1003	6b1d920a-d5f7-4271-ab82-158d51490cc6	2022-05-19 15:07:49.928697	2022-06-06 11:09:53.297804
6	1007	c8e8fab2-c9d4-4d61-ab76-bd666b6dcc13	2022-05-14 04:04:45.776592	2022-06-03 13:21:20.532403
7	1005	2407ff05-1a10-4743-bb06-0fd2e9b93888	2022-05-27 00:12:33.005904	2022-05-23 16:14:51.566287
8	1001	88bab982-5122-4b30-b5fa-4468f1aac86a	2022-05-21 06:42:30.949054	2022-06-07 11:06:47.559838
9	1004	fa0781d0-94d7-4e58-98b8-ca6900907ce9	2022-05-22 21:16:05.068815	2022-05-16 00:00:11.971392
10	1003	8c20fa00-8e3a-42b0-93f0-6c2ecd788e3e	2022-05-22 15:00:33.576269	2022-05-29 08:50:47.727981
11	1002	63811f66-3869-4928-9068-744822401837	2022-05-28 00:45:18.322378	2022-05-14 11:14:01.015156
12	1005	49d4ac8e-63b0-49b7-bf69-6d4496e85ec0	2022-06-01 05:30:44.52925	2022-05-16 04:47:50.410291
13	1007	b3901340-e097-42ef-b18e-eec0b2d75ae0	2022-05-21 09:25:46.180137	2022-06-01 22:52:34.159209
14	1009	3689f093-a2fc-44e4-afd8-da5e898ec383	2022-05-29 19:27:17.873381	2022-05-13 17:48:52.739162
15	1008	6c765a8e-4325-463c-9b71-ba95398e118c	2022-05-20 02:51:06.928648	2022-05-29 13:52:30.211748
16	1000	28a4180a-dc99-44b1-902b-68b7b8ac5d47	2022-06-08 23:03:36.641869	2022-05-20 21:15:14.794108
17	1009	72609a6d-cdb4-4fea-bd77-b785bd413a2d	2022-06-07 02:48:24.686094	2022-06-06 15:21:06.565712
18	1003	81dd0ad1-4c48-4a49-88d8-06580d8b1a30	2022-06-01 03:27:54.678045	2022-05-23 22:24:25.527264
19	1005	6678b5d7-a722-4939-af5f-f090477ea0da	2022-06-07 22:47:07.916957	2022-05-18 03:28:52.609123
20	1004	84428263-e1fe-4406-b70f-43217dfd97a8	2022-06-05 10:25:05.826789	2022-05-17 20:41:31.161999
21	1009	659ad052-d097-41f4-ac7c-f1713efd2b2c	2022-06-05 18:34:56.534364	2022-05-21 07:28:20.222234
22	1008	160c5f11-68e6-4f0e-b052-916781053960	2022-05-29 00:25:28.992268	2022-05-27 07:25:37.796108
23	1006	e16e7a9e-42fe-4a71-b10e-b5b737ee6b22	2022-06-09 02:51:20.08204	2022-06-08 02:47:54.261279
24	1001	32ff3d37-c46c-47d5-860c-0487185b5234	2022-05-22 09:23:37.698942	2022-05-22 23:09:29.733248
25	1003	41e1a684-6de4-47ee-8999-d15f8874443f	2022-05-28 16:08:53.449644	2022-06-08 15:45:30.145462
26	1007	2d7fd023-5751-4130-b94c-3335079a2d89	2022-06-05 14:31:53.883383	2022-05-31 16:57:32.27221
27	1000	d38093f3-a12d-457e-a795-602cc2bce6b0	2022-05-16 06:38:47.690028	2022-05-12 23:20:19.154491
28	1000	0d144993-24f8-4d77-a96a-cfab11fd0dcf	2022-05-29 14:42:25.034905	2022-05-12 03:22:53.826317
29	1008	be7d8d2f-dd1e-4228-bbed-4b7acaf8ffec	2022-05-10 19:34:34.818748	2022-05-15 00:08:52.14767
30	1010	07ec8637-e427-450e-9c8c-6d8d705b9894	2022-05-19 22:31:44.08273	2022-06-06 22:46:34.961891
31	1007	ed0c0bbf-20cb-4cce-b83a-06eb72419e31	2022-05-25 12:38:06.097682	2022-05-19 16:46:59.43448
32	1001	ed6d212f-4c63-426d-b991-41905083f00f	2022-05-23 16:14:10.636497	2022-05-12 06:43:44.137745
33	1000	3012be39-6ade-40d0-aba6-be37875751e3	2022-05-16 18:30:37.614805	2022-05-12 18:06:24.147951
34	1010	00af5ecf-57b1-4baf-b666-90e9c0e7a00d	2022-05-20 09:46:56.499214	2022-05-31 14:27:38.628961
35	1009	8c2939d1-839e-42f7-bf67-4d1e32b97b14	2022-05-20 14:08:17.510188	2022-05-21 08:23:32.908385
36	1008	5c974da0-97c4-4a2d-aa98-0185d3d9bb7b	2022-06-04 22:32:25.596174	2022-05-29 17:50:30.013658
37	1001	7452276f-4ed9-4b1d-8e77-f6bc8a054056	2022-05-11 11:14:07.686115	2022-06-04 12:30:15.473429
38	1005	a297f109-d579-44a0-970d-cc0adc120ad0	2022-06-02 16:18:48.593717	2022-05-22 07:03:02.627362
39	1004	360cb93a-bf99-4de4-8fba-b55cb08e3888	2022-05-27 19:30:14.076576	2022-06-02 07:13:47.65328
40	1008	f7e952cc-6556-4ccd-a743-bb3e1c88f733	2022-05-22 01:43:04.475953	2022-05-16 05:28:24.061117
41	1008	e28ef0a6-26df-44f2-a88d-c250095c6455	2022-05-27 19:19:11.411224	2022-05-26 13:36:59.219972
42	1002	aa03cb61-ecb2-450d-85cf-f3d3083dbee4	2022-06-06 05:18:58.092055	2022-06-07 06:10:48.94499
43	1006	bf4110b0-f548-4fed-8ba9-f4cb1e3643a3	2022-05-20 02:01:27.28935	2022-05-25 21:34:07.168929
44	1002	7863f0b0-d67e-4d49-93fc-0d32c9392281	2022-06-07 07:38:48.848472	2022-05-26 07:13:11.986615
45	1006	960b2e05-41b8-41ac-94e0-123cfa7c92e4	2022-05-26 21:38:45.228692	2022-06-08 08:56:46.004879
46	1005	55fe32ba-17ac-4944-89e0-8a518cbd58d3	2022-05-12 12:27:37.716093	2022-05-20 23:33:54.252376
47	1001	b0cd06d5-64bb-4d42-b226-4e91528998ce	2022-05-10 18:57:41.464418	2022-05-24 11:20:21.168795
48	1002	e4630a8b-9172-4130-b35f-c6d5bca74082	2022-05-11 03:08:04.60369	2022-05-18 10:15:23.413282
49	1009	2cf48335-e809-49e3-a746-7ac39dcb8d2c	2022-06-07 07:51:28.905207	2022-05-27 21:13:59.151437
50	1004	095fd2c6-0a0b-4842-acb7-aca0590a6325	2022-05-12 10:04:10.679866	2022-05-17 21:22:33.589719
51	1006	ee94c0e2-2a1e-4aca-ad58-9a26e4731f1c	2022-05-26 13:13:06.160416	2022-06-08 07:06:10.792875
52	1005	9fce8a6b-d1f1-41ca-aa29-496cc95cb1b1	2022-05-20 08:45:23.115844	2022-06-02 04:12:25.699493
53	1004	ae8392f6-c483-4872-bd0b-be277d6e9306	2022-05-21 16:00:15.312871	2022-05-20 14:08:17.238123
54	1001	2fa1fb77-af76-4236-bc3a-b81159a91410	2022-06-08 03:46:05.102767	2022-05-30 08:09:40.980831
55	1001	f831b158-bcb4-444c-afcd-6f8ab937d8d9	2022-05-29 21:27:08.692494	2022-06-03 20:38:18.270154
56	1006	b29672f1-49f2-4056-8c29-9e636285c80a	2022-05-19 14:11:19.815002	2022-05-16 12:34:53.890212
57	1009	e41b876c-b2a3-4d67-ac5d-5e93821315db	2022-06-04 23:26:41.924218	2022-05-27 06:42:12.128886
58	1001	15372dc5-42fa-4d04-b659-25118d7dafc6	2022-06-06 17:38:08.841238	2022-05-26 14:28:59.489607
59	1001	ccda0a78-9c78-4ec7-bb22-72185116b001	2022-05-28 15:36:34.859023	2022-05-17 12:29:01.443395
60	1007	e2157f3a-daf8-4d20-b7d5-e9f3d17ec49c	2022-06-06 22:06:35.853539	2022-06-02 19:45:18.595383
61	1007	d7696d57-0279-4733-95eb-eef185b8bda2	2022-06-09 05:18:07.306282	2022-06-05 04:59:34.763616
62	1006	18c28d8b-6561-473e-a915-ad1c8f987819	2022-05-31 15:52:19.282901	2022-05-21 00:03:39.48027
63	1003	e8191691-58f0-42b3-898a-378216096a44	2022-06-04 21:27:29.444231	2022-05-20 22:26:13.468054
64	1001	acc66375-72a8-4c95-8c1c-ae7cc444d5c1	2022-05-26 05:08:57.528766	2022-05-17 19:00:15.567779
65	1004	8af6402c-2106-4169-8390-23b1bc1f474c	2022-05-31 10:50:33.151606	2022-05-13 05:08:31.976129
66	1008	ffbe3677-e626-4824-b2a2-b63fdb1519d8	2022-06-03 20:14:16.379319	2022-05-27 11:01:43.504316
67	1007	d694ff2c-06ad-4a4a-a702-182e84a83227	2022-06-06 17:29:11.923475	2022-05-18 00:51:55.902733
68	1001	c43b0453-779a-4b0e-a688-88947276310d	2022-05-28 00:39:43.421139	2022-06-08 21:44:01.338359
69	1009	39dc8c71-a630-4cc8-9637-8d533885feb8	2022-05-19 14:51:45.649822	2022-05-17 18:22:29.106505
70	1000	77433b47-429d-45a5-b2cf-a3cd352e4684	2022-05-24 08:42:02.205685	2022-05-11 10:00:33.736229
71	1008	9765a98c-575d-44e9-b584-cf78c8e0b3cf	2022-05-19 14:19:41.547552	2022-05-16 04:44:15.498715
72	1000	e1b0fec0-b3d4-4f69-b8fe-5c533df8a225	2022-06-02 04:48:29.092775	2022-05-14 08:58:04.919696
73	1001	80a37bb1-4e06-458b-8024-d4177f3ef6a5	2022-05-20 17:25:14.311106	2022-05-18 02:34:05.7499
74	1004	b594ba03-05a7-4078-a786-24fca55928ab	2022-06-04 09:55:12.044103	2022-05-12 04:14:28.256202
75	1006	43915610-e6c7-4eec-a717-511dbe04ab42	2022-06-03 00:15:09.399801	2022-06-06 09:57:20.484869
76	1009	e68f1d48-23ca-40b9-a248-da8e7ecce61a	2022-06-05 13:22:04.402967	2022-05-28 02:31:01.52579
77	1002	248fb780-4381-4a68-81d6-f6fa03656860	2022-05-27 09:55:13.687569	2022-05-25 02:52:30.573597
78	1009	b73fc90d-d9a5-4743-8cd9-4f8ac78fd7b3	2022-06-05 12:26:25.056283	2022-06-05 23:09:54.975188
79	1007	2fdf7134-1a42-4981-93e8-d35cd4ffa4fd	2022-06-06 12:52:32.497485	2022-05-12 06:33:01.432526
80	1002	17674779-917b-488c-ba39-569c53ab60a6	2022-05-13 02:33:08.266211	2022-05-22 07:17:25.722669
81	1001	a664beda-6b41-45b7-8a29-a59ef2fcb9a7	2022-05-11 03:15:36.18747	2022-06-07 10:32:41.032942
82	1010	327e0900-7122-41ee-aac3-54f91dbce104	2022-05-25 17:22:11.07622	2022-06-02 08:07:14.529835
83	1009	22ba4f25-3bc5-4be6-89ff-ce298e09c587	2022-05-21 14:11:17.950307	2022-05-29 10:16:36.258377
84	1006	2dbf7526-5d35-44bc-9209-66213857edcd	2022-05-27 20:48:37.121576	2022-05-14 12:38:16.863612
85	1005	0e525988-369b-4b57-b57b-61206f6070f2	2022-05-18 20:47:18.323232	2022-05-20 08:42:44.253273
86	1004	9ab8cb3a-e827-4fb1-87e0-10927910d4aa	2022-06-01 07:18:37.664288	2022-05-30 19:55:38.792735
87	1003	e2133337-66d4-4bb2-9af9-7a377285ca10	2022-05-30 20:46:45.91976	2022-05-10 23:31:11.819388
88	1008	c2ce88dc-d3e2-4162-bc45-ce04814b4422	2022-05-19 13:31:13.740943	2022-05-27 01:13:59.548485
89	1001	ccb4d2a3-4e0f-4758-a22a-52afa5705997	2022-06-07 20:50:56.46271	2022-05-30 16:34:15.633572
90	1001	3b372498-72ff-4dce-a29d-1e8b5b1a990c	2022-06-04 08:09:42.397114	2022-05-15 16:59:56.269421
91	1007	2a941e59-5381-4745-aa4b-b9a5c29cd811	2022-05-12 15:01:10.328731	2022-06-08 09:20:51.300865
92	1003	b602093b-710c-4d4a-9db0-9de4bfa20092	2022-05-18 16:54:24.832751	2022-06-05 13:40:01.96402
93	1007	4b489fac-b60a-4823-a9a9-c40a8840c959	2022-05-22 05:27:05.819717	2022-05-24 08:08:05.989811
94	1002	9ffbdf9c-8bb4-4aef-9b27-824f7e9db6b3	2022-05-26 15:01:29.846199	2022-05-28 02:58:03.260847
95	1001	59ace9c7-7a96-4355-acce-a1f8988c98e0	2022-05-21 03:49:40.708534	2022-06-02 03:15:22.499907
96	1002	0afd6dfc-8cdb-45aa-8fc2-e38080894835	2022-05-25 18:33:29.32381	2022-05-14 05:07:02.725818
97	1005	4e05082d-5c5b-4bb8-914f-21b139cecb0c	2022-05-29 11:45:34.826049	2022-06-08 06:14:10.676686
98	1006	3de696b5-65ec-4b3e-a579-a869a3dc855a	2022-05-31 10:36:51.736716	2022-05-31 10:31:24.673011
99	1001	58514a90-039e-42b3-a5d9-39d4815b13d0	2022-05-20 23:07:15.406801	2022-06-01 22:24:20.551146
100	1001	8dcf4d35-e4e6-4d2a-9977-c31340e4279f	2022-06-03 08:04:02.663701	2022-05-31 04:54:47.600409
\.


--
-- Data for Name: station_hardware_inventory; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.station_hardware_inventory (hardware_id, station_id, terminal_model, install_date) FROM stdin;
1	sta_1004	Model T1	2022-11-26
2	sta_1004	Model T2-revA	2022-11-15
3	sta_1014	Model T2-revA	2022-01-28
4	sta_1013	Model T3	2022-02-03
5	sta_1016	Model T1	2022-01-02
6	sta_1006	Model T3	2022-03-27
7	sta_1007	Model T2-revA	2022-04-24
8	sta_1019	Model T2-revA	2022-04-22
9	sta_1014	Model T3	2022-03-18
10	sta_1005	Model T2-revA	2022-05-03
11	sta_1010	Model T2-revA	2022-12-05
12	sta_1011	Model T3	2022-01-03
13	sta_1006	Model T2-revA	2022-08-19
14	sta_1012	Model T1	2022-08-07
15	sta_1014	Model T3	2022-09-30
16	sta_1002	Model T1	2022-05-26
17	sta_1013	Model T3	2022-06-04
18	sta_1012	Model T1	2022-10-17
19	sta_1013	Model T2-revA	2022-05-13
20	sta_1006	Model T2-revA	2022-03-02
21	sta_1000	Model T3	2022-01-04
22	sta_1014	Model T2-revA	2022-06-06
23	sta_1011	Model T2-revA	2022-10-04
24	sta_1006	Model T1	2022-03-16
25	sta_1008	Model T3	2022-01-11
26	sta_1013	Model T1	2022-09-18
27	sta_1016	Model T3	2022-12-03
28	sta_1011	Model T2-revA	2022-01-12
29	sta_1003	Model T1	2022-08-13
30	sta_1003	Model T2-revA	2022-04-29
31	sta_1002	Model T2-revA	2022-10-08
32	sta_1006	Model T3	2022-07-22
33	sta_1012	Model T1	2022-12-21
34	sta_1019	Model T3	2022-01-17
35	sta_1013	Model T2-revA	2022-05-28
36	sta_1002	Model T3	2022-06-16
37	sta_1008	Model T2-revA	2022-09-04
38	sta_1007	Model T2-revA	2022-04-06
39	sta_1015	Model T2-revA	2022-02-20
40	sta_1008	Model T2-revA	2022-07-11
41	sta_1002	Model T1	2022-07-09
42	sta_1010	Model T2-revA	2022-10-26
43	sta_1019	Model T2-revA	2022-04-07
44	sta_1009	Model T2-revA	2022-06-10
45	sta_1011	Model T3	2022-12-24
46	sta_1017	Model T1	2022-02-05
47	sta_1010	Model T1	2022-09-15
48	sta_1003	Model T1	2022-10-28
49	sta_1011	Model T2-revA	2022-12-19
50	sta_1012	Model T2-revA	2022-06-15
51	sta_1017	Model T1	2022-01-27
52	sta_1004	Model T1	2022-03-09
53	sta_1001	Model T1	2022-08-07
54	sta_1005	Model T2-revA	2022-05-30
55	sta_1015	Model T3	2022-09-26
56	sta_1003	Model T1	2022-02-25
57	sta_1018	Model T3	2022-10-08
58	sta_1015	Model T1	2022-01-05
59	sta_1016	Model T3	2022-02-03
60	sta_1015	Model T2-revA	2022-01-04
61	sta_1009	Model T3	2022-03-19
62	sta_1012	Model T2-revA	2022-07-02
63	sta_1006	Model T1	2022-06-07
64	sta_1008	Model T2-revA	2022-08-26
65	sta_1011	Model T1	2022-03-28
66	sta_1002	Model T1	2022-03-16
67	sta_1010	Model T2-revA	2022-09-14
68	sta_1018	Model T3	2022-01-08
69	sta_1008	Model T1	2022-09-08
70	sta_1019	Model T2-revA	2022-08-18
71	sta_1003	Model T2-revA	2022-05-20
72	sta_1013	Model T1	2022-01-13
73	sta_1016	Model T2-revA	2022-09-09
74	sta_1005	Model T1	2022-11-15
75	sta_1014	Model T1	2022-01-19
76	sta_1012	Model T2-revA	2022-03-10
77	sta_1016	Model T3	2022-07-01
78	sta_1002	Model T2-revA	2022-02-13
79	sta_1002	Model T3	2022-04-21
80	sta_1016	Model T2-revA	2022-05-12
81	sta_1013	Model T1	2022-03-13
82	sta_1012	Model T2-revA	2022-08-04
83	sta_1015	Model T1	2022-03-28
84	sta_1011	Model T2-revA	2022-03-05
85	sta_1019	Model T3	2022-05-25
86	sta_1018	Model T1	2022-05-18
87	sta_1018	Model T3	2022-02-24
88	sta_1010	Model T2-revA	2022-02-03
89	sta_1002	Model T2-revA	2022-10-09
90	sta_1013	Model T2-revA	2022-11-14
91	sta_1001	Model T2-revA	2022-10-20
92	sta_1020	Model T3	2022-12-01
93	sta_1003	Model T2-revA	2022-06-08
94	sta_1016	Model T3	2022-03-25
95	sta_1015	Model T1	2022-10-11
96	sta_1000	Model T2-revA	2022-10-15
97	sta_1009	Model T2-revA	2022-08-28
98	sta_1012	Model T2-revA	2022-01-27
99	sta_1011	Model T3	2022-02-15
100	sta_1014	Model T1	2022-10-30
\.


--
-- Data for Name: station_maintenance_schedule; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.station_maintenance_schedule (maintenance_id, station_id, planned_date, technician_name, status) FROM stdin;
1	sta_1003	2025-10-25	Bernard	Pending
2	sta_1002	2025-11-07	Martin	Cancelled
3	sta_1007	2025-10-12	Martin	Done
4	sta_1014	2025-10-18	Martin	Done
5	sta_1004	2025-11-09	Martin	Done
6	sta_1001	2025-10-27	Bernard	Cancelled
7	sta_1011	2025-11-06	Martin	Pending
8	sta_1001	2025-10-31	Martin	Cancelled
9	sta_1014	2025-10-28	Bernard	Pending
10	sta_1010	2025-10-13	Martin	Done
11	sta_1018	2025-11-08	Dupont	Pending
12	sta_1008	2025-10-25	Robert	Done
13	sta_1010	2025-10-15	Bernard	Cancelled
14	sta_1002	2025-11-04	Dupont	Cancelled
15	sta_1003	2025-10-17	Bernard	Pending
16	sta_1009	2025-10-15	Bernard	Done
17	sta_1010	2025-10-25	Bernard	Done
18	sta_1017	2025-11-07	Bernard	Done
19	sta_1012	2025-10-27	Bernard	Done
20	sta_1018	2025-10-30	Dupont	Done
21	sta_1018	2025-11-08	Martin	Done
22	sta_1007	2025-10-28	Bernard	Cancelled
23	sta_1017	2025-11-05	Bernard	Cancelled
24	sta_1008	2025-10-31	Bernard	Pending
25	sta_1003	2025-10-23	Robert	Done
26	sta_1010	2025-10-30	Martin	Pending
27	sta_1011	2025-10-23	Martin	Done
28	sta_1018	2025-11-05	Martin	Pending
29	sta_1006	2025-10-21	Bernard	Done
30	sta_1016	2025-10-23	Bernard	Cancelled
31	sta_1019	2025-10-15	Bernard	Cancelled
32	sta_1012	2025-10-22	Bernard	Pending
33	sta_1013	2025-11-06	Bernard	Done
34	sta_1002	2025-11-05	Bernard	Done
35	sta_1015	2025-11-01	Bernard	Done
36	sta_1003	2025-10-24	Bernard	Pending
37	sta_1000	2025-11-06	Bernard	Pending
38	sta_1002	2025-10-14	Bernard	Cancelled
39	sta_1003	2025-10-14	Martin	Cancelled
40	sta_1011	2025-10-24	Dupont	Done
41	sta_1007	2025-10-18	Bernard	Done
42	sta_1010	2025-10-14	Martin	Pending
43	sta_1014	2025-10-30	Bernard	Done
44	sta_1015	2025-11-08	Robert	Pending
45	sta_1002	2025-11-08	Dupont	Pending
46	sta_1007	2025-10-29	Martin	Pending
47	sta_1014	2025-10-24	Martin	Cancelled
48	sta_1000	2025-10-26	Bernard	Done
49	sta_1001	2025-10-19	Robert	Pending
50	sta_1002	2025-10-25	Bernard	Pending
51	sta_1015	2025-10-24	Robert	Done
52	sta_1004	2025-10-30	Bernard	Done
53	sta_1017	2025-10-13	Martin	Done
54	sta_1020	2025-11-03	Robert	Done
55	sta_1016	2025-11-08	Dupont	Cancelled
56	sta_1003	2025-10-26	Robert	Done
57	sta_1019	2025-11-05	Dupont	Done
58	sta_1015	2025-11-08	Bernard	Done
59	sta_1010	2025-11-07	Martin	Pending
60	sta_1014	2025-11-08	Robert	Done
61	sta_1002	2025-10-14	Bernard	Cancelled
62	sta_1001	2025-10-18	Martin	Pending
63	sta_1020	2025-10-18	Martin	Done
64	sta_1018	2025-10-22	Martin	Done
65	sta_1011	2025-10-24	Bernard	Pending
66	sta_1018	2025-10-19	Martin	Cancelled
67	sta_1014	2025-11-11	Martin	Cancelled
68	sta_1018	2025-10-28	Robert	Done
69	sta_1002	2025-10-21	Martin	Done
70	sta_1003	2025-10-28	Martin	Cancelled
71	sta_1020	2025-10-17	Dupont	Done
72	sta_1012	2025-11-10	Bernard	Done
73	sta_1001	2025-10-26	Martin	Pending
74	sta_1018	2025-11-01	Dupont	Cancelled
75	sta_1015	2025-10-14	Dupont	Cancelled
76	sta_1016	2025-11-01	Bernard	Pending
77	sta_1002	2025-10-20	Dupont	Cancelled
78	sta_1017	2025-10-12	Bernard	Cancelled
79	sta_1001	2025-10-25	Bernard	Pending
80	sta_1001	2025-10-29	Robert	Cancelled
81	sta_1011	2025-11-10	Bernard	Done
82	sta_1006	2025-10-15	Martin	Pending
83	sta_1019	2025-10-13	Bernard	Pending
84	sta_1010	2025-10-29	Martin	Done
85	sta_1018	2025-10-20	Bernard	Pending
86	sta_1007	2025-10-30	Robert	Cancelled
87	sta_1018	2025-10-22	Dupont	Done
88	sta_1013	2025-11-06	Martin	Done
89	sta_1014	2025-11-09	Bernard	Cancelled
90	sta_1017	2025-10-21	Bernard	Done
91	sta_1019	2025-11-10	Dupont	Done
92	sta_1014	2025-10-24	Dupont	Cancelled
93	sta_1007	2025-10-20	Bernard	Done
94	sta_1002	2025-11-07	Martin	Done
95	sta_1004	2025-10-19	Martin	Cancelled
96	sta_1012	2025-10-20	Bernard	Pending
97	sta_1015	2025-10-26	Martin	Done
98	sta_1006	2025-11-03	Dupont	Done
99	sta_1002	2025-10-15	Martin	Done
100	sta_1010	2025-10-19	Bernard	Cancelled
\.


--
-- Data for Name: subscriptions; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.subscriptions (subscription_id, subscription_type, price_eur) FROM stdin;
sub_001	Mensuel	29.99
sub_002	Annuel	299.99
sub_003	Pay-as-you-go	1.50
sub_004	Étudiant	14.99
SUB_005	Mensuel	29.99
sub_006	Gratuit	0.00
sub_007	\N	45.00
\.


--
-- Data for Name: supplier_invoices; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.supplier_invoices (supplier_invoice_id, supplier_name, item_description, total_eur) FROM stdin;
SI-10001	BikeParts Inc.	Lot de 100 pneus	1495.29
SI-10002	BikeParts Inc.	Lot de 100 pneus	5015.27
SI-10003	CloudServices LLC	Lot de 100 pneus	1244.63
SI-10004	CloudServices LLC	Lot de 100 pneus	2352.53
SI-10005	OfficeRentals Co.	Lot de 100 pneus	5750.32
SI-10006	CloudServices LLC	Lot de 100 pneus	1929.58
SI-10007	OfficeRentals Co.	Lot de 100 pneus	2384.79
SI-10008	CloudServices LLC	Lot de 100 pneus	1213.11
SI-10009	BikeParts Inc.	Lot de 100 pneus	3930.26
SI-10010	CloudServices LLC	Lot de 100 pneus	2604.46
SI-10011	CloudServices LLC	Lot de 100 pneus	2510.72
SI-10012	CloudServices LLC	Lot de 100 pneus	5756.54
SI-10013	OfficeRentals Co.	Lot de 100 pneus	5678.18
SI-10014	BikeParts Inc.	Lot de 100 pneus	2914.69
SI-10015	CloudServices LLC	Lot de 100 pneus	3719.64
SI-10016	BikeParts Inc.	Lot de 100 pneus	2176.24
SI-10017	CloudServices LLC	Lot de 100 pneus	2986.10
SI-10018	BikeParts Inc.	Lot de 100 pneus	3328.95
SI-10019	BikeParts Inc.	Lot de 100 pneus	2403.79
SI-10020	CloudServices LLC	Lot de 100 pneus	3061.27
SI-10021	OfficeRentals Co.	Lot de 100 pneus	2750.86
SI-10022	OfficeRentals Co.	Lot de 100 pneus	2017.64
SI-10023	BikeParts Inc.	Lot de 100 pneus	3599.79
SI-10024	CloudServices LLC	Lot de 100 pneus	1690.02
SI-10025	BikeParts Inc.	Lot de 100 pneus	5822.75
SI-10026	CloudServices LLC	Lot de 100 pneus	4855.80
SI-10027	BikeParts Inc.	Lot de 100 pneus	4515.39
SI-10028	BikeParts Inc.	Lot de 100 pneus	4831.06
SI-10029	BikeParts Inc.	Lot de 100 pneus	4290.80
SI-10030	CloudServices LLC	Lot de 100 pneus	2967.77
SI-10031	CloudServices LLC	Lot de 100 pneus	5824.15
SI-10032	BikeParts Inc.	Lot de 100 pneus	5911.81
SI-10033	CloudServices LLC	Lot de 100 pneus	5316.09
SI-10034	CloudServices LLC	Lot de 100 pneus	5541.21
SI-10035	BikeParts Inc.	Lot de 100 pneus	2217.67
SI-10036	BikeParts Inc.	Lot de 100 pneus	2982.79
SI-10037	CloudServices LLC	Lot de 100 pneus	4295.32
SI-10038	BikeParts Inc.	Lot de 100 pneus	3748.99
SI-10039	CloudServices LLC	Lot de 100 pneus	5711.75
SI-10040	CloudServices LLC	Lot de 100 pneus	4873.18
SI-10041	CloudServices LLC	Lot de 100 pneus	5388.01
SI-10042	CloudServices LLC	Lot de 100 pneus	5208.06
SI-10043	BikeParts Inc.	Lot de 100 pneus	4450.06
SI-10044	CloudServices LLC	Lot de 100 pneus	1917.91
SI-10045	OfficeRentals Co.	Lot de 100 pneus	1370.44
SI-10046	CloudServices LLC	Lot de 100 pneus	2375.91
SI-10047	CloudServices LLC	Lot de 100 pneus	3476.66
SI-10048	OfficeRentals Co.	Lot de 100 pneus	3043.61
SI-10049	BikeParts Inc.	Lot de 100 pneus	2257.83
SI-10050	CloudServices LLC	Lot de 100 pneus	4306.70
SI-10051	CloudServices LLC	Lot de 100 pneus	1521.44
SI-10052	BikeParts Inc.	Lot de 100 pneus	2788.79
SI-10053	CloudServices LLC	Lot de 100 pneus	4948.13
SI-10054	OfficeRentals Co.	Lot de 100 pneus	1822.15
SI-10055	CloudServices LLC	Lot de 100 pneus	2573.29
SI-10056	CloudServices LLC	Lot de 100 pneus	5662.39
SI-10057	BikeParts Inc.	Lot de 100 pneus	2438.08
SI-10058	CloudServices LLC	Lot de 100 pneus	2607.71
SI-10059	OfficeRentals Co.	Lot de 100 pneus	2789.88
SI-10060	CloudServices LLC	Lot de 100 pneus	3630.80
SI-10061	OfficeRentals Co.	Lot de 100 pneus	2305.47
SI-10062	CloudServices LLC	Lot de 100 pneus	5565.20
SI-10063	CloudServices LLC	Lot de 100 pneus	3103.11
SI-10064	CloudServices LLC	Lot de 100 pneus	2590.55
SI-10065	BikeParts Inc.	Lot de 100 pneus	4309.34
SI-10066	BikeParts Inc.	Lot de 100 pneus	1551.51
SI-10067	CloudServices LLC	Lot de 100 pneus	1806.60
SI-10068	CloudServices LLC	Lot de 100 pneus	1642.90
SI-10069	BikeParts Inc.	Lot de 100 pneus	2330.46
SI-10070	CloudServices LLC	Lot de 100 pneus	5962.04
SI-10071	OfficeRentals Co.	Lot de 100 pneus	3343.27
SI-10072	OfficeRentals Co.	Lot de 100 pneus	4115.39
SI-10073	BikeParts Inc.	Lot de 100 pneus	1994.04
SI-10074	CloudServices LLC	Lot de 100 pneus	4828.58
SI-10075	CloudServices LLC	Lot de 100 pneus	4983.70
SI-10076	BikeParts Inc.	Lot de 100 pneus	1881.22
SI-10077	BikeParts Inc.	Lot de 100 pneus	4164.69
SI-10078	BikeParts Inc.	Lot de 100 pneus	1506.40
SI-10079	BikeParts Inc.	Lot de 100 pneus	4759.62
SI-10080	CloudServices LLC	Lot de 100 pneus	2962.28
SI-10081	OfficeRentals Co.	Lot de 100 pneus	5771.19
SI-10082	OfficeRentals Co.	Lot de 100 pneus	2699.71
SI-10083	CloudServices LLC	Lot de 100 pneus	5042.88
SI-10084	CloudServices LLC	Lot de 100 pneus	4643.90
SI-10085	CloudServices LLC	Lot de 100 pneus	2157.69
SI-10086	OfficeRentals Co.	Lot de 100 pneus	4127.20
SI-10087	OfficeRentals Co.	Lot de 100 pneus	1983.35
SI-10088	CloudServices LLC	Lot de 100 pneus	1648.78
SI-10089	CloudServices LLC	Lot de 100 pneus	2265.41
SI-10090	CloudServices LLC	Lot de 100 pneus	2372.70
SI-10091	OfficeRentals Co.	Lot de 100 pneus	5970.93
SI-10092	OfficeRentals Co.	Lot de 100 pneus	3700.37
SI-10093	CloudServices LLC	Lot de 100 pneus	2584.24
SI-10094	CloudServices LLC	Lot de 100 pneus	2044.40
SI-10095	CloudServices LLC	Lot de 100 pneus	5374.91
SI-10096	OfficeRentals Co.	Lot de 100 pneus	4275.99
SI-10097	CloudServices LLC	Lot de 100 pneus	4278.06
SI-10098	CloudServices LLC	Lot de 100 pneus	5740.34
SI-10099	CloudServices LLC	Lot de 100 pneus	5329.92
SI-10100	CloudServices LLC	Lot de 100 pneus	1628.14
\.


--
-- Data for Name: user_accounts; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.user_accounts (user_id, first_name, last_name, email, birthdate, registration_date, subscription_id) FROM stdin;
a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890	Alice	Martin	alice.martin@email.com	1992-03-15	2023-01-10	sub_001
b2c3d4e5-f6a7-8901-b2c3-d4e5f6a78901	Bob	Dupont	bob.dupont@email.com	1985-07-22	2023-02-15	sub_002
c3d4e5f6-a7b8-9012-c3d4-e5f6a7b89012	Clara	Petit	clara.petit@email.com	2001-11-05	2023-03-20	sub_004
d4e5f6a7-b8c9-0123-d4e5-f6a7b8c90123	David	\N	david.leroy@email.com	1995-01-30	05/04/2023	sub_003
e5f6a7b8-c9d0-1234-e5f6-a7b8c9d01234	Eva	Moreau	eva.moreau@email.com	1900-01-01	2023-05-12	sub_001
f6a7b8c9-d0e1-2345-f6a7-b8c9d0e12345	Frank	Garcia	frank.garcia@email.com	\N	2023-06-18	sub_999
07b8c9d0-e1f2-3456-07b8-c9d0e1f23456	Grace	Lee	grace.lee@email.com	1998-09-10	2023-07-22	sub_002
18c9d0e1-f2a3-4567-18c9-d0e1f2a34567	Hugo	Roux	hugo.roux@email.com	2003-05-12	2023-08-01	sub_004
29d0e1f2-a3b4-5678-29d0-e1f2a3b45678	Ines	Da Silva	ines.dasilva@email.com	19/02/1996	2023-09-05	SUB_005
30e1f2a3-b4c5-6789-30e1-f2a3b4c56789	Julien	Mercier	julien.mercier@email.com	1988-12-25	2023-10-15	\N
\.


--
-- Data for Name: user_accounts_deprecated; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.user_accounts_deprecated (user_id, email, birthdate, registration_date) FROM stdin;
520ca48e-c73b-4ff5-9d81-2aa8e8202cf8	08c0bd06a90632b5e552c1eb7d9e6e29@old-email.com	1980-01-01	2021-01-01
4edac67a-9322-40fb-8447-2ba1a25d5774	46aa296a466f493c2e7d4aab7a23c626@old-email.com	1980-01-01	2021-01-01
c77defcd-248c-4c46-9ece-0367e4ed612a	7d8d70112d77de26b55686006d958d47@old-email.com	1980-01-01	2021-01-01
b476ccf9-b199-4fe7-8890-2d71d0f87ee5	37f681cc0d1145fc668d37a9a7da61fd@old-email.com	1980-01-01	2021-01-01
bc97792e-a41d-4fdc-945b-a8c5d36ef952	256e68015a03f42f159591bb23e93d28@old-email.com	1980-01-01	2021-01-01
705dabaf-127a-458d-ac31-c35de5208532	55486e2d9506fbe159564a099a030358@old-email.com	1980-01-01	2021-01-01
7dc0926c-811d-487e-96f7-e41c2b300687	0f00150ca874d94e5e7decc07db25bd9@old-email.com	1980-01-01	2021-01-01
2efdb90e-dbaa-4b7f-a949-4dd29c39abd4	fd703f808d83158ed2500a63eea763a8@old-email.com	1980-01-01	2021-01-01
95e53258-b2c1-44c9-bd49-910737200563	71dd59cbc51d755dc82f6b0f01faa3b7@old-email.com	1980-01-01	2021-01-01
27f16e6a-0bd7-45d9-ad16-c236a1d038b2	437749f95c6ab92aca8e58aba7f06311@old-email.com	1980-01-01	2021-01-01
25410092-9db8-4236-b21f-f523ec5b5595	03b27cda0910607c8c188b926d871b44@old-email.com	1980-01-01	2021-01-01
fec718e0-5fbb-4af1-8e7b-90126d1756cf	a9b34ee074ec3c1dab7b6119eed25499@old-email.com	1980-01-01	2021-01-01
a7ca2c85-6b6f-4c33-b0c8-7222729b1b92	5900fe9b542a0f61f5c057e3ae4e868d@old-email.com	1980-01-01	2021-01-01
61e5a039-edcf-432b-b0f8-4baf2e62a2df	baa5f24aca1c4c49376b65f6aae752db@old-email.com	1980-01-01	2021-01-01
95adb23f-ec80-4a2e-8a24-5cb93e9e79f6	6d6eacc42f461e852308bd2a481aeaff@old-email.com	1980-01-01	2021-01-01
105144c4-caa1-44e5-bf96-3689c9871391	2d5f5b6bd61b95d2e4ae7baf991d5095@old-email.com	1980-01-01	2021-01-01
9bcf4c28-9a93-45a2-a738-efafba48d8da	fd36c82bcda8b600751b8d5e5067d6e1@old-email.com	1980-01-01	2021-01-01
89ae7a0b-153c-4014-84cd-73dfd1383e35	0e37efeddea4b58c3be5332442fad8bf@old-email.com	1980-01-01	2021-01-01
397abbee-b6b5-4007-aa58-184904395971	2eacda599017762220afd99931ed2325@old-email.com	1980-01-01	2021-01-01
27637c47-12d3-480a-83c5-c8c6351de0af	7e9edf43d9d4075faad4f0db6c826f57@old-email.com	1980-01-01	2021-01-01
2cabda50-aad6-48cc-9d54-baf9b66f701f	d7d6af3cb6284ec3359929e3ce34b063@old-email.com	1980-01-01	2021-01-01
777be327-4f2e-443b-bdea-bff4a5696600	3c0b50e082c4b3797fc6da3b8add170f@old-email.com	1980-01-01	2021-01-01
2acb1fe1-72b6-414e-9f96-447029ebaa5b	a94593adea326db29b2cdf87af00c6b3@old-email.com	1980-01-01	2021-01-01
8ae28a3a-7860-4600-872e-4726779452a3	5380c2a2cebad051d23a81d423f73a76@old-email.com	1980-01-01	2021-01-01
2e8be6dd-273d-4325-9c42-391b38024495	d46b3601a09c08ac8ee44ede2a28da59@old-email.com	1980-01-01	2021-01-01
9a77c6f7-6d63-4697-8f52-eb1ee1991e58	6e762f71cc5c047d72c11727b633a048@old-email.com	1980-01-01	2021-01-01
8ae2eb56-4891-4c1f-b969-28d92f8790ae	a78513922811af7d1ee8b518831497ad@old-email.com	1980-01-01	2021-01-01
3cdfccdd-b275-487f-92a9-10dd5e05bbb8	8f463cd5c49e1adadc7af687c969e6da@old-email.com	1980-01-01	2021-01-01
02cebd83-9c10-4c38-829d-8b677461d559	1c2789ccaa79d55a744defb0a2c7a1ae@old-email.com	1980-01-01	2021-01-01
2e842a19-1315-44ff-8fb3-fb4a16a68f96	ec9dbe19b88f5c14f875ef890e1436f2@old-email.com	1980-01-01	2021-01-01
6b152ec1-e140-4550-b63c-7d8ab37df750	1b7413f43257a7175a7bdc6603eec944@old-email.com	1980-01-01	2021-01-01
20a4354c-fc75-40bd-b361-3efcaf88b4b4	aea8698e744171170c80d18b25e2a00e@old-email.com	1980-01-01	2021-01-01
0500a2a3-47a2-4e6f-9321-d303e89ae943	84bcea3ea96312041b85e3c8e7027de2@old-email.com	1980-01-01	2021-01-01
0b78f242-a341-4bdf-a968-2eb0d7640b12	377f154b204b5e654f8081147e266201@old-email.com	1980-01-01	2021-01-01
508a80cf-903d-4ada-a186-7ecb92cbd549	d20dabae2b75c34d4aca8df2bbac40db@old-email.com	1980-01-01	2021-01-01
cf9cfae2-bda5-4431-b300-9a5799a6444c	34a6492c56b7a41702b48da853a811c1@old-email.com	1980-01-01	2021-01-01
5011439d-7efc-46a8-8891-f47aff3331ec	4ee1acddbc94a417fd0db7c0b5dcdfa1@old-email.com	1980-01-01	2021-01-01
6e4c7dd7-be2a-408f-aa48-c3904177b75d	fab83f4767d74c302e764091d696a7e3@old-email.com	1980-01-01	2021-01-01
d2539a67-fc8c-4c84-898f-8719df2a83e7	9411b6035ee9581df20e5b45e58a77eb@old-email.com	1980-01-01	2021-01-01
f0ce423e-d848-4757-8adc-e5b0eef9dc31	aa8cda20bd10364ef18e23cbfc4f5881@old-email.com	1980-01-01	2021-01-01
abe72fa2-e311-44e7-be1e-3bd70f916391	ec4bba398e758357f769f090f0d5562e@old-email.com	1980-01-01	2021-01-01
bbe00978-82a5-4af2-9164-62d8e9d0b815	3eb11818f06d94b34ed2c4f9849317cd@old-email.com	1980-01-01	2021-01-01
d9e508ba-8a92-42a4-8b44-c8332993a228	2a8642c33ff4702bb2ce67fee7f728a9@old-email.com	1980-01-01	2021-01-01
e5c54e19-8368-4d64-ac2f-535ca2db896f	5f60b8eb7ef93322a80dc66f24911448@old-email.com	1980-01-01	2021-01-01
d82ce8f5-fc91-4761-bebf-ec53d5941064	eca0ed77d73ea6b9c2b8c7c1bbb8c8b3@old-email.com	1980-01-01	2021-01-01
841856d3-445c-4344-b9ad-002ec7b8d478	f5b8c285fc7ce0d2754ec806ce5decc8@old-email.com	1980-01-01	2021-01-01
e776fb1c-07a6-4588-a1b6-11cb5d513905	8fdc75310ecc04d0dfaffc27dbc02399@old-email.com	1980-01-01	2021-01-01
d6e934f8-5c8a-4815-94ff-fc0fa8d228a2	8357b4db5848a8e0854b4fe493f1d506@old-email.com	1980-01-01	2021-01-01
914fd568-201d-4572-9961-d209067c9fc4	34d9600e72a3181dc7dc43d37c291f89@old-email.com	1980-01-01	2021-01-01
bca2716c-c467-4e1c-a781-c148a33123dc	64b5e4a9eae97c7e4c4159eaa2fbd78c@old-email.com	1980-01-01	2021-01-01
4e5bc1fa-faad-4724-af8c-7b08b09b908c	5c77fbb2a959b859e87d9d82334be052@old-email.com	1980-01-01	2021-01-01
10d57d8b-08dc-470a-8382-11240bd68dbb	4864ac5244fca5cf55a5d1654473a5ca@old-email.com	1980-01-01	2021-01-01
89524c77-943a-4cb3-a700-fc283e01e559	ccf14bbc43252aaeb80feb7a4a3259e3@old-email.com	1980-01-01	2021-01-01
994eb259-ed4d-472f-b00d-103aa49c6df6	f730444e6bbb690335a8ce2b08b3dc63@old-email.com	1980-01-01	2021-01-01
53065b0a-bca7-41d7-9c5f-8d69d2553175	c4607a64e502678be51b2772d35c5a73@old-email.com	1980-01-01	2021-01-01
63835900-3e0a-48f5-9da1-fa94ca4170a0	0d03605f3fe9dcc4d1467a2e9dd23ccb@old-email.com	1980-01-01	2021-01-01
bb5af1cf-fb45-433a-a503-875e5c915a34	f302d0e8b5f61f79d9548192e9433873@old-email.com	1980-01-01	2021-01-01
4e5e7490-68ca-47a4-b24f-8eee61b5c0f5	3ca502e2894334922aa2a69f6049f2d2@old-email.com	1980-01-01	2021-01-01
6a54a81f-9d86-431d-a656-6f65b96034c1	f75563f6f078deb7c3224805b1676e68@old-email.com	1980-01-01	2021-01-01
12799f7c-2fb6-4963-b8ae-cc396d8fcdec	f7775a5f48c408606a1a80166c368432@old-email.com	1980-01-01	2021-01-01
10858561-968c-4960-9611-bccdd1e1874f	4096826446fe9d6973a87ed2ddfe3a80@old-email.com	1980-01-01	2021-01-01
554fce21-45d4-4427-96ba-698358621503	3ef7573b65e6c0d710f44a3e97cefce8@old-email.com	1980-01-01	2021-01-01
1a40f77a-d5b4-425c-b149-f5c24f1d589e	2fa9a8538f54ee03c0e89f2e33dedc4f@old-email.com	1980-01-01	2021-01-01
5ea13d9c-9e4a-4f79-845e-6196ee95764a	4a5100cfb170293ea23bb6441e19ae1d@old-email.com	1980-01-01	2021-01-01
4a504680-7f0a-4986-9ca8-4fd2e1e04678	b57887f692e25311a19d7a2e8f468b83@old-email.com	1980-01-01	2021-01-01
3f6b9aa5-14db-4375-b180-a7f3658daa9a	93a85a3eafd4078a60aece7392e9bfe4@old-email.com	1980-01-01	2021-01-01
00d50f4c-b221-439a-abb1-04c4314eed4e	f0572f7be67332bd22995230653df950@old-email.com	1980-01-01	2021-01-01
91600967-ce99-45fd-8b4e-cafb3e89ef8f	19a088443e8d85462cec0d5d815750f8@old-email.com	1980-01-01	2021-01-01
e0be8dd3-b0d1-416c-8149-e617de4233ac	22ab05e183726decedb65d168c7eef3e@old-email.com	1980-01-01	2021-01-01
d7454549-330b-4e29-b302-15358d095e90	e279628db543384879366a9d100c37e2@old-email.com	1980-01-01	2021-01-01
2d467155-679e-4968-a43d-e3a1ec3120cb	e1813ba6feda565d89c3c13c4772b918@old-email.com	1980-01-01	2021-01-01
2a92c159-7303-439e-a492-e252b43000aa	aa2d724a146c9f18ebb6aa90ded4ff82@old-email.com	1980-01-01	2021-01-01
4eb469b5-3d05-4fd8-94fa-5081a846eb9c	d1d3b7e2807395dde496bb5df98c6598@old-email.com	1980-01-01	2021-01-01
97c61aed-eb38-4690-ac83-7aebfa7a471b	98b2693af809bf4384ec643c1c65db30@old-email.com	1980-01-01	2021-01-01
07aea3fa-3e10-4e95-a8f7-49625e9d903a	386825d504dbd3e154f097901ff2031c@old-email.com	1980-01-01	2021-01-01
71dd3515-99ee-4c3c-b9b0-3bcb8a8347e4	26739b95e5477c732b865a6759e6c016@old-email.com	1980-01-01	2021-01-01
4aca6945-2e0a-451f-95a7-f0b949ecf9e0	d638dec9f005eba556475be9d9ff46ce@old-email.com	1980-01-01	2021-01-01
0e9aa107-d89d-474b-afdd-0b42fb28facf	58372ae89478f1ab4e00666232edef4d@old-email.com	1980-01-01	2021-01-01
695de9c7-a72f-43b1-8ced-66fe2f62b4bb	8272156535eaa49a2eaaf98c598244f9@old-email.com	1980-01-01	2021-01-01
d5c4cebc-f69a-4129-8264-53ff689c635a	cb3f7f1107d193fcf89304a7e7b06769@old-email.com	1980-01-01	2021-01-01
c3f0200d-d685-441a-9d21-4ba900b6bacd	73d93285f29692b37cf64dfeb8dd1cfb@old-email.com	1980-01-01	2021-01-01
7fffe91a-70ad-479c-8127-a2785af8b89b	ce37228a8d45d4c495fd07f5cba7656f@old-email.com	1980-01-01	2021-01-01
0ff57fab-1522-4576-9d2e-ea70776d8c75	ea119bf21d377e0c14ff2ee09eef98bf@old-email.com	1980-01-01	2021-01-01
72210c5b-2c4a-446f-ad1c-ce2ea314ebe9	ad1fc7a0d50943ac6445fc0a789095bf@old-email.com	1980-01-01	2021-01-01
89bac566-6b22-493c-85eb-b9d3c3d1e1c7	ed04e65d66bc3ccd459833b11c0fd053@old-email.com	1980-01-01	2021-01-01
2dfd1d28-7286-4540-b46b-e35353a75483	42afaec12eccf441e74a96a82adaa98c@old-email.com	1980-01-01	2021-01-01
0ef34842-f349-49ea-9be1-5139eaa297e1	b3f2e153a9c99662afa9bd4c53287b1e@old-email.com	1980-01-01	2021-01-01
d595f810-0358-433d-8006-447b14c3363e	34652b79b4d910df05b47ccf1d6b2156@old-email.com	1980-01-01	2021-01-01
082edc25-92e2-485d-8d1a-b6ce5e48791c	8273ed03d37fa58ca63720498c300c3c@old-email.com	1980-01-01	2021-01-01
10d71cfd-7e41-4401-a2d9-18422eb8fc5e	0bb18244a4464fe7e252830da214a999@old-email.com	1980-01-01	2021-01-01
a9e756c5-62b3-49d5-a7c2-0a9212195c81	2e43f4b8ec7f163dc2e083f6f5058f29@old-email.com	1980-01-01	2021-01-01
82062112-8052-4896-88c3-82731f926549	19763a9f106a00fb23cda4e81187a276@old-email.com	1980-01-01	2021-01-01
cae9d039-fb92-4976-9950-b1bb4e32dfab	25e8fe3b4a800b640e61a66c6a8e4f1c@old-email.com	1980-01-01	2021-01-01
4d2de49c-8219-468d-a537-31dca77d2e29	35fb4c63f54c146413c246b074b7a604@old-email.com	1980-01-01	2021-01-01
489af7f8-d8da-487e-acd2-535d1b4257c7	80b06405b4c0eff570085e77d5425569@old-email.com	1980-01-01	2021-01-01
f7761013-a180-4a2d-8950-d967d4651310	0bd174a51116d051740c282b95967ea3@old-email.com	1980-01-01	2021-01-01
fcb2ffb4-d0ad-48c2-8947-9761b1502f47	b5fddf9b57735f3fcd806ec88fd0ada3@old-email.com	1980-01-01	2021-01-01
e09c2352-23be-4434-bc14-9d33f4e0ecd8	5c0fb82168fa3546ad820bac603b4e93@old-email.com	1980-01-01	2021-01-01
2964065c-9b56-46ee-804a-bfb9d987ac69	32f32fc7be81dc2acd515b9f828ee168@old-email.com	1980-01-01	2021-01-01
f4ec000f-9682-4da1-acfb-6940b558c478	839f5b36394e77d635f81f5cfe5de7c5@old-email.com	1980-01-01	2021-01-01
\.


--
-- Data for Name: user_session_logs; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.user_session_logs (session_id, user_id, login_time, duration_seconds, device_type) FROM stdin;
1bda6095-63fe-4c7b-972f-e2c3385cdb16	6c23bfa5-cdd5-4cb0-9beb-8dc305a0833e	2025-11-09 10:41:19.458212	803	iOS
374b7dbc-df31-445c-97c5-e5ff3c797052	c590b29a-8398-4e2b-bc34-5e35a299a553	2025-11-05 00:55:54.077688	180	Web
bc1022c2-7eb5-4d63-aa7f-ddf85647419b	b285e4ac-e28f-42bf-846c-3969bcc99726	2025-11-11 10:07:11.801007	291	Android
cb9c5019-efda-427d-9588-719e668cd9ce	f5c609fe-23a2-4c72-8bd2-954607be3d9b	2025-11-07 05:21:53.816492	571	iOS
1cebf0c9-18f9-4d92-94e0-af7f808058e9	44d33619-b42e-4a5e-adbc-aad67fc945a4	2025-11-11 04:52:35.617242	914	Android
0555aaa2-e6bc-442e-bd1e-e3d32aa4d54d	d6333684-3d1d-40f9-9d72-37eb8c86cd85	2025-11-06 05:35:35.202595	682	Android
5ce89286-61fb-43df-af11-15d5ef1ce41e	a13d0620-71c0-42de-a39b-a20a4fade955	2025-11-03 16:32:22.191603	525	iOS
b70e1005-9122-4ebb-8e20-ffdb67cb7e57	75e64199-0138-44f5-8194-6260f7d053de	2025-11-02 13:53:04.918387	184	iOS
9ad74c24-34e0-4f73-bf1b-28cc668a8293	6abbcdcd-335f-49de-984e-c894267486fe	2025-11-08 17:38:13.941374	750	Android
75fdcea5-0ee6-4d27-9eb8-91e9d35ab0ca	bd93f95c-5045-4d95-be02-36c3c8134349	2025-11-04 22:41:00.92046	923	Web
d44ef880-7364-405d-998d-1029b0de137b	1a011443-b403-45ff-ba6f-0bdd2ac33303	2025-11-05 01:42:59.095807	539	iOS
84f77c3a-c6e6-40bb-9608-2aa32e983493	678504f1-1f83-4099-b4d2-b200208f8d9d	2025-11-10 22:09:44.267973	250	iOS
b8118f1c-f4dc-40a4-ae48-ddde3b68995e	439a6e44-7e71-4f0c-9300-5deab4897b7e	2025-11-09 08:36:38.774314	255	Android
38ca3985-d369-4c74-8943-2d9b567bf3cd	ef5f9b01-ab3b-4bef-b66e-587f4d9207c1	2025-11-07 22:03:10.607415	536	Web
680975b7-0e5e-4635-99f8-24b70bbd6ca3	3ba65982-5873-4cfd-b1b2-bf50da44f3c0	2025-11-05 00:35:00.338944	330	iOS
ed3b7157-78ee-4ebb-b89a-e02619d40646	ac4d6a45-6a83-4031-bda0-48e86c2ddf65	2025-11-07 20:26:13.898135	478	Android
d2ab9778-4324-48af-8148-ec5078858399	6199c6ee-5e40-4d60-9c4d-f92db0fbf58b	2025-11-07 02:42:00.317433	221	Android
7a6ef454-797b-4525-9fde-6e6b5c4b8991	b0bb8f53-5ff5-42c0-804f-9a98f3a37976	2025-11-08 19:58:20.21009	219	Web
73a91414-7d2e-4caa-acda-51cab0ce7171	4eeb7f0a-0838-4cb4-a970-e3ca6f0ecfef	2025-11-06 06:30:52.091421	291	Web
f5a0336f-5753-428c-b46d-2936b827bf67	d5a883b8-8f3a-48f6-a0c8-7aa6f3fc4b86	2025-11-05 15:36:24.664611	953	iOS
b150971a-a131-4fce-ba5c-ffe7265773b0	3ed769a3-22d9-4e6b-ae8e-eb6445c3a539	2025-11-09 08:43:27.784711	473	Android
3bf50072-2625-4c6f-9131-f1bbf8173ba2	368b46c6-6933-47ba-a446-896a8ea41833	2025-11-06 22:45:40.246096	323	Android
c3126a22-7e61-418f-891b-173b9e9ce2bd	593d3a6b-fd60-4640-a52f-d38bdd093755	2025-11-03 21:37:19.396317	785	iOS
294f572d-ba0d-43e0-9953-429c1eb7b874	c821c01a-f0ee-4dfb-af53-6a1c2af98577	2025-11-04 04:20:06.528042	463	iOS
151ce553-d23f-4bf5-a0c0-e45e99889390	1f719f08-68f8-444b-8ab2-81ff141167a7	2025-11-01 20:15:17.486005	747	iOS
c0e7763a-e3e4-4c34-86ca-e649ce968d2a	127c4e1f-7958-4ad8-a14a-701b6ef1796c	2025-11-03 08:10:23.958211	127	Android
23ecb6f5-e015-498f-bf60-90d5fe818518	8b6485bb-4dae-44c9-a8fa-18518457247a	2025-11-03 20:05:47.563355	783	Android
3fec8f9f-7ec4-4730-a6d2-2d1f7263d2b3	98eb387f-c87a-4fcd-bfa5-2fa849ba57e1	2025-11-09 13:05:36.07853	749	Web
06b2b1fe-74d0-4481-9230-b06866b5b9c2	6394ae0d-9572-4c3b-82cf-4952d5bb2063	2025-11-07 03:57:49.242228	238	iOS
d56246a5-56f4-40b8-9a76-a0fb54840aab	2ea9c250-3dae-4627-9fe8-f85c2bd3562f	2025-11-06 20:44:23.914382	954	iOS
19c0af8d-b292-412a-99cc-e7a734f818ae	23264504-1c4b-43ec-9df1-ec7e55d00050	2025-11-08 15:26:00.852665	773	iOS
ccd1461f-3b7a-4b0e-b86c-3c7787eb0217	2f5e25a8-c1e4-400a-8886-e54ac7b060ec	2025-11-03 23:22:14.644732	689	iOS
ee5b3638-2672-48dc-9179-41c1b3bbb2cd	7208cf9a-6b15-4fec-840e-2fe9bc1d9d64	2025-11-01 18:15:16.085333	579	Web
eeb3c9f9-a3ee-42df-a213-016bfe335896	2b5259ef-8b35-4d26-a125-b9c060eb850f	2025-11-10 21:31:33.345531	721	iOS
b3062590-0747-49f2-88ac-029e121544c1	24598e18-63e2-4861-ad55-d2c79971a795	2025-11-09 10:28:19.714211	451	Web
dfeb42fc-edf8-41d2-b9c4-96657759575e	eab8ce77-f749-4616-8e9e-ce2f07b473f2	2025-11-10 03:29:43.241223	685	Android
becca658-35d3-4b30-ac11-7861c26e2912	e503347b-17f8-4688-9e3a-dcc97cf489f3	2025-11-07 00:54:36.946648	564	Android
226d57af-2734-4c05-b05c-5d43796046b0	ec92b01c-f304-4df5-8f86-0cc765724e81	2025-11-02 06:30:16.831497	303	Android
7942aa25-ab09-4ecc-a645-6c73f1b5de21	9d9ada1d-3520-444d-a10a-bc66f1b5f612	2025-11-06 01:44:00.076286	570	Android
928a31be-54f3-4878-a645-d3449db33b01	38f52091-c6ba-4f02-a0c1-dbe1d61a6465	2025-11-07 16:08:53.833333	653	Android
383f44d7-1d23-434d-9337-689f9e59331c	b6ea1038-dc90-4317-a335-ec5562799390	2025-11-11 04:37:37.274316	784	iOS
47517670-ecd7-4e08-a5b5-b535382a8ec2	100adcec-f01f-43c8-ae85-c207257a35b3	2025-11-08 19:14:39.366679	881	iOS
a5d5f6aa-6ab9-4077-b63b-c7ac626ed521	e85e3622-5787-42e9-b4b0-745775ff9e8c	2025-11-06 02:10:24.84428	253	Web
bb8e5588-ddaf-41f0-9dbf-5ee29b72d074	6a6af5e3-9759-451d-b20e-901b5a54fe6b	2025-11-09 08:19:29.824645	657	Android
51009594-d38f-4013-af6e-fecaf78650c3	1cbe1009-dc6b-476c-a942-1eada1e02180	2025-11-07 21:34:02.838597	453	Android
9656dc53-fc61-42bc-8d9f-a219672588b5	bdbc158e-2781-4c29-900f-efba29a68407	2025-11-10 08:39:16.921935	188	Web
4d5653ee-1e14-4d65-9d2d-42e1fe745ce5	7fa0ff8b-7478-48b7-b919-5c47bd4c69ad	2025-11-10 02:48:47.663506	499	iOS
438336e6-b341-4cb3-b757-64c91513c750	224357c9-f884-4214-9e28-c91eb01c8a50	2025-11-09 14:53:42.759362	786	Android
f6d817bf-9690-42f1-9dbb-264d193076f6	9a856255-418d-4056-9c6d-f2f30674713a	2025-11-03 22:56:32.01556	267	Android
0b5a9777-b012-4ede-a33c-250c1aab15ec	b205fa65-98ab-4712-bedf-dc3ec661f11d	2025-11-08 05:56:00.064266	299	Android
afb5ceed-8d41-47d5-92f1-4b2a0318eafa	d0ddc5df-b8e5-4fdd-8230-4f125b44de6e	2025-11-11 01:54:54.486539	668	iOS
808a2055-6b46-4ba5-ba87-25eca1a9789b	70df0463-6714-4fd1-a6a4-ec10fc8f76ed	2025-11-07 06:27:05.990717	534	Android
1e936ae4-1d2a-4d61-bdad-71bc8b2eb9ab	bc8193f5-428c-4b0f-b297-aa15b726fbcd	2025-11-10 08:33:13.158956	562	Android
486b3e58-255c-4a1e-8f42-3d892b7f8e17	969bfea9-5829-4bdb-bbb4-1fca2377d987	2025-11-03 11:19:34.762508	681	iOS
61cf0847-1d26-45b5-b598-a70934c8bb7c	d2333e1d-d8f8-46b1-8394-9c2ef8a0a851	2025-11-07 09:00:54.430764	450	Web
03cdc923-17e4-4da5-b101-748aaaa69b9e	7fa96180-dcaf-4b86-817a-b89d5cfd6cf5	2025-11-03 04:11:14.10782	193	Android
a35f0196-5450-476b-a67c-a5139800cd25	49e538ae-c7ce-4fcb-a6de-c6f88fee0f12	2025-11-09 07:53:54.967889	939	iOS
9505c87e-707e-49e2-b59f-bd353d1570ea	a72fac33-e237-4385-9020-43a11618e23f	2025-11-07 12:03:20.619768	958	Android
7cf702aa-22d6-409a-837d-dc9b3754ec90	e2deba89-e0a1-4aa3-8b3e-f24c82460243	2025-11-02 13:58:27.422187	253	Web
b409c701-74ea-48ae-903a-39e7fb3a03da	03d6cf18-db69-4010-b15d-ce7986976811	2025-11-02 23:08:50.31548	126	iOS
d859776a-8ddd-4abf-8c49-23b7f992e2c7	1cd15608-9510-4597-8c46-468e4893d4d2	2025-11-10 20:59:41.290983	352	iOS
eda77c96-1c79-40f6-b231-4b27dd6e4617	f788c29d-e04c-4bd3-93af-0cf3e3dac2de	2025-11-02 06:14:53.462026	302	Android
cfb7ef13-7d28-4ee4-b102-ab9194b3baf0	3f4a6ff2-976e-4998-afbb-53068e9a7a78	2025-11-02 11:46:27.714913	230	Android
854ae8ac-2257-42f6-a1ba-7c589a9dc0c4	a0139154-9688-4cc9-8785-ff4d119c8e3f	2025-11-02 08:29:11.956254	608	Android
0687a629-fb04-4996-9aa0-5d43b60fcae0	7fd3f1ee-2c1a-4cbd-a28a-28b30fba755f	2025-11-09 18:54:12.121287	544	Android
df9ee999-5600-4df4-a79f-f54d9b65007f	432b53d4-5d39-4f75-a6cc-62e02fa44d0d	2025-11-08 06:55:01.747799	328	Android
c616932a-c21f-49ac-9660-8f046fc89781	ce734637-4347-4e0e-b87b-58a1f69c27d0	2025-11-10 12:40:15.828241	802	Web
4ce680f0-e834-4309-9a3d-3fd859174ee4	4ad1ee7f-4498-42b4-8408-1fab2bca7d9e	2025-11-10 22:37:02.429434	493	Web
0ca23f06-6d10-4806-ab78-6d01172007d1	2be935d9-1383-4ab3-b378-5a474f928fd4	2025-11-02 02:15:40.630512	152	Android
1ea52f0f-0db3-4882-ba42-da935d80df38	15219739-72cc-4059-bc54-197b8f48642c	2025-11-03 23:39:12.71273	841	iOS
10e7b343-87ef-4a06-b7b0-91cc8371f9e2	d03e2155-573a-4078-9a41-1d52f57f33fd	2025-11-11 15:40:53.384816	150	Android
a5aee008-4402-452a-8531-a3e4644f816c	1609eea3-48a9-4b56-a693-1ee8bcdd4302	2025-11-10 10:53:39.988086	674	Android
bb1118e7-bab1-4521-bb1d-a6027c5af949	fa021ed4-03d2-4791-b04f-94d99a51af38	2025-11-08 15:54:15.075209	84	Android
3998652f-3a11-4d37-ac48-0a4906a8ef54	3ed56577-5428-4b53-8ff4-9f8a2d29d997	2025-11-07 11:08:42.618936	632	Android
d8653efa-be26-4240-9a73-d663f32bbaac	49adbee4-232b-4ba0-8990-87a49a7c93a2	2025-11-11 11:56:29.74872	923	Web
e12b0fde-76cc-48b4-a81e-65fb391cc19f	1c973d2f-fa4f-487e-a3b9-c72ecbd871ec	2025-11-10 03:30:18.61447	280	Android
db16a33e-3a29-44fc-970f-9d9bf1e24dda	91796275-06c8-4ecd-adff-00df4c29fb91	2025-11-09 23:17:50.681755	736	iOS
77101ab2-d933-40c6-9791-70e2d38987fc	72422fdc-71b9-42d2-b652-f30c87f5a423	2025-11-08 17:03:50.551721	202	Android
67a2f38d-5d14-4342-ae8d-a47ebedbd40c	5f0f5bc3-485e-49c5-ad3f-3860ed838dcf	2025-11-10 12:34:51.761949	660	Web
f88fb43f-0894-46b1-b22c-b90198624301	0b41a582-27ef-4c9f-9ce8-87703d09dfbe	2025-11-09 09:49:37.31903	701	Android
6ca84e48-9778-4f7a-b625-4422ea337462	f195dbea-4edb-4584-8157-76615f92bfc6	2025-11-03 07:14:11.06611	828	Android
1c38f7a0-120e-4f26-9a91-bba03636378b	b02e3a08-e87a-4f67-b20e-c1f904306ee6	2025-11-10 07:59:06.701626	591	Android
635dfa9e-bcef-48b8-bff5-f4b1659ac9a6	cdeec7d1-5c26-438a-a214-9dcd05a4084d	2025-11-03 14:04:48.198043	897	Android
7b9790db-92e3-47a7-bb8b-c0a872bf7aae	b51b155f-d6c9-4d05-827c-c13d148a14e8	2025-11-04 12:35:06.656976	182	Android
e3cc3e29-05b4-43d1-aba7-3f9874977205	f365d609-2788-4bc4-bb7d-dcca0d04eb90	2025-11-09 04:07:37.086928	726	iOS
614a45fb-a476-4111-964b-b4e3e7181b12	6422173f-c333-437f-a682-58e0b0676a02	2025-11-11 07:56:12.448444	425	Web
29b1016f-af46-4d48-8478-feb010982593	07b74722-ce29-4fbf-8df8-ef90106ab37c	2025-11-09 20:06:53.225648	457	Android
c0b8ade7-ea4e-4bda-bd85-b68cd35b9e24	a34c5a99-6c1d-4e66-a82e-bc69c6d395b6	2025-11-07 06:05:40.765932	571	Web
60327d55-c26e-44b9-9bb7-9a5c046e584c	889a82d9-f60d-448c-b6aa-3521395ccc57	2025-11-08 09:55:14.018134	232	Web
08ed45b9-c5dc-42fa-9fb6-22c557289599	bc083131-5553-4140-9a7e-ad35640e59d1	2025-11-11 07:26:11.347216	135	Android
c8181e3b-c156-4bee-9ab3-302b7e0e06bc	8158971f-da6d-4e26-98a4-06ef767dd6b2	2025-11-04 11:46:06.20823	916	Web
d7e0d48c-04d7-44f1-a8a9-b20c49648350	9a0244fa-3c83-4844-8bd8-a82a92f41846	2025-11-06 00:32:47.611501	507	Web
48d6bc6f-ac13-42e9-abac-42557f31c6cd	aa509ef7-20d5-4930-a255-8a7b5253a4a8	2025-11-11 10:42:22.382645	703	Android
5feb63e6-da31-4b3b-8785-c4852cf98dac	d9e7ee2a-6766-4e45-a1cb-06fe3cf65b56	2025-11-09 10:00:15.657681	406	iOS
02591363-d29a-41ac-a3da-64be91f5b709	33f4f2f2-ff57-427a-b2a2-ef51fbbd68cb	2025-11-08 09:23:59.795484	379	Web
9dbeb0f0-d3aa-4b77-bb91-d6196b05efe4	6042e7f2-b972-4843-9a45-44862fe9c795	2025-11-05 21:02:06.195939	486	Android
8188fd16-9d3c-4487-b1b1-73edcb92bb2d	3ad7b170-f2ab-4eb5-880a-451488873a50	2025-11-03 17:53:58.943843	725	Android
9ca1a20c-e2ac-49da-b4d3-00a10100a053	1d21e1e2-8521-4f78-98fa-1d157a10fdd4	2025-11-05 09:26:25.302937	337	iOS
b6873849-c4f9-4b5c-bdfb-9398f1fc537f	75c43cdd-da8c-4e7b-a5a7-ca86ccb699ab	2025-11-10 17:20:26.244997	276	Android
0570ba9e-30ae-4176-936f-a19b626d972f	79c9e112-5230-405a-9e81-a8ee4d459702	2025-11-03 05:10:51.536322	263	Android
\.


--
-- Data for Name: vehicle_gps_tracking; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.vehicle_gps_tracking (ping_id, vehicle_id, ping_time, latitude, longitude) FROM stdin;
1	V-110	2025-11-11 17:33:43.039492	48.8566	2.3522
2	V-168	2025-11-11 17:37:23.891521	48.8566	2.3522
3	V-157	2025-11-11 17:43:58.570738	48.8566	2.3522
4	V-123	2025-11-11 17:03:57.980192	48.8566	2.3522
5	V-196	2025-11-11 17:02:46.765934	48.8566	2.3522
6	V-124	2025-11-11 17:07:52.716191	48.8566	2.3522
7	V-103	2025-11-11 17:17:46.367502	48.8566	2.3522
8	V-111	2025-11-11 17:49:49.930047	48.8566	2.3522
9	V-161	2025-11-11 17:54:06.779927	48.8566	2.3522
10	V-152	2025-11-11 16:55:57.052703	48.8566	2.3522
11	V-133	2025-11-11 17:43:52.338812	48.8566	2.3522
12	V-155	2025-11-11 17:46:09.688706	48.8566	2.3522
13	V-115	2025-11-11 17:37:06.75698	48.8566	2.3522
14	V-104	2025-11-11 17:00:42.912075	48.8566	2.3522
15	V-120	2025-11-11 17:01:17.919282	48.8566	2.3522
16	V-198	2025-11-11 17:53:22.199617	48.8566	2.3522
17	V-109	2025-11-11 17:48:05.64787	48.8566	2.3522
18	V-160	2025-11-11 17:35:34.31742	48.8566	2.3522
19	V-146	2025-11-11 17:43:00.682632	48.8566	2.3522
20	V-126	2025-11-11 17:03:10.345341	48.8566	2.3522
21	V-187	2025-11-11 17:28:32.683134	48.8566	2.3522
22	V-148	2025-11-11 17:54:38.709851	48.8566	2.3522
23	V-143	2025-11-11 17:33:58.640063	48.8566	2.3522
24	V-174	2025-11-11 17:40:44.991216	48.8566	2.3522
25	V-137	2025-11-11 17:04:20.332336	48.8566	2.3522
26	V-117	2025-11-11 17:29:01.510262	48.8566	2.3522
27	V-153	2025-11-11 17:36:17.536416	48.8566	2.3522
28	V-159	2025-11-11 17:18:35.540789	48.8566	2.3522
29	V-176	2025-11-11 17:05:21.495392	48.8566	2.3522
30	V-155	2025-11-11 17:22:50.013154	48.8566	2.3522
31	V-123	2025-11-11 17:03:37.023341	48.8566	2.3522
32	V-140	2025-11-11 17:28:08.141565	48.8566	2.3522
33	V-143	2025-11-11 17:33:52.023554	48.8566	2.3522
34	V-177	2025-11-11 17:53:09.738007	48.8566	2.3522
35	V-137	2025-11-11 17:19:25.004927	48.8566	2.3522
36	V-140	2025-11-11 17:07:09.239946	48.8566	2.3522
37	V-192	2025-11-11 17:41:05.077612	48.8566	2.3522
38	V-164	2025-11-11 17:41:44.31751	48.8566	2.3522
39	V-108	2025-11-11 17:30:14.952631	48.8566	2.3522
40	V-160	2025-11-11 17:19:08.054343	48.8566	2.3522
41	V-181	2025-11-11 17:39:24.157068	48.8566	2.3522
42	V-153	2025-11-11 17:40:55.4127	48.8566	2.3522
43	V-148	2025-11-11 17:14:40.554946	48.8566	2.3522
44	V-112	2025-11-11 17:31:29.867253	48.8566	2.3522
45	V-128	2025-11-11 17:10:55.6222	48.8566	2.3522
46	V-147	2025-11-11 17:29:14.797717	48.8566	2.3522
47	V-119	2025-11-11 17:36:05.0589	48.8566	2.3522
48	V-153	2025-11-11 17:27:24.691618	48.8566	2.3522
49	V-187	2025-11-11 17:44:25.113099	48.8566	2.3522
50	V-135	2025-11-11 17:37:53.567076	48.8566	2.3522
51	V-140	2025-11-11 17:30:56.924113	48.8566	2.3522
52	V-109	2025-11-11 17:47:43.772448	48.8566	2.3522
53	V-174	2025-11-11 17:42:08.017557	48.8566	2.3522
54	V-102	2025-11-11 17:05:02.881862	48.8566	2.3522
55	V-187	2025-11-11 17:52:09.462116	48.8566	2.3522
56	V-118	2025-11-11 17:14:34.99301	48.8566	2.3522
57	V-197	2025-11-11 17:38:14.311948	48.8566	2.3522
58	V-120	2025-11-11 17:07:46.687038	48.8566	2.3522
59	V-115	2025-11-11 17:18:43.567067	48.8566	2.3522
60	V-172	2025-11-11 17:01:12.290405	48.8566	2.3522
61	V-142	2025-11-11 17:49:51.023895	48.8566	2.3522
62	V-193	2025-11-11 17:46:21.347295	48.8566	2.3522
63	V-158	2025-11-11 17:22:18.722049	48.8566	2.3522
64	V-138	2025-11-11 17:51:55.186069	48.8566	2.3522
65	V-128	2025-11-11 17:22:09.94779	48.8566	2.3522
66	V-160	2025-11-11 17:10:30.147941	48.8566	2.3522
67	V-184	2025-11-11 17:24:35.430107	48.8566	2.3522
68	V-160	2025-11-11 17:07:37.067653	48.8566	2.3522
69	V-125	2025-11-11 17:23:21.836965	48.8566	2.3522
70	V-167	2025-11-11 17:53:43.53841	48.8566	2.3522
71	V-105	2025-11-11 17:26:29.687576	48.8566	2.3522
72	V-122	2025-11-11 17:50:42.345654	48.8566	2.3522
73	V-183	2025-11-11 17:39:00.471506	48.8566	2.3522
74	V-137	2025-11-11 16:55:40.301798	48.8566	2.3522
75	V-143	2025-11-11 17:21:31.305589	48.8566	2.3522
76	V-143	2025-11-11 17:44:07.350323	48.8566	2.3522
77	V-108	2025-11-11 17:30:40.25219	48.8566	2.3522
78	V-161	2025-11-11 17:12:25.12292	48.8566	2.3522
79	V-187	2025-11-11 17:26:43.748414	48.8566	2.3522
80	V-185	2025-11-11 17:40:18.12743	48.8566	2.3522
81	V-182	2025-11-11 16:55:13.860814	48.8566	2.3522
82	V-109	2025-11-11 17:34:27.080956	48.8566	2.3522
83	V-140	2025-11-11 17:17:19.173231	48.8566	2.3522
84	V-174	2025-11-11 17:42:26.569237	48.8566	2.3522
85	V-180	2025-11-11 17:03:37.248289	48.8566	2.3522
86	V-154	2025-11-11 17:29:39.767132	48.8566	2.3522
87	V-168	2025-11-11 17:46:23.575382	48.8566	2.3522
88	V-140	2025-11-11 17:46:15.894857	48.8566	2.3522
89	V-164	2025-11-11 17:49:16.394189	48.8566	2.3522
90	V-181	2025-11-11 17:15:13.523213	48.8566	2.3522
91	V-163	2025-11-11 17:05:34.766969	48.8566	2.3522
92	V-197	2025-11-11 17:32:08.258911	48.8566	2.3522
93	V-158	2025-11-11 17:07:35.812481	48.8566	2.3522
94	V-102	2025-11-11 17:43:27.350393	48.8566	2.3522
95	V-123	2025-11-11 17:50:47.165802	48.8566	2.3522
96	V-120	2025-11-11 17:38:50.316514	48.8566	2.3522
97	V-153	2025-11-11 17:27:16.217845	48.8566	2.3522
98	V-109	2025-11-11 17:03:37.801603	48.8566	2.3522
99	V-160	2025-11-11 17:31:22.193857	48.8566	2.3522
100	V-138	2025-11-11 17:13:55.776415	48.8566	2.3522
\.


--
-- Data for Name: weather_forecast_hourly; Type: TABLE DATA; Schema: raw; Owner: postgres
--

COPY raw.weather_forecast_hourly (forecast_id, city_id, forecast_time, temperature_celsius, precipitation_mm) FROM stdin;
1	2	2025-11-11 18:27:06.30689	10.4	2.0
2	2	2025-11-11 19:48:59.909632	10.0	0.3
3	4	2025-11-11 22:59:56.308543	17.2	0.6
4	4	2025-11-12 08:44:18.557107	8.8	0.2
5	3	2025-11-11 21:31:28.158569	17.3	1.0
6	3	2025-11-11 19:44:45.02641	23.0	0.8
7	2	2025-11-11 22:28:00.198981	9.7	2.0
8	3	2025-11-11 19:50:10.222859	23.0	1.3
9	3	2025-11-11 19:44:15.979389	16.0	0.5
10	2	2025-11-11 21:32:58.662128	8.5	0.8
11	2	2025-11-11 21:59:29.714679	9.4	0.6
12	3	2025-11-12 00:12:10.408979	22.5	0.8
13	3	2025-11-11 22:59:11.014352	11.9	1.7
14	3	2025-11-11 21:15:36.627047	10.2	0.6
15	4	2025-11-12 13:08:36.714764	14.4	1.9
16	5	2025-11-12 00:52:08.955986	7.8	0.8
17	3	2025-11-12 03:18:28.155208	21.3	2.0
18	3	2025-11-12 15:01:16.71048	6.9	0.1
19	3	2025-11-12 17:27:19.4282	15.8	1.7
20	4	2025-11-12 10:14:54.090858	11.3	1.3
21	2	2025-11-12 11:33:10.140844	10.8	1.9
22	3	2025-11-11 18:52:58.225558	12.5	1.6
23	4	2025-11-12 11:49:12.583862	10.3	1.7
24	2	2025-11-12 09:44:22.924549	17.0	0.1
25	2	2025-11-12 03:36:11.864441	19.8	1.3
26	4	2025-11-12 15:09:45.955246	5.1	1.2
27	3	2025-11-12 13:59:38.816852	16.6	1.5
28	4	2025-11-11 22:59:23.352215	6.7	0.4
29	4	2025-11-12 12:36:55.320524	5.5	0.6
30	2	2025-11-12 11:35:52.544624	7.7	1.0
31	3	2025-11-11 18:00:03.795796	16.4	0.4
32	2	2025-11-12 02:20:32.303106	7.6	0.9
33	1	2025-11-12 09:43:24.208225	11.8	0.4
34	2	2025-11-12 04:34:44.299092	14.7	0.6
35	2	2025-11-11 18:56:48.307131	15.7	1.2
36	4	2025-11-12 16:36:36.954976	22.3	0.0
37	1	2025-11-12 11:12:18.634405	20.7	1.9
38	1	2025-11-12 15:40:05.223493	9.0	1.5
39	2	2025-11-12 10:24:48.378205	19.5	1.3
40	3	2025-11-11 19:34:41.521318	11.1	1.2
41	3	2025-11-12 08:17:56.674972	10.3	1.0
42	1	2025-11-12 00:09:46.03963	7.0	0.4
43	1	2025-11-12 00:13:31.193183	17.2	1.8
44	3	2025-11-12 16:58:48.950717	22.3	0.0
45	4	2025-11-12 17:42:11.459015	18.6	1.6
46	3	2025-11-11 19:05:26.768808	16.2	0.3
47	5	2025-11-12 15:33:09.150998	20.1	0.7
48	2	2025-11-12 15:26:57.167643	15.7	1.6
49	2	2025-11-12 02:37:38.611319	15.7	1.7
50	4	2025-11-12 09:46:33.156543	23.5	0.2
51	1	2025-11-12 09:16:45.667998	8.2	1.0
52	5	2025-11-12 14:07:19.173489	10.8	1.5
53	1	2025-11-11 20:24:02.315429	7.3	0.6
54	1	2025-11-12 13:13:08.382699	14.9	0.6
55	2	2025-11-12 02:02:03.020531	19.6	1.6
56	3	2025-11-12 07:40:31.123577	5.3	1.4
57	4	2025-11-12 01:12:14.710425	23.4	1.6
58	3	2025-11-11 23:38:47.74276	16.1	0.1
59	4	2025-11-12 06:32:03.123754	19.4	0.1
60	4	2025-11-11 22:35:39.941769	19.8	1.8
61	5	2025-11-11 23:43:55.955781	7.1	0.9
62	3	2025-11-12 06:26:55.137039	23.2	0.5
63	3	2025-11-12 10:39:49.416808	11.4	1.4
64	5	2025-11-11 21:32:20.137987	6.7	1.2
65	2	2025-11-12 04:11:05.462305	24.9	1.5
66	1	2025-11-12 10:19:23.204299	13.2	1.5
67	3	2025-11-11 22:10:34.104513	18.3	0.3
68	3	2025-11-12 05:34:37.807025	5.7	0.6
69	3	2025-11-12 08:39:08.878667	20.2	2.0
70	4	2025-11-11 19:28:13.684657	22.2	1.5
71	1	2025-11-12 08:23:43.357042	18.6	1.1
72	4	2025-11-12 12:41:05.630117	7.1	0.8
73	4	2025-11-12 02:10:52.435138	9.8	1.3
74	2	2025-11-12 01:06:51.849204	16.9	2.0
75	5	2025-11-12 00:33:40.28074	8.4	1.9
76	4	2025-11-12 07:22:29.611098	18.7	1.6
77	4	2025-11-12 01:34:52.296014	15.3	0.0
78	5	2025-11-12 04:02:15.653569	15.1	0.4
79	2	2025-11-11 22:58:35.123898	19.2	1.9
80	5	2025-11-11 21:06:57.697342	16.0	0.6
81	4	2025-11-12 16:59:17.040061	14.3	1.5
82	5	2025-11-11 20:38:22.121848	10.3	0.5
83	2	2025-11-11 20:37:28.574428	13.1	1.6
84	4	2025-11-12 16:50:28.213111	15.1	0.5
85	2	2025-11-12 12:25:00.299455	20.3	1.1
86	2	2025-11-12 06:05:39.109666	21.7	1.8
87	4	2025-11-12 02:11:42.730297	21.6	0.9
88	5	2025-11-12 11:47:27.622627	17.5	0.3
89	1	2025-11-11 19:20:50.621568	21.4	1.7
90	1	2025-11-12 06:48:47.965069	10.6	0.1
91	2	2025-11-12 17:07:54.108889	19.0	0.7
92	3	2025-11-12 05:42:06.066985	21.9	0.1
93	3	2025-11-12 08:05:57.161817	8.6	1.7
94	3	2025-11-11 22:10:28.306022	5.8	0.6
95	5	2025-11-11 23:08:27.147811	9.6	1.4
96	3	2025-11-12 02:04:39.23875	14.6	0.5
97	2	2025-11-12 07:43:55.975021	22.5	1.2
98	4	2025-11-12 14:54:11.432687	22.7	0.8
99	2	2025-11-12 08:30:46.627253	16.9	1.5
100	4	2025-11-12 03:41:26.513477	13.3	1.2
\.


--
-- Name: bike_maintenance_logs_log_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.bike_maintenance_logs_log_id_seq', 100, true);


--
-- Name: bike_part_orders_order_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.bike_part_orders_order_id_seq', 100, true);


--
-- Name: customer_support_tickets_ticket_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.customer_support_tickets_ticket_id_seq', 100, true);


--
-- Name: email_sends_log_email_log_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.email_sends_log_email_log_id_seq', 100, true);


--
-- Name: employee_payrolls_payroll_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.employee_payrolls_payroll_id_seq', 100, true);


--
-- Name: employees_employee_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.employees_employee_id_seq', 100, true);


--
-- Name: station_hardware_inventory_hardware_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.station_hardware_inventory_hardware_id_seq', 100, true);


--
-- Name: station_maintenance_schedule_maintenance_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.station_maintenance_schedule_maintenance_id_seq', 100, true);


--
-- Name: vehicle_gps_tracking_ping_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.vehicle_gps_tracking_ping_id_seq', 100, true);


--
-- Name: weather_forecast_hourly_forecast_id_seq; Type: SEQUENCE SET; Schema: raw; Owner: postgres
--

SELECT pg_catalog.setval('raw.weather_forecast_hourly_forecast_id_seq', 100, true);


--
-- Name: api_request_logs api_request_logs_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.api_request_logs
    ADD CONSTRAINT api_request_logs_pkey PRIMARY KEY (request_id);


--
-- Name: bike_maintenance_logs bike_maintenance_logs_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_maintenance_logs
    ADD CONSTRAINT bike_maintenance_logs_pkey PRIMARY KEY (log_id);


--
-- Name: bike_part_orders bike_part_orders_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_part_orders
    ADD CONSTRAINT bike_part_orders_pkey PRIMARY KEY (order_id);


--
-- Name: bike_rentals bike_rentals_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_rentals
    ADD CONSTRAINT bike_rentals_pkey PRIMARY KEY (rental_id);


--
-- Name: bike_stations bike_stations_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bike_stations
    ADD CONSTRAINT bike_stations_pkey PRIMARY KEY (station_id);


--
-- Name: bikes bikes_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.bikes
    ADD CONSTRAINT bikes_pkey PRIMARY KEY (bike_id);


--
-- Name: billing_invoices billing_invoices_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.billing_invoices
    ADD CONSTRAINT billing_invoices_pkey PRIMARY KEY (invoice_id);


--
-- Name: cities cities_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (city_id);


--
-- Name: customer_support_tickets customer_support_tickets_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.customer_support_tickets
    ADD CONSTRAINT customer_support_tickets_pkey PRIMARY KEY (ticket_id);


--
-- Name: daily_activity_summary_old daily_activity_summary_old_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.daily_activity_summary_old
    ADD CONSTRAINT daily_activity_summary_old_pkey PRIMARY KEY (summary_date);


--
-- Name: email_sends_log email_sends_log_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.email_sends_log
    ADD CONSTRAINT email_sends_log_pkey PRIMARY KEY (email_log_id);


--
-- Name: employee_payrolls employee_payrolls_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.employee_payrolls
    ADD CONSTRAINT employee_payrolls_pkey PRIMARY KEY (payroll_id);


--
-- Name: employees employees_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.employees
    ADD CONSTRAINT employees_pkey PRIMARY KEY (employee_id);


--
-- Name: logistics_fleet_vehicles logistics_fleet_vehicles_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.logistics_fleet_vehicles
    ADD CONSTRAINT logistics_fleet_vehicles_pkey PRIMARY KEY (vehicle_id);


--
-- Name: marketing_campaigns marketing_campaigns_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.marketing_campaigns
    ADD CONSTRAINT marketing_campaigns_pkey PRIMARY KEY (campaign_id);


--
-- Name: rental_transactions rental_transactions_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.rental_transactions
    ADD CONSTRAINT rental_transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: rentals_archive_2022 rentals_archive_2022_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.rentals_archive_2022
    ADD CONSTRAINT rentals_archive_2022_pkey PRIMARY KEY (rental_id);


--
-- Name: station_hardware_inventory station_hardware_inventory_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.station_hardware_inventory
    ADD CONSTRAINT station_hardware_inventory_pkey PRIMARY KEY (hardware_id);


--
-- Name: station_maintenance_schedule station_maintenance_schedule_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.station_maintenance_schedule
    ADD CONSTRAINT station_maintenance_schedule_pkey PRIMARY KEY (maintenance_id);


--
-- Name: subscriptions subscriptions_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.subscriptions
    ADD CONSTRAINT subscriptions_pkey PRIMARY KEY (subscription_id);


--
-- Name: supplier_invoices supplier_invoices_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.supplier_invoices
    ADD CONSTRAINT supplier_invoices_pkey PRIMARY KEY (supplier_invoice_id);


--
-- Name: user_accounts_deprecated user_accounts_deprecated_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.user_accounts_deprecated
    ADD CONSTRAINT user_accounts_deprecated_pkey PRIMARY KEY (user_id);


--
-- Name: user_accounts user_accounts_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.user_accounts
    ADD CONSTRAINT user_accounts_pkey PRIMARY KEY (user_id);


--
-- Name: user_session_logs user_session_logs_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.user_session_logs
    ADD CONSTRAINT user_session_logs_pkey PRIMARY KEY (session_id);


--
-- Name: vehicle_gps_tracking vehicle_gps_tracking_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.vehicle_gps_tracking
    ADD CONSTRAINT vehicle_gps_tracking_pkey PRIMARY KEY (ping_id);


--
-- Name: weather_forecast_hourly weather_forecast_hourly_pkey; Type: CONSTRAINT; Schema: raw; Owner: postgres
--

ALTER TABLE ONLY raw.weather_forecast_hourly
    ADD CONSTRAINT weather_forecast_hourly_pkey PRIMARY KEY (forecast_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 4IJhCeuFWTmIKKu4iLJ1xk9At0OZ0lQM6NuiYcQtI75YiOeZjCRlhIU7hT4d9zz

--
-- PostgreSQL database cluster dump complete
--

