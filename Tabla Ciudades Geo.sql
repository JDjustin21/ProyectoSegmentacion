--
-- PostgreSQL database dump
--

\restrict eSD6QpJeZDJoWD07DJz1Si2TelzaAzIeMwnh6gpfVsIecydVhGLHI4oR4RVrKbl

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

-- Started on 2026-05-15 07:37:46

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 284 (class 1259 OID 30920)
-- Name: maestra_ciudades_geo; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.maestra_ciudades_geo (
    ciudad text NOT NULL,
    departamento text,
    latitud numeric(12,8) NOT NULL,
    longitud numeric(12,8) NOT NULL,
    estado text DEFAULT 'Activa'::text NOT NULL,
    fecha_actualizacion timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_maestra_ciudades_geo_estado CHECK ((estado = ANY (ARRAY['Activa'::text, 'Inactiva'::text])))
);


ALTER TABLE public.maestra_ciudades_geo OWNER TO postgres;

--
-- TOC entry 5235 (class 0 OID 30920)
-- Dependencies: 284
-- Data for Name: maestra_ciudades_geo; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO public.maestra_ciudades_geo VALUES ('ARMENIA', 'Quindío', 4.53390000, -75.68110000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('BARRANCABERMEJA', 'Santander', 7.06530000, -73.85470000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('BARRANQUILLA', 'Atlántico', 10.96850000, -74.78130000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('BELLO', 'Antioquia', 6.33820000, -75.56280000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('BOGOTÁ', 'Bogotá D.C.', 4.71100000, -74.07210000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('BUCARAMANGA', 'Santander', 7.11930000, -73.12270000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('CALI', 'Valle del Cauca', 3.45160000, -76.53200000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('CARTAGENA DE INDIAS', 'Bolívar', 10.39100000, -75.47940000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('CAUCASIA', 'Antioquia', 7.98650000, -75.19350000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('ENVIGADO', 'Antioquia', 6.16740000, -75.58330000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('FLORENCIA', 'Caquetá', 1.61440000, -75.60620000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('IBAGUÉ', 'Tolima', 4.43890000, -75.23220000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('JAMUNDÍ', 'Valle del Cauca', 3.26070000, -76.53490000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('MEDELLÍN', 'Antioquia', 6.24420000, -75.58120000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('MONTERÍA', 'Córdoba', 8.74790000, -75.88140000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('NEIVA', 'Huila', 2.93450000, -75.28090000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('PEREIRA', 'Risaralda', 4.80870000, -75.69060000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('PIEDECUESTA', 'Santander', 6.98790000, -73.04950000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('POPAYÁN', 'Cauca', 2.44480000, -76.61470000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('RIONEGRO', 'Antioquia', 6.15520000, -75.37370000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('SABANETA', 'Antioquia', 6.15090000, -75.61660000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('SAN JOSÉ DE CÚCUTA', 'Norte de Santander', 7.89390000, -72.50780000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('SANTA MARTA', 'Magdalena', 11.24080000, -74.19900000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('SINCELEJO', 'Sucre', 9.30470000, -75.39780000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('SOLEDAD', 'Atlántico', 10.91840000, -74.76460000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('VALLEDUPAR', 'Cesar', 10.46310000, -73.25320000, 'Activa', '2026-05-12 11:00:58.190409-05');
INSERT INTO public.maestra_ciudades_geo VALUES ('VILLAVICENCIO', 'Meta', 4.14200000, -73.62660000, 'Activa', '2026-05-12 11:00:58.190409-05');


--
-- TOC entry 5054 (class 2606 OID 30934)
-- Name: maestra_ciudades_geo maestra_ciudades_geo_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maestra_ciudades_geo
    ADD CONSTRAINT maestra_ciudades_geo_pkey PRIMARY KEY (ciudad);


-- Completed on 2026-05-15 07:37:46

--
-- PostgreSQL database dump complete
--

\unrestrict eSD6QpJeZDJoWD07DJz1Si2TelzaAzIeMwnh6gpfVsIecydVhGLHI4oR4RVrKbl

