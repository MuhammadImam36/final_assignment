-- Tabel dim_status
DROP TABLE IF EXISTS public.dim_status;
CREATE TABLE public.dim_status (
    id_status INTEGER PRIMARY KEY,
    status VARCHAR(50),
    deskripsi_status VARCHAR(255)
);

-- Tabel dim_provinsi
DROP TABLE IF EXISTS public.dim_provinsi;
CREATE TABLE public.dim_provinsi (
    id_provinsi INTEGER PRIMARY KEY,
    nama_provinsi VARCHAR(255),
    is_provinsi VARCHAR(10),
    is_produsen VARCHAR(10)
);

-- Tabel dim_date
DROP TABLE IF EXISTS public.dim_date;
CREATE TABLE public.dim_date (
    id_date INTEGER PRIMARY KEY,
    date DATE, 
    month INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER
);

-- Tabel dim_komoditas
DROP TABLE IF EXISTS public.dim_komoditas;
CREATE TABLE public.dim_komoditas (
    id_komoditas INTEGER PRIMARY KEY,
    komoditas VARCHAR(255),
    nama_sektor VARCHAR(255)
);