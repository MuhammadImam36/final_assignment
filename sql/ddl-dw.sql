SELECT 'CREATE DATABASE pangan_indonesia_2025'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pangan_indonesia_2025')\gexec