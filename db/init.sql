-- db/init.sql
-- Tabla de ventas sintéticas (mínimo 10,000 registros vía seed)
CREATE TABLE IF NOT EXISTS sales (
    sale_id        BIGSERIAL PRIMARY KEY,
    sale_date      DATE        NOT NULL,
    customer_name  TEXT        NOT NULL,
    product_name   TEXT        NOT NULL,
    quantity       INTEGER     NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount   NUMERIC(12,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON sales (sale_date);

-- Ciclo de vida del reporte
CREATE TABLE IF NOT EXISTS reports (
    id                UUID        PRIMARY KEY,
    report_type       TEXT        NOT NULL,
    start_date        DATE        NOT NULL,
    end_date          DATE        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'pending',
    -- 'pending' | 'queued' | 'processing' | 'retrying' | 'completed' | 'failed'
    idempotency_key   TEXT        UNIQUE,
    attempts          INTEGER     NOT NULL DEFAULT 0,
    exported_rows     INTEGER,
    file_path         TEXT,        -- ruta relativa dentro de ./output
    error_message     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reports_status ON reports (status);
CREATE INDEX IF NOT EXISTS idx_reports_idempotency ON reports (idempotency_key);

-- Tabla para reconciliación de mensajes no publicados (outbox simplificado)
CREATE TABLE IF NOT EXISTS pending_publications (
    report_id     UUID PRIMARY KEY REFERENCES reports(id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trigger para actualizar updated_at automáticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reports_updated_at ON reports;
CREATE TRIGGER trg_reports_updated_at
    BEFORE UPDATE ON reports
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();