-- A stand-in for a business database the platform reads from. Deterministic, 1,000 rows.
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    customer text NOT NULL,
    amount numeric(10, 2) NOT NULL,
    paid boolean NOT NULL,
    ordered_on date NOT NULL,
    updated_at timestamptz NOT NULL,
    ref uuid NOT NULL,
    note text
);

INSERT INTO orders
SELECT
    i,
    'customer ' || (i % 97),
    round((i * 7.31)::numeric % 1000, 2),
    i % 4 <> 0,
    date '2024-01-01' + (i % 365),
    timestamptz '2024-01-01 00:00:00+00' + i * interval '37 minutes',
    md5(i::text)::uuid,
    CASE WHEN i <= 500 THEN NULL ELSE 'note ' || i END
FROM generate_series(1, 1000) AS i;

-- A table an older system filled, with its problems built in on purpose for sources/messy_db:
-- no primary key, so order 3007 is in it twice; quantity and ordered_on are free text, so one
-- quantity is a word and one date does not exist; regions and statuses are spelt and cased
-- several ways; some values are missing; one amount is a typing error in the millions; and one
-- quantity is negative. Plain SQL, so tests/test_demo_sources.py can run it in SQLite too.
CREATE TABLE messy_orders (
    id bigint,
    customer text,
    region text,
    quantity text,
    amount numeric(10, 2),
    ordered_on text,
    status text
);

INSERT INTO messy_orders VALUES
    (3001, 'Anna de Vries', 'North', '2', 39.90, '2024-04-01', 'paid'),
    (3002, 'Bram Jansen', 'north', '1', 12.50, '2024-04-01', 'PAID'),
    (3003, 'Chris Peters', 'NORTH', 'a dozen', 64.00, '2024-04-02', 'open'),
    (3004, NULL, 'South', '3', 27.75, '2024-04-02', 'open'),
    (3005, 'Erik Bos', 'south', '2', NULL, '2024-04-31', 'paid'),
    (3006, 'Fleur Mulder', 'Zuid', '4', 18.20, '2024-04-03', 'Paid'),
    (3007, 'Gijs Visser', 'East', '1', 22.00, '2024-04-04', 'refunded'),
    (3007, 'Gijs Visser', 'East', '1', 22.00, '2024-04-04', 'refunded'),
    (3008, 'Hanna Meijer', 'east', '-1', 15.00, '2024-04-05', 'refunded'),
    (3009, 'Ivo de Boer', 'Oost', '5', 4500000.00, '2024-04-05', 'paid'),
    (3010, 'Jet Bakker', 'West', NULL, 31.40, '2024-04-06', 'open'),
    (3011, 'Kees Dekker', 'west', '2', NULL, '2024-04-07', 'paid'),
    (3012, 'Lotte Smits', 'WEST', '1', 9.95, '2024-04-07', 'open'),
    (3013, 'Mark Hendriks', 'North', '6', 54.00, '2024-04-08', 'paid'),
    (3014, 'Noor Vermeulen', 'South', '2', 26.30, '2024-04-09', 'paid'),
    (3015, 'Olaf Kok', 'East', '3', 41.25, '2024-04-10', 'open');
