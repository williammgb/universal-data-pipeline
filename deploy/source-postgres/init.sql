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
