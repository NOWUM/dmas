CREATE TABLE hourly_orders
(
    hour     bigint,
    block_id bigint,
    name     text,
    price    double precision,
    volume   double precision,
    type     text
);
ALTER TABLE "hourly_orders"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE linked_orders
(
    block_id bigint,
    hour     bigint,
    name     text,
    price    double precision,
    volume   double precision,
    link     bigint,
    type     text
);
ALTER TABLE "linked_orders"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE exclusive_orders
(
    block_id bigint,
    hour     bigint,
    name     text,
    price    double precision,
    volume   double precision
);
ALTER TABLE "exclusive_orders"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE capacities
(
    "time"  timestamp without time zone,
    bio     double precision,
    coal    double precision,
    gas     double precision,
    lignite double precision,
    nuclear double precision,
    solar   double precision,
    water   double precision,
    wind    double precision,
    storage double precision,
    agent   text,
    area    text
);
ALTER TABLE "capacities"
    ADD PRIMARY KEY ("time", "agent");
CREATE TABLE demand
(
    "time" timestamp without time zone,
    power  double precision,
    heat   double precision,
    step   text,
    agent  text,
    area   text
);
ALTER TABLE "demand"
    ADD PRIMARY KEY ("time", "step", "agent");
CREATE TABLE generation
(
    "time"  timestamp without time zone,
    total   double precision,
    solar   double precision,
    wind    double precision,
    water   double precision,
    bio     double precision,
    lignite double precision,
    coal    double precision,
    gas     double precision,
    nuclear double precision,
    allocation double precision,
    step    text,
    agent   text,
    area    text
);
ALTER TABLE "generation"
    ADD PRIMARY KEY ("time", "step", "agent");
CREATE TABLE auction_results
(
    "time" timestamp without time zone,
    price  double precision,
    volume double precision,
    magic_source double precision
);
ALTER TABLE "auction_results"
    ADD PRIMARY KEY ("time");
CREATE TABLE hourly_results
(
    hour     bigint,
    block_id bigint,
    name     text,
    price    double precision,
    volume   double precision,
    type     text
);
ALTER TABLE "hourly_results"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE linked_results
(
    block_id bigint,
    hour     bigint,
    name     text,
    price    double precision,
    volume   double precision,
    link     bigint,
    type     text
);
ALTER TABLE "linked_results"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE exclusive_results
(
    block_id bigint,
    hour     bigint,
    name     text,
    price    double precision,
    volume   double precision
);
ALTER TABLE "exclusive_results"
    ADD PRIMARY KEY ("block_id", "hour", "name");
CREATE TABLE orders
(
    "time" timestamp without time zone,
     volume double precision,
     price double precision,
     agent text,
     area text
);
ALTER TABLE "orders"
    ADD PRIMARY KEY ("time","agent");
CREATE TABLE cash_flows
(
    "time" timestamp without time zone,
     profit double precision,
     emission double precision,
     fuel double precision,
     start_ups double precision,
     agent text,
     area text
);
ALTER TABLE "orders"
    ADD PRIMARY KEY ("time","agent");



SELECT create_hypertable('orders', 'time', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('auction_results', 'time', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('capacities', 'time', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('generation', 'time', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('demand', 'time', if_not_exists => TRUE, migrate_data => TRUE);
