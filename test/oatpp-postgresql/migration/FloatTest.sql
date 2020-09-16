DROP TABLE IF EXISTS test_floats;

CREATE TABLE test_floats (
  f_decimal         decimal,
  f_number          numeric(1000),
  f_real            real,
  f_double          double precision
);

INSERT INTO test_floats
(f_decimal, f_number, f_real, f_double) VALUES (null, null, null, null);

INSERT INTO test_floats
(f_decimal, f_number, f_real, f_double) VALUES (0, 0, 0, 0);

INSERT INTO test_floats
(f_decimal, f_number, f_real, f_double) VALUES (0.1, 0.1, 0.1, 0.1);

INSERT INTO test_floats
(f_decimal, f_number, f_real, f_double) VALUES (-0.1, -0.1, -0.1, -0.1);

