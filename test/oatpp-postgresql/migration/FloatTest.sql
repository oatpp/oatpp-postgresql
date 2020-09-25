DROP TABLE IF EXISTS test_floats;

CREATE TABLE test_floats (
  f_real            real,
  f_double          double precision
);

INSERT INTO test_floats
(f_real, f_double) VALUES (null, null);

INSERT INTO test_floats
(f_real, f_double) VALUES (0, 0);

INSERT INTO test_floats
(f_real, f_double) VALUES (1, 2);

INSERT INTO test_floats
(f_real, f_double) VALUES (-1, -2);
