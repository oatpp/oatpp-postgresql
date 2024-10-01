DROP TABLE IF EXISTS test_enumasstring;

CREATE TABLE test_enumasstring (
  f_enumint      int,
  f_enumstring   varchar(256)
);

INSERT INTO test_enumasstring
(f_enumint, f_enumstring) VALUES (null, null);

INSERT INTO test_enumasstring
(f_enumint, f_enumstring) VALUES (0, 'dog');

INSERT INTO test_enumasstring
(f_enumint, f_enumstring) VALUES (1, 'cat');
