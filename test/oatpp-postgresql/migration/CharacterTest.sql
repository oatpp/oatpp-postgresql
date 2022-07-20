DROP TABLE IF EXISTS test_characters;

CREATE TABLE test_characters (
  f_char            "char",
  f_bpchar          char,
  f_bpchar4         char(4),
  f_varchar         varchar(4),
  f_text            text
);

INSERT INTO test_characters
(f_char, f_bpchar, f_bpchar4, f_varchar, f_text) VALUES (null, null, null, null, null);

INSERT INTO test_characters
(f_char, f_bpchar, f_bpchar4, f_varchar, f_text) VALUES ('#', '$', '%', '^', '&');

INSERT INTO test_characters
(f_char, f_bpchar, f_bpchar4, f_varchar, f_text) VALUES ('a', 'b', 'cccc', 'dddd', 'eeeee');
