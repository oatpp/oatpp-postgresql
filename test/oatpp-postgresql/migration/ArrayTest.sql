DROP TABLE IF EXISTS test_arrays1;
DROP TABLE IF EXISTS test_arrays2;

CREATE TABLE test_arrays1 (
    f_real          real[],
    f_double        double precision[],

    f_int16         smallint[],
    f_int32         integer[],
    f_int64         bigint[],
    f_bool          boolean[],

    f_text          text[]
);

CREATE TABLE test_arrays2 (
    f_real          real[][],
    f_double        double precision[][],

    f_int16         smallint[][],
    f_int32         integer[][],
    f_int64         bigint[][],
    f_bool          boolean[][],

    f_text          text[][]
);

INSERT INTO test_arrays1
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
(null, null, null, null, null, null, null);

INSERT INTO test_arrays1
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{}', '{}', '{}', '{}', '{}', '{}', '{}');

INSERT INTO test_arrays1
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{null, null}', '{null, null}', '{null, null}', '{null, null}', '{null, null}', '{null, null}', '{null, null}');

INSERT INTO test_arrays1
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{0}', '{0}', '{0}', '{0}', '{0}', '{false}', '{"", ""}');

INSERT INTO test_arrays1
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{1}', '{1}', '{1}', '{1}', '{1}', '{true}', '{"hello"}');


INSERT INTO test_arrays2
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
(null, null, null, null, null, null, null);

INSERT INTO test_arrays2
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{}', '{}', '{}', '{}', '{}', '{}', '{}');

INSERT INTO test_arrays2
(f_real, f_double, f_int16, f_int32, f_int64, f_bool, f_text)
VALUES
('{{0, 1}, {2, 3}}', '{{0, 1}, {2, 3}}', '{{0, 1}, {2, 3}}', '{{0, 1}, {2, 3}}', '{{0, 1}, {2, 3}}', '{{false, true}, {true, false}}', '{{"Hello_1", "World_1"}, {Hello_2, World_2}}');
