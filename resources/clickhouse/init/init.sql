CREATE DATABASE IF NOT EXISTS external;

CREATE FUNCTION dateInAvalancheSeason AS (d) ->
  (d::date >= toDate(concatWithSeparator('-', toString(toYear(d::date)), '9', '1'), 'UTC')
  OR d::date < toDate(concatWithSeparator('-', toString(toYear(d::date)), '6', '1'), 'UTC'))::bool;
