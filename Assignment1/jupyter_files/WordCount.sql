-- Create table in hive referencing /data/helloworld

CREATE EXTERNAL TABLE IF NOT EXISTS helloworld (line STRING) LOCATION ${input};

-- Run query

DROP TABLE IF EXISTS word_counts;
CREATE TABLE word_counts LOCATION ${output} AS
SELECT
    word,
    count(1) AS count
FROM
    (
        SELECT
            explode(split(line, ' ')) AS word
        FROM
            helloworld
    ) words
GROUP BY
    word
ORDER BY
    word;
    