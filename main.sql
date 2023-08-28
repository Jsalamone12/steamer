"""
-- Given the following SQL tables:

 

-- streamers: it contains time series data, at a 1-min granularity, of all the channels that broadcast on Twitch. The columns of the table are:

-- username: Channel username

-- timestamp: Epoch timestamp, in seconds, corresponding to the moment the data was captured

-- game: Name of the game that the user was playing at that time

-- viewers: Number of concurrent viewers that the user had at that time

-- followers: Number of total followers that the channel had at that time

-- games_metadata: it contains information of all the games that have ever been broadcasted on Twitch. The columns of the table are:

-- game: Name of the game

-- release_date: Timestamp, in seconds, corresponding to the date when the game was released

-- publisher: Publisher of the game

-- genre: Genre of the game


-- Write an SQL query to:

-- Obtain, for each month of 2020, how many streamers broadcasted on Twitch and how many hours of content were broadcasted. The output should contain the month, unique_streamers and hours_broadcast.


--  Question one 


Obtain, for each month of 2020, how many streamers broadcasted on Twitch and 
how many hours of content were broadcasted. 
The output should contain the month, unique_streamers and hours_broadcast.
"""

SELECT
    DATE_FORMAT(FROM_UNIXTIME(s.timestamp), '%Y-%m') AS month,
    COUNT(DISTINCT s.username) AS unique_streamers,
    SUM(TIMESTAMPDIFF(SECOND, s.timestamp, LEAD(s.timestamp, 1) OVER(PARTITION BY s.username ORDER BY s.timestamp))) / 3600 AS hours_broadcast
FROM
    streamers s
WHERE
    YEAR(FROM_UNIXTIME(s.timestamp)) = 2020
GROUP BY
    month
ORDER BY
    month;

"""
--  Question two 
Obtain the Top 10 publishers that have been watched the most during the second quarter of 2019. 
The output should contain the publisher and hours_watched.
Note: Hours watched can be defined as the total amount of hours watched 
by all the viewers combined. Ie: 10 viewers watching for 2 hours will generate 20 Hours Watched.
"""

SELECT
    gm.publisher,
    SUM(TIMESTAMPDIFF(SECOND, s.timestamp, LEAD(s.timestamp, 1) OVER(PARTITION BY s.username ORDER BY s.timestamp))) / 3600 * SUM(s.viewers) AS hours_watched
FROM
    streamers s
JOIN
    games_metadata gm ON s.game = gm.game
WHERE
    YEAR(FROM_UNIXTIME(s.timestamp)) = 2019
    AND QUARTER(FROM_UNIXTIME(s.timestamp)) = 2
GROUP BY
    gm.publisher
ORDER BY
    hours_watched DESC
LIMIT
    10;

"""
-- Question three
Obtain the Top 10 streamers that have percentually gained more followers 
during January 2021, and that primarily stream FPS games. 
The output should contain the username and follower_growth.

Note: If a streamer primarly broadcasts FPS games it means that 
the most streamed genre is FPS. However, that streamer might have broadcast other genres as well.
"""


WITH FollowerGrowth AS (
    SELECT
        s.username,
        s.followers AS starting_followers,
        LEAD(s.followers, 1) OVER(PARTITION BY s.username ORDER BY s.timestamp) AS ending_followers,
        gm.genre
    FROM
        streamers s
    JOIN
        games_metadata gm ON s.game = gm.game
    WHERE
        YEAR(FROM_UNIXTIME(s.timestamp)) = 2021
        AND MONTH(FROM_UNIXTIME(s.timestamp)) = 1
)
SELECT
    username,
    ((ending_followers - starting_followers) / starting_followers) * 100 AS follower_growth
FROM
    FollowerGrowth
WHERE
    genre = 'FPS'
ORDER BY
    follower_growth DESC
LIMIT
    10;
