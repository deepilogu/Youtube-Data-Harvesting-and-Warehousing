USE youtube_data;


#1.What are the names of all the videos and their corresponding channels?

select v.video_name AS `VIDEO NAME`, ch.channel_name AS `CHANNEL NAME` from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id;

#2.Which channels have the most number of videos, and how many videos do they have?

Select channel_name, video_count from channel WHERE video_count = (SELECT MAX(video_count) from channel);


#3.What are the top 10 most viewed videos and their respective channels?

SELECT v.video_name, v.view_count, ch.channel_name from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id ORDER BY v.view_count DESC LIMIT 10;

#4.How many comments were made on each video, and what are their corresponding video names?

SELECT v.video_name ,v.comment_count, ch.channel_name from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id;

#5.Which videos have the highest number of likes, and what are their corresponding channel names?

SELECT v.video_name, v.like_count , ch.channel_name FROM videos AS v INNER JOIN channel AS ch  WHERE v.channel_id = ch.channel_id AND v.like_count = (SELECT MAX(like_count) FROM videos); 

#6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?

SELECT video_name, like_count FROM videos ORDER BY like_count;


#7.What is the total number of views for each channel, and what are their corresponding channel names?

SELECT channel_name, channel_views from channel ORDER BY channel_views DESC;

#8.What are the names of all the channels that have published videos in the year 2022?


SELECT ch.channel_name, v.video_name,DATE_FORMAT(publishedat, '%Y') as publish_year
FROM videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id and 
DATE_FORMAT(publishedat, '%Y') = '2023';

#9.What is the average duration of all videos in each channel, and what are their corresponding channel names?

SELECT ch.channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v.duration)))), '%H:%i:%s') AS average_duration from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id GROUP BY ch.channel_name;


#10.Which videos have the highest number of comments, and what are their corresponding channel names?

SELECT v.video_name, v.comment_count, ch.channel_name from videos AS v INNER JOIN channel AS ch WHERE v.channel_id = ch.channel_id and v.comment_count = (SELECT MAX(comment_count) from videos)