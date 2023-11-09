YouTube Data Harvesting and Warehousing Project
(Using SQL, MongoDB and Streamlit)

DESCRIPTION:

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

TECHNOLOGIES NEED TO KNOW:

•	Python
•	mongoDB
•	MySQL
•	Streamlit Library


SOFTWARE REQUIRED:

•	VSCode
•	MongoDB Compass
•	MySQL Workbench
•	Updated web browser

REQUIRED LIBRARIES TO INSTALL:

pip install google-api-python-client, pymongo, mysql-connector-python, sqlalchemy, pandas, streamlit.

INTRODUCTION:

This project is a YouTube API scrapper that allows users to retrieve and analyze data from YouTube channels. It utilizes the YouTube Data API to fetch information such as channel statistics, video details, comments, and more. The scrapper provides various functionalities to extract and process YouTube data for further analysis and insights.



FEATURES:

The YouTube Data Scraper offers a range of features to help you extract and analyze data from YouTube. 

Some of the key features include:

Retrieve channel details: Get detailed information about YouTube channels, including subscriber count, view count, video count, and other relevant metrics.

Retrieve video details: Extract data such as video title, description, duration, view count, like count, dislike count, and publish date for individual videos.

Retrieve comments: Retrieve comments made on YouTube videos and perform analysis, such as sentiment analysis or comment sentiment distribution.

Generate reports: Generate reports and visualizations based on the collected data, allowing users to gain insights into channel performance, video engagement, and audience interaction.

Data storage: Store the collected YouTube data in a database for easy retrieval and future reference.





TECHNOLOGIES USED:

	Python: The project is implemented using the Python programming language.

	YouTube Console:  Utilizes the official YouTube Data API to interact with YouTube's platform and retrieve data.

	Streamlit: The user interface and visualization are created using the Streamlit framework, providing a seamless and interactive experience.

	MongoDB: The collected data can be stored in a MongoDB database for efficient data management and querying.

	MySQL: A powerful open-source relational database management system used to store and manage the retrieved data.

	PyMongo:  A Python library that enables interaction with MongoDB, a NoSQL database. It is used for storing and retrieving data from MongoDB in the YouTube Data Scraper.

	MySQL-Connector: A MySQL adapter for Python that allows seamless integration between Python and MySQL. It enables the YouTube Data Scraper to connect to and interact with the PostgreSQL database.

	Pandas: A powerful data manipulation and analysis library in Python. Pandas is used in the YouTube Data Scraper to handle and process data obtained from YouTube, providing functionalities such as data filtering, transformation, and aggregation.

]


ETL PROCESS:

	 Extracting Data from YouTube Console using Your own API Key.

     Transforming data into the JSON format.

     Loading Data into MongoDB and MySQL

APPLICATION FLOW:

	Input the Channel Id in the input text field.

	Next click on Extract data button will extract data and store MongoDB (Unstructured format).

	In next part of the application, we can choose the channel ID 
Which we can move to MySQL

	 Click on Next button to more to the next part of the application.

	There you can select any of a query from the dropdown to see the detailed reports of collected data right below.

ADDITIONAL INFORMATION:

Please note that when using this application, it is essential to comply with the YouTube Data API's terms of service and adhere to its usage limits to ensure uninterrupted access to the API. If you encounter any issues or have questions regarding the YouTube Data Scraper, please refer to the project's detailed documentation available in the GitHub repository.


LICENSE:

The YouTube Data Scraper is released under the MIT License. Feel free to modify and use the code according to the terms of the license.

CONCLUSION:

This YouTube API scrapper project aims to provide a powerful tool for retrieving, analyzing, and visualizing YouTube data, enabling users to gain valuable insights into channel performance, video engagement, and audience feedback.
