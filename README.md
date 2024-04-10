# Data Engineering Take Home Assignment:

### How to Run:

Run the entire application:
```bash
docker-compose up --build
```

Run the python application only:
```bash
docker-compose up --build pythonapp
```

Connect to postgres database:
```bash
docker exec -it $(docker-compose ps -q  db) psql -U user -d mydatabase
```

# PROJECT ARCHITECTURE

<img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/Project%20Architecture.gif"  width="100%" height="100%">


## SYSTEM SPECIFICATION, INSTALLATIONS AND TOOL

* **OS**: Ubuntu (Virtual machine - WSL)
* **Installation**: Installed docker, docker compose, jupyter noteboook
* **Integrated Development Environment (IDE)**: VSCODE
* **Tools**: Dbeaver (for connecting to my local postgres database runing in docker container)
* **Python version**: python3.10 as per project requirements

## STEP USED TO COMPLETE PROJECT

### 1 - Setting up environment for the project 

* Created an empty directory called takehomeassement using `mkdir takehomeassement`
* Navigate to the takehomeassement directory Unzip assessment content i.e., dock-compose, docker file etc. into this new directory
* create a git working directory using
`git init .`
* Create a remote repository on github called take_home_assement
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/1-remote%20repo.png"  width="100%" height="100%">
* Add remote repo to local  take homeassement directory using
`git remote add origin https://github.com/Joshua-omolewa/take_home_assement.git` and check remote repo has been added using  `git remote -v`  
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/2-%20ubuntu%20.png"  width="100%" height="100%">
* Rename current branch from master to main using
`git branch -M main` .This is because the master branch in github is called main hence we change the local branch on pc to main
* Add all untrack files for assessment to staging area using `git add .` 
* Commit changes and add commit message to new files using `git commit -m "adding original assessment files"`
* Push new files to remote repository using `git push origin -u main`
  
### 2 - creating local python3.10 environment and local posgres database

* I created a local python3.10 environment using python virtual environment and local database runing on docker using `docker run --name weclouddwh -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=post123 -p 5432:5432 -v /home/ubuntu/weclouddwh-data:/var/lib/postgresql/data -d postgres:alpine3.18` and the username is `postgres` and password is `post123` 
 
* Created a python 3.10 virtual environment (joshua-python3) for project so I can run python script locally before using docker compost to run python script as it is based on python3.10 to avoid dependency issue. I ran the following commands to create and activate the python virtual environment 
`sudo apt install python3.10-venv` , 
`python3.10 -m venv joshua-python3` , 
`source joshua-python3/bin/activate` 
* Created a .env fie to store credential of my local postgres databse
* Created  .gitignore file to exclude the .env file from being tracked, so I can test code locally using .env file with local postgres database for faster development before containerizing code 

### 3 - Developing code for project 
* Installing requests, pandas, psycopg2, dotenv using `pip install requests`, `pip install psycopg2-binary`, `pip install pandas`, `pip install python-dotenv` and then importing the relevant modules into the app.py python script
* Developed the python script to extract data from the API by importing the relevant libraries (modules) and using requests library and paginate through the response using offset as shown in the  **[API documentation](https://openlibrary.org/dev/docs/api/recentchanges)** 

  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/4-%20importing%20libraries.png"  width="100%" height="100%">
* Inspecting schema of response from API for each kinds using the requests module, in order to properly model the data  for KINDS in the set (add-cover, add-book, edit-book, merge-authors) 
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/3-%20inspecting%20repsonse.png"  width="100%" height="100%">
* Created the DDL SQL script called `create_tables.sql` for the project based on the API reponse which can be found   **[here](https://github.com/Joshua-omolewa/take_home_assement/blob/main/create_tables.sql)** 
* Used Jupyter notebook to inspect the response from the API so I can properly structure my python scripts
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/13%20-jupyter%20notedbook%20for%20exploration.png"  width="100%" height="100%">
* I created a separate python file called `etl.py` to encapsulate the extraction and transformation process. The function for loading the data into the data is in the `app.py` python script
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/5-%20data%20extrction.png"  width="100%" height="100%">
* I performed two extraction processes as I needed to first ingest the raw data from the API as per requirements, transform the data and then use the tranformed data to perform another extraction process from the API for each book endpoint. To accomplish this I transformed the raw data to focus on books as per the requirements and then I created a new column to store only one of the book id as per the requirements. The function logic can be found in the `etl.py` python script which can be found **[here](https://github.com/Joshua-omolewa/take_home_assement/blob/main/etl.py)**
* Tested  data extraction locally using the `app.py` and `etl.py` python scripts. the data extraction in progress as seen in the images below
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/6-%20data%20extraction%20process.png"  width="100%" height="100%">
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/7-%20extracting%20book%20data%20using%20data%20from%20raw%20data.png"  width="100%" height="100%"> 
* The extraction and transfromation process completed and then table is  created in database using the `create_tables.sql` script and finally data is loaded into the two tables called `books` and `request_changes`. Please see images below for the logs and ERD diagram for the databse
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/8-%20data%20ETL%20proces%20complete.png"  width="100%" height="100%"> 
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/ERD%20diagram.png"  width="100%" height="100%">  
* Checked database using dbeaver to see if the tables have been created and if data has been loaded into the two tables. Please see images below
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/9-%20checking%20databse%20to%20see%20if%20tables%20have%20been%20created.png"  width="100%" height="100%">
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/10-%20checking%20databse%20to%20see%20if%20tables%20have%20been%20created.png"  width="100%" height="100%"> 
### 4 - Developing unit tests for the data pipeline
* prepared unit test using pytest framework. See sample picture below
 <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/11-unit%20testsmaple%20fialed.png"  width="100%" height="100%">
 
* Unit tests created for project include testing the API connection, testing the database connection just to ensure it works correctly as data from the api will be loaded into the database tables, testing the extraction, transformation and loading processes
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/12-%20unit%20test%20passed.png"  width="100%" height="100%"> 
* Executed the `app.py` python script which includes the unit tests to see if everything works perfectly. As seen in the image below the code works perfectly.
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/14-%20testing%20code%20in%20full%20locally.png"  width="100%" height="100%"> 

### 5 - Runing python script in a containerized approach using docker with docker compose
* I ran the entire applicaton using 
```bash
docker-compose up --build
``` 
*  The unit tests, data extraction, transformation and loading process complete succesfully as seen in the images below
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/15-%20testing%20script%20runing%20in%20docker%20environment.png"  width="100%" height="100%">  


