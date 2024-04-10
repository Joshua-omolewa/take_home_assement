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

* Created an empty directory called takehomeassement using mkdir takehomeassement
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
* Add all untrack files for assessment to staging area using git add . 
* Commit changes and add commit message to new files using git commit -m “adding original assessment files”
* Push new files to remote repository using `git push -u origin main`
  
### 2 - creating local python3.10 environment and local posgres database

* I created a local python3.10 environment using python virtual environment and local database runing on docker using `docker run --name weclouddwh -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=post123 -p 5432:5432 -v /home/ubuntu/weclouddwh-data:/var/lib/postgresql/data -d postgres:alpine3.18` and the username is `postgres` and password is `post123` 
 
* Created a python 3.10 virtual environment (joshua-python3) for project so I can run python script locally before using docker compost to run python script as it is based on python3.10 to avoid dependency issue. I ran the following commands to create and activate the python virtual environment 
`sudo apt install python3.10-venv` , 
`python3.10 -m venv joshua-python3` , 
`source joshua-python3/bin/activate` 
* Created a .env fie to store credential of my local postgres databse
* Created  .gitignore file to exclude the .env file from being tracked, so I can test code locally using .env file with local postgres database for faster development before containerizing code 

### 3 - Developing code for project 
* Install requests, pandas, psycopg2, dotenv using `pip install requests`, `pip install psycopg2-binary`, `pip install pandas`, `pip install python-dotenv` and then importing the relevant modules into the app.py python script
* Developed the python script to extract data from the API by importing the relevant libraries (modules) and using requests library and paginate through the response using offset as shown in the  **[API documentation](https://openlibrary.org/dev/docs/api/recentchanges)** 

  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/4-%20importing%20libraries.png"  width="100%" height="100%">
* Inspecting schema of response from API for each kinds using the requests module, in order to properly model the data  for KINDS in the set (add-cover, add-book, edit-book, merge-authors) 
  <img src="https://github.com/Joshua-omolewa/take_home_assement/blob/main/images/3-%20inspecting%20repsonse.png"  width="100%" height="100%">




