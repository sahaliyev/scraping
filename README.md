# Files Responsibilities

#### turboaz_scraping.py
     1. This file is responsible for to scrap new items from turbo.az
     2. BeatifulSoup4 which is Python library used
     3. In order to avoid server blocking our ip, user-agents.py used which
     contains some user-agents and in every request one random user-agent sent

### turboaz_scraping_archive.py
    1. This file scraps all data in turbo.az by paginating
    
### turbo_db_operations.py
    1. This file is reponsible for database queries
    2. All queries are written there and inherited as needed

### filter_cars_to_send_email.py
    1. This file is responsible for filtering data based on requirements provided
    2. New data are passed to pandas dataframe and filtered

### filter_cars_based_on_requirements.py
    1. This file is also filtering data based on requirements, 
       however it export result to .xlsx file

### send_mail.py
    1. Filtered data sent to subscribers by the from of table
   
     
"# scraping" 
"# scraping" 
"# scraping" 
