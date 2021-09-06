# Overview

# 
* Change Directory's instances from string/Pathbuf to AsRef<Path>



Command line examples:


migrate:

osprey migrate table <table> directory <directory> up <up> down <down>

directory - the directory of where the sql files to migrate are, default is "./migrations"

table - the table name of where to store migration information, default will be "____migrations___" 

 up - the key for the queries to migrate up, if there is a missing key in a new file then it'll exit
   - Required for migrations
   
 down - the key for the queries to migrate down if the up query fails
   - if this isn't provided then app will exit upon first up query fail
   - if one is provided and an sql file doesn't have one then the app will exit if the up query fails
   - if one is provided and an sql file does have one then the that query will execute if the up query fails


sanity:

