--------------------------------------------------------------------------		
			CS 166 Databases Project phase 3
--------------------------------------------------------------------------
Folder structure 

> data - holds the necessary data files, these are used by create.sql to insert data into the tables 

> java - holds DBproject.java, Boiler plate code. Your code goes here!
       - compile.sh, run this .sh file to start your java program
       - pg73jdbc3.jar, jar file used by DBproject.java - do not touch! 

> postgresql - holds startPostgreSQL.sh, createPostgreDB.sh, stopPostgreDB.sh files 
> sql - holds create.sql. This file holds SQL Statements to create appropriate tables and data in DB

-----------------------------------------------------------------------

Steps :

> Run -- startPostgreSQL.sql                -- to start the Database
> Run -- createPostgreDB.sh                 -- to create Database 
> Run -- cp -a data/* $PGDATA/              -- to copy data files to appropriate location
> Run -- cs166_psql $USER'_DB' < create.sql -- to create DB tables and copy data
> Run -- compile.sh                         -- to start your java program

Now that you are able to run the JAVA program and have the appropriate tables, go back to modify the Java code to add the logic needed. 





 