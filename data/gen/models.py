from datetime import datetime
from pony.orm import Database, Required, Set, Optional, PrimaryKey
from pony.orm import db_session
import os

# Clears the existing database
if os.path.exists('/code/data/gen/database.sqlite'):
    os.remove('/code/data/gen/database.sqlite')

# Init
db = Database()

class Table_ats(db.Entity):
    _table_ = 'ats' 
    time = Required(datetime) 
    person_name = Required(str) 
    phone = Required(str) 
    email = Required(str) 
    company = Required(str) 
    role = Required(str) 
    application_status = Required(str) 

# Establish DB connection
db.bind(provider="sqlite", filename="database.sqlite", create_db=True)
db.generate_mapping(create_tables=True)

# db insert
@db_session 
def insert_Table_ats(list_of_dicts: []):
   for idx in range(len(list_of_dicts)):
        Table_ats(
             time=list_of_dicts[idx].get('time'),
             person_name=list_of_dicts[idx].get('person_name'),
             phone=list_of_dicts[idx].get('phone'),
             email=list_of_dicts[idx].get('email'),
             company=list_of_dicts[idx].get('company'),
             role=list_of_dicts[idx].get('role'),
             application_status=list_of_dicts[idx].get('application_status'),
            
        )


@db_session
def select_statement(statement):
    return db.select(statement)