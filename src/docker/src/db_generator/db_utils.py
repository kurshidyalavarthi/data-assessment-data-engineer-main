from jinja2 import Template, FileSystemLoader, Environment
import json
import yaml
from pony.orm import db_session


# Ephemeral Pony ORM models.py generator (cooks inside docker contianer, for reference - /app/db in container is {root}/src/docker/db in repo
def generate_pony_orm_model():
    """
    Takes the table model in {root}/src/docker/db/models/tables.yml and uses that as a base for a pony orm models jinja
    Returns: None
    """
    template = __get_template()
    table_spec = __get_table_models()

    output = template.render(table_spec)
    __ship_models_py(output)

def __get_template():
    """
    Fetches template from {root}/src/docker/db/templates/models.py.jinja
    Returns: jinja template in string format
    """

    with open('/code/data/templates/models.py.jinja', 'r') as f:
        return Template(f.read())

def __get_table_models():
    """
    Fetches table models from {root}/src/docker/db/models/tables.yml
    Returns: table spec as dict
    """
    with open('/code/data/models/tables.yml', 'r') as f:
        return yaml.safe_load(f)

def __ship_models_py(output):
    """
    Ships the resulting models.py into {root}/src/docker/db/gen/models.py (which is ephemeral....)
    Returns: None

    """
    # with open('/app/src/gen/models.py', 'w') as f:
    #     f.write(output)
    with open('/code/data/gen/models.py', 'w') as f:
        f.write(output)