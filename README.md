# data-assessment-data-engineer

Welcome to the take-home assessment for the data engineer role on the Data Team at Workstep!  

This exercise has two parts.  We expect this exercise to consume about 2 hours in total. If you exceed 3 hours, please reach out, as we value your time and we would take the feedback to narrow the scope of the exercise.  

## Background
For the purposes of tracking our HIRE product’s impact with a particular customer, we ingest their Applicant Tracking Systems’ (ATS) data.  For the purposes of this exercise the data is a table of applicant events as they cascade through the interview process.  For example, the data could look like this:

| timestamp           |  applicant_name   |        role_title        |  application_status   |
|:--------------------|:-----------------:|:------------------------:|:---------------------:|
| 2022-02-21 19:08:52 | Arthur Applicant  |   Forklift Operator II   |        applied        |     
| 2022-02-25 10:43:45 | Arthur Applicant  |   Forklift Operator II   |     disqualified      |    
| 2022-02-23 10:43:45 | Celeste Jobhunter | Packaging Technician III |        applied        |   
| 2022-02-22 15:51:16 | Arthur Applicant  |   Forklift Operator II   | waiting for interview |  


In the above, you can see how an applicant ‘Arthur Applicant’ applied, waited for an interview, and got disqualified(following presumably an interview).  Note that the data may not appear ordered, and will include events pertaining to other applicants/roles.  

All possible application statuses are depicted below:
- Applied
- Waiting for interview
- Offer sent
- Offer Rejected
- Hired
- Disqualified

Typically (and for the purposes of this exercise) the flow of applicants’ status as time goes on looks like this:

	Applied -> Waiting for Interview -> Offer Sent -> Hired 

There are two possible final states that are not covered in the above depiction.  Between any two states in the above process, an applicant can be Disqualified, and this would be their final state over time. Additionally, an applicant may choose to reject the offer at which point the status is updated to Offer Rejected and this is the applicant’s final state.  An example of this could be:

    Applied -> Waiting for Interview -> Disqualified

or

    Applied -> Waiting for Interview -> Offer Sent -> Offer Rejected

## Assessment

### Getting Around (a.k.a Repository Structure)

The code you will work on is containerized and requires docker to run (https://www.docker.com/products/personal/).  The heart of the code sits in `src/docker`.  `src/docker/Dockerfile` contains information on how the contianer is made.  If you wish to add different packages to the python instance running in the container, simply add it to `src/dockerlibs/requirements.txt`.  The container can be built and run on your local docker installation by executing `./scripts/build.sh` and `./scripts/run.sh` (you may need to `chmod +x` the scripts on your local machine.  Your starting point will be `src/docker/src/entrypoint.py`.

### Part One - ETL Design

Using the datasets from three fictional companies ATS’ (in `/input`) compose a data pipeline in Python that extracts the data from the three data sources, does a few transformations (described below) and loads the data into a database.

As mentioned above of the code sits in `src/docker/src/entrypoint.py` with the excerpt pertinent to this exercise depicted below

```
# Pipeline steps
@pipeline.task()
def part_one__extract_raw_data(path: str):
    """
    Extracts the raw data from csv, xlsx, json files
    Args:
        path: location of raw data
    Returns: Enitrely up to you! included is a dummy dataset, that passes through the rest of the pipeline
    """
    logging.info('Extracting Raw Data...')


    dummy = [
        {
            'time': '2022-04-05 10:00:00.0',
            'person_name': 'Arthur Applicant',
            'phone': '2134567890',
            'email': 'arthur.applicant@cox.net',
            'company': 'Acme Anvil Corporation',
            'role': 'Wile E. Coyote Revivor',
            'application_status': 'applied'
        },
        {
            'time': '2022-04-05 11:00:00.0',
            'person_name': 'Arthur Applicant',
            'phone': '2134567890',
            'email': 'arthur.applicant@cox.net',
            'company': 'Acme Anvil Corporation',
            'role': 'Wile E. Coyote Revivor',
            'application_status': 'disqualified'
        }
    ]
    return dummy

@pipeline.task(depends_on=part_one__extract_raw_data)
def part_one__transform_1(dataset):
    """
    Define a transformation here! One of [Quarantine, Normalization, Common Status Mapping]
    Args:
        dataset: Input Dataset (from extract, presumably)
    Returns: transformed dataset
    """
    logging.info('Performing Transform 1...')
    return dataset

@pipeline.task(depends_on=part_one__transform_1)
def part_one__transform_2(dataset):
    """
    Define a transformation here! One of [Quarantine, Normalization, Common Status Mapping]
    Args:
        dataset: Input Dataset (from first transform, presumably)
    Returns: transformed dataset
    """
    logging.info('Performing Transform 2...')
    return dataset

@pipeline.task(depends_on=part_one__transform_2)
def part_one__transform_3(dataset):
    """
    Define a transformation here! One of [Quarantine, Normalization, Common Status Mapping]
    Args:
        dataset: Input Dataset (from second transform, presumably)
    Returns: transformed dataset
    """
    logging.info('Performing Transform 3...')
    return dataset

@pipeline.task(depends_on=part_one__transform_3)
def load_data(dataset):
    """
    Loads the supplied Data into the table,
    Nothing needs to be done here as part of the assessment

    Args:
        dataset: data from the last task having a format as follows (also depicted in line 44): [
            {
                'time': '2022-04-05 11:00:00.0',
                'person_name': 'Arthur Applicant',
                'phone': '2134567890',
                'email': 'arthur.applicant@cox.net',
                'company': 'Acme Anvil Corporation',
                'role': 'Wile E. Coyote Revivor',
                'application_status': 'disqualified'
            }
        ]
        Note: that each dictionary will represent a record and the keys of the dictionary MUST match the keys above in order to be added to the database.
    Returns: None
    """
    # Load Data Into Table
    logging.info('Loading Data in SQLite Database...')
    insert_Table_ats(dataset)
```

Your job will be to complete the extraction code to load data from the three companies, write the appropriate transformations, and let the code load the data into a sqlite database.  For illustrative purposes, the extraction function has a dummy dataset hardcoded so that you may follow its evolution through the code as it exists.  

    Hint:  Use the dummy dataset as a checkpoint between writing the load function(s) and the transform function(s).  One could choose to use the dummy dataset to write the transforms before working on the extraction or visa versa.

While the objective of data extraction seems straightforward (extract the data from the `csv`, `xlsx`, and `json` formats, put it in a format that looks like the dummy dataset), the transformations the data requires are more open to interpretation.  To add some guidelines, the transformations we would like the data to undergo (order determined by you) is as follows:
     
* Common Status Mapping - The data includes the applicant’s status for each row.  Each ATS uses their own unique but obvious convention for the status states.  Please map them to a common set of states so that in the final database, the set of states is the same, regardless of the source ATS. 
* Quarantine - Some of the records will contain anomalies that are not possible to alleviate (for the purposes of this exercise).  Devise and execute a rubric by which these anomalous records can be quarantined. Examples of this are limited to:
  * We assume all applicants’ phone numbers are US and 10 digits long (area code + 7 digit number), some will be more or less. 
  * Emails that are missing obvious elements (i.e. `@` , or a `.com`)
* Normalization of data
  * Reformat the phone numbers to a 10 character long alphanumeric string (i.e. 3 digit area code, 3 digit exchange code and 4 digit subscriber number - `9221234567`).  This typically involves removing the `+` or `00` in front of the number and the digits in the front that represent the country code (we assume all numbers  in the data are US and start with either +1 or 001)

Once the extraction and transformations are complete and provided that the data matches the format depicted by the dummy data, the data will then be loaded into a sqlite database.  No candidate input is necessary for the loading function

### Part Two - Analysis

Now put yourself in the shoes of the Analyst that would be analyzing this data and that has access to the database where you loaded the data.  Write two SQL queries against the data that you loaded into the database from Part One - ETL Design that return 
1. The distribution of final applicant states, on a per company basis and include the result. 
1. The distribution of applicant states, prior to disqualification, for the applicants that were disqualified.

The following code excerpt from `src/docker/src/entrypoint.py` is wired to execute on the sqlite db from part one.  All that is necessary is to fill the query variables and the code will print the query and the result at execution!  More information about supported SQL functions is available at https://docs.ponyorm.org/api_reference.html#queries-and-functions

> If you wish to leverage something other than PonyORM, the generated and complete sqlite file is accessible at `data/gen` folder, and you could (for instance) leverage pandas' sql functionality instead)

```
@pipeline.task(depends_on=load_data)
def part_two_query_one(input: None):
    """
    Great place to write the first query of part 2!
    Args:
        input: None
    Returns: None
    """

    logging.info('Running First Query...')
    # Select Statement - This Throws SQL over PonyORM (https://docs.ponyorm.org/queries.html)
    query = """
    *
    FROM ats
    LIMIT 10
    """
    logging.info('Query:\n{}'.format(query))
    data = select_statement(query)
    logging.info('Results:\n{}'.format(
        json.dumps(data, indent = 2)
    ))

@pipeline.task(depends_on=part_two_query_one)
def part_two_query_two(input: None):
    """
    Great place to write the second query of part 2!
    Args:
        input: None
    Returns: None
    """

    logging.info('Running Second Query...')
    # Select Statement - This Throws SQL over PonyORM (https://docs.ponyorm.org/queries.html)
    query = """
    *
    FROM ats
    LIMIT 10
    """
    logging.info('Query:\n{}'.format(query))
    data = select_statement(query)
    logging.info('Results:\n{}'.format(
        json.dumps(data, indent = 2)
    ))
```

For example, if the data in the database from Part One - ETL Design looked something like the following:

| timestamp           |  applicant_name   |   company   |        role_title        |  application_status   |
|:--------------------|:-----------------:|:-----------:|:------------------------:|:---------------------:|
| 2022-02-21 19:08:52 | Arthur Applicant  | Whole Seeds |   Forklift Operator II   |        applied        |        
| 2022-02-25 10:43:45 | Arthur Applicant  | Whole Seeds |   Forklift Operator II   |     disqualified      | 
| 2022-03-04 12:32:32 |   Garrett Boxer   | Whole Seeds |    Packaging Manager     | waiting for interview | 
| 2022-02-23 10:43:45 | Celeste Jobhunter |   U Ship    | Packaging Technician III |        applied        |      
| 2022-02-22 15:51:16 | Arthur Applicant  | Whole Seeds |   Forklift Operator II   | waiting for interview |
| 2022-02-25 05:14:33 | Celeste Jobhunter |   U Ship    | Packaging Technician III |     disqualified      |
| 2022-05-04 18:13:12 | Cindy Roadmaster  |  18 Wheels  |  CDL Logistics Manager   |      offer sent       |

Then the result of the 1st query *should* look something like this:

| company     |  application_status   | percentage |
|:------------|:---------------------:|-----------:|
| Whole Seeds | waiting for interview |        50% |
| Whole Seeds |     disqualified      |        50% |
| U Ship      |     disqualified      |       100% |
| 18 Wheels   |      offer sent       |       100% |

And the result for the 2nd query *should* look something like this (with the caveat being the distribution is not that insteresting, given that for the purposes of illustrating the example, the starting dataset is too small):


| company     | application_status (prior to DQ) | percentage |
|:------------|:--------------------------------:|-----------:|
| Whole Seeds |      waiting for interview       |        50% |
| Whole Seeds |             applied              |        50% |

### Evaluation Criteria

All decisions regarding *how* the engineering are left *entirely* to you, you are free to determine how you perform the extraction, and transformations of the data.  If you feel the urge to 
* write the code in separate python files and call on those, or 
* keep it in the function as displayed, or 
* modify how the data is loaded 
  * format wise, or 
  * maybe want to use a different database, or 
  * just plain decide that the jinja juggle I wrote is just plain weird and that it should be hardcoded, etc... or
* modify how the code executes, then



Using this assessment we seek to understand the following
* what 'good' code looks like to you
* how you think about ETL processes and steps
* how you structure your software in terms of readability and execution by others.

### Submitting your work

Please clone this repository, complete the assessment, and send the entire thing back to us over email!




