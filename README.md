# data-assessment-data-engineer

Welcome to the solution to the take-home assessment for the data engineer role on the Data Team at Workstep!  

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

Using the datasets from three fictional companies ATS’ (in `/input`) the data pipeline in Python extracts the data from the three data sources, does a few transformations and loads the data into a database.

Extraction and Transformations applied are depicted below
```
# Pipeline steps
@pipeline.task()
def part_one__extract_raw_data(path: str):
    """
    Extracts the raw data from csv, xlsx, json files
    Args:
        path: location of raw data
    Returns: Pandas DataFrame that passes through the rest of the pipeline
    """
    logging.info('Extracting Raw Data...')

    logging.info('scanning input directory...')
    # scans and grabs only files
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

    data = pd.DataFrame()
    #reading each file by format
    for file in onlyfiles:
        filepath = join(path, file)
        fileformat = file.split('.')[-1]
        if fileformat.lower() == 'csv':
            temp = pd.read_csv(filepath, index_col=False)
            temp.drop("Unnamed: 0", axis=1, inplace=True)

        elif fileformat.lower() == 'xlsx':

            temp = pd.read_excel(filepath, engine='openpyxl', index_col=False)
            temp.drop("Unnamed: 0", axis=1, inplace=True)

        elif fileformat.lower() == 'json':

            temp = pd.read_json(filepath)

        #Appending Parent Dataframe with current file data(temp)
        data = pd.concat([data, temp], ignore_index=True )
        #data = data.append(temp, ignore_index=True)   --- The frame.append method is deprecated and will be removed from pandas in a future version


    return data

@pipeline.task(depends_on=part_one__extract_raw_data)
def part_one__transform(dataset):
    """
    Following Transformations are done in the same order
        1. Common Status Mapping - striped, removed underscores and transformed to lower case on "application_status"
        2. Quarantine - removed records with invalid "phone" and "email"
        3. Normalization - applied on "phone"
    Args:
        dataset: pandas dateframe from extract
    Returns: list of dictionaries (acceptable by load function)
    """
    ## Common Status Mapping
    logging.info('Performing Transform 1: Common Status Mapping...')

    # makes application status into lower case, strips and replaces underscores("_")
    dataset["application_status"] = dataset["application_status"].str.lower().str.strip().replace('_', ' ', regex=True)



    ## Quarantine
    logging.info('Performing Transform 2: Quarantine...')

    #Casting "phone" datatype to string
    dataset['phone'] = dataset['phone'].astype(str)

    #extracting records where "phone" length is not either 10 or 11 digits
    phone_records_invalid = dataset[dataset["phone"].apply(lambda x: len(x) not in range(10, 12))]

    #droping the records that are captured above
    dataset.drop(phone_records_invalid.index, inplace=True)

    #extracting records where email format is not "string1@string2.string3"
    email_records_invalid = dataset[
        ~dataset["email"].str.contains("([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+").astype(bool)]

    #appending all invalid records
    quarentine_records = pd.concat([phone_records_invalid,email_records_invalid], ignore_index=True)

    """
    Managing Quarentine options
    1. we can write the records to a remote location where business can access for further investigation
    2. Write to another table in the data lake/repository
    """

    # droping the records that are captured above for email records
    dataset.drop(email_records_invalid.index, inplace=True)



    ## Normalization
    logging.info('Performing Transform 3: Normalization...')

    # Normalizing phone number: matches a number comprised of either 10 digits, or 11 digits if the first number is 1
    dataset['phone'] = dataset["phone"].str.extract("^1?(\d{10})")


    return dataset.to_dict('records')


@pipeline.task(depends_on=part_one__transform)
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


While the objective of data extraction seems straightforward (extract the data from the `csv`, `xlsx`, and `json` formats using pandas DataFrame). 
The following transformations are applied in the same order
* Common Status Mapping -  Regardless of the source application status column will be in lower case, no trailing spaces and no underscores
* Quarantine -
  * Identifies records that have "phone" length not equal to either 10 or 11 
  * Identifies records that have invalid format of "email" i.e., "string1@string2.string3"
  * Managing quarantined options
    1. we can write the records to a remote location where business can access for further investigation
    2. Write to another table in the data lake/repository
* Normalization of data
  * Transforms the "phone" using Regex that matches a number comprised of either 10 digits, or 11 digits if the first number is 1 
  
Once the extraction and transformations are completed and before loading to the database, the pandas Dataframe is transformed into list of dictionaries (the format depicted by the dummy data), then be loaded into a sqlite database.

### Part Two - Analysis

Now write two SQL queries against the data that you loaded into the database from Part One - ETL Design that return 
1. The distribution of final applicant states, on a per company basis and include the result. 
1. The distribution of applicant states, prior to disqualification, for the applicants that were disqualified.

The following solution excerpt from `src/docker/src/entrypoint.py` is wired to execute on the sqlite db from part one.

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
	company,
	application_status,
	printf("%.2f",(COUNT(*) / CAST(SUM(count(*)) OVER (PARTITION BY company) AS float )) * 100)  || "%"  as "percentage"
FROM
	ats
GROUP BY
	company,
	application_status
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
	a.company,
	a.application_status as "application_status(prior to DQ)",
	printf ("%.2f",(COUNT(*) / CAST(SUM(count(*)) OVER (PARTITION BY a.company) AS float)) * 100) || "%" AS "percentage"
FROM
	ats a
	JOIN ( SELECT DISTINCT
			company,
			email
		FROM
			ats
		WHERE
			application_status = 'disqualified') b ON a.company = b.company
	AND a.email = b.email
WHERE
	a.application_status != 'disqualified'
GROUP BY
	a.company, a.application_status
    """
    logging.info('Query:\n{}'.format(query))
    data = select_statement(query)
    logging.info('Results:\n{}'.format(
        json.dumps(data, indent = 2)
    ))

```

### Notes

Part TWO - Query 2 : I used Sub-Select query to calculate the distribution. We can also approach the solution by creating temp table, which is efficient when dealing with large data sets, however consumes space. Since it is small data set and we don't want to spend time on updating data/gen model. I just went ahead with sub select statement. Please find below statement for Temp table.


```
Create TEMP table abc as 
select distinct company, email from ats
where application_status = 'disqualified';
SELECT
	a.company,
	a.application_status,
	printf ("%.2f",
		(COUNT(a.*) / CAST(SUM(count(*)) OVER (PARTITION BY a.company) AS float)) * 100) || "%" AS "Percentage"
FROM
	ats a
	JOIN abc b ON a.company = b.company
		AND a.email = b.email
WHERE
	a.application_status != 'disqualified'
GROUP BY
	a.company,
	a.application_status


```




