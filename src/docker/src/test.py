from os import listdir
from os.path import isfile, join
import pandas as pd
import json
import sys
from dataprep.clean import clean_phone
import xlrd




path = '/Users/kurshidyalavarthy/Desktop/workstep_Assignment/data-assessment-data-engineer-main/input'


#data = []
data = pd.DataFrame()
onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]


for file in onlyfiles:
    filepath = join(path, file)
    fileformat = file.split('.')[-1]
    if fileformat.lower() == 'csv':
        temp = pd.read_csv(filepath, index_col=False)
        temp.drop("Unnamed: 0", axis=1, inplace = True)

    elif fileformat.lower() == 'xlsx':
        print("Invoke XSLX Reader")

        temp = pd.read_excel(filepath,engine='openpyxl', index_col=False)
        temp.drop("Unnamed: 0", axis=1, inplace = True)
    elif fileformat.lower() == 'json':
        print("Invoke Json reader")

        temp = pd.read_json(filepath)

    #data = pd.concat([data, temp], ignore_index=True)
    data = data.append(temp, ignore_index=True)
    print(temp.tail())
    #data.extend(temp.to_dict('records'))


print(data.shape[0])



# converting and overwriting values of  column
data["application_status"] = data["application_status"].str.lower().str.strip().replace('_', ' ', regex=True)

# reomvig under scores
#data["application_status"] = data["application_status"].replace('_', ' ', regex=True)

# cleaned = clean_phone(data,"phone", inplace=True)
#
# print(cleaned)
#
# cleaned.to_csv("cleaned.csv")

data['phone'] = data['phone'].astype(str)
dataTypeSeries = data.dtypes

print(dataTypeSeries)





quarentine_records = data[data["phone"].apply(lambda x: len(x) not in range(10,12) )]
print("1st",quarentine_records.shape[0])

data.drop(quarentine_records.index, inplace=True)

email_quarentine_records = data[~data["email"].str.contains("([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+").astype(bool)]

quarentine_records = quarentine_records.append(email_quarentine_records,ignore_index=True )
print("2nd",quarentine_records.shape[0])

data.drop(email_quarentine_records.index, inplace=True)

#quarentine_records1 = data.loc[(data["phone"].map(len) < 10) | (data["phone"].map(len) > 11 )]

#data.drop(quarentine_records.index, inplace=True)

data['phone'] = data["phone"].str.extract("^1?(\d{10})")
# data["invalid_phone"]= data["phone"][~data["phone"].str.contains("^1?\d{10}").astype(bool)]
# # #data["twelve"]= data["phone"][data["phone"].str.contains('^\d{12}$').astype(bool)]
# # data["twelve"]= data["phone"].str.contains('^\d{12}$').astype(bool)
#
# data["Invalid_email"] = data["email"][~data["email"].str.contains("[^@]+@[^@]+\.[^@]+").astype(bool)]
    # data['email'].str.contains(r'[^@]+@[^@]+\.[^@]+')

print(data.shape[0])
print(quarentine_records.shape[0])
print(email_quarentine_records.shape[0])

data.to_csv('results.csv')
quarentine_records.to_csv("quarentine.csv")
