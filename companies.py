import csv
import os
import shutil
import urllib.request
import zipfile
from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, InnerDoc, Object, Byte, Nested, Short
from elasticsearch_dsl.connections import connections

companies_zip_url = 'http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2019-04-01.zip'
companies_zip_file = 'BasicCompanyDataAsOneFile-2019-04-01.zip'
# Directory objects?
file_path = os.path.dirname(os.path.realpath(__file__))
project_path = os.path.normpath(os.path.join(file_path))
data_path = os.path.join(project_path, 'data')
companies_csv_filename = 'BasicCompanyDataAsOneFile-2019-04-01.csv'
companies_csv_filepath = os.path.join(data_path, companies_csv_filename)

connections.create_connection(hosts=['localhost'])


class Address(InnerDoc):
	care_of = Text(fields={'raw': Keyword()})
	po_box = Text(fields={'raw': Keyword()})
	line1 = Text(fields={'raw': Keyword()})
	line2 = Text(fields={'raw': Keyword()})
	town = Text(fields={'raw': Keyword()})
	county = Text(fields={'raw': Keyword()})
	country = Text(fields={'raw': Keyword()})
	post_code = Text(fields={'raw': Keyword()})


class Accounts(InnerDoc):
	ref_day = Byte()
	ref_month = Byte()
	next_due = Date(format='dd/MM/yyyy')
	last_made_up = Date(format='dd/MM/yyyy')
	category = Keyword()


class Returns(InnerDoc):
	next_due = Date(format='dd/MM/yyyy')
	last_made_up = Date(format='dd/MM/yyyy')


class Mortgages(InnerDoc):
	charges = Short()
	outstanding = Short()
	part_satisfied = Short()
	satisfied = Short()


class LimitedPartnerships(InnerDoc):
	general_partners = Short()
	limited_partners = Short()


class PreviousName(InnerDoc):
	company_name = Text(analyzer='snowball', fields={'raw': Keyword()})
	date = Date(format='dd/MM/yyyy')


class Company(Document):
	"""Class representing the searchable companies metadata"""
	name = Text(analyzer='snowball', fields={'raw': Keyword()})
	number = Keyword()
	registered_address = Object(Address)
	category = Keyword()
	status = Keyword()
	country_of_origin = Keyword()
	dissolution = Date(format='dd/MM/yyyy')
	incorporation = Date(format='dd/MM/yyyy')

	accounts = Object(Accounts)
	returns = Object(Returns)
	mortgages = Object(Mortgages)
	SIC_code = [Text(analyzer='snowball', fields={'raw': Keyword()}), Text(analyzer='snowball', fields={'raw': Keyword()}), Text(analyzer='snowball', fields={'raw': Keyword()}), Text(analyzer='snowball', fields={'raw': Keyword()})]
	limited_partnerships = Object(LimitedPartnerships)
	URI = Keyword()
	previous_name = Nested(PreviousName)
	confirmation_statement = Object(Returns)

	def add_address(self, care_of, po_box, line1, line2, town, county, country, post_code):
		self.registered_address.update(
			Address(care_of=care_of,
					po_box=po_box,
					line1=line1,
					line2=line2,
					town=town,
					county=county,
					country=country,
					post_code=post_code))

	def age(self):
		return datetime.now() - self.incorporation

	class Index:
		"""The index that all instances of this metadata will be saved to"""
		name = 'companies'
		settings = {
			"number_of_shards": 1,
			"mapping.ignore_malformed": True,
		}

	def save(self, ** kwargs):
		"""Saves the current item to the index"""
		#self.lines = len(self.body.split())
		return super(Company, self).save(** kwargs)

	def is_published(self):
		return datetime.now() >= self.published_from


def download():
	"""Download the csv from companies house"""
	print('{time} Downloading companies house zip'.format(time=datetime.now()))
	with urllib.request.urlopen(companies_zip_url) as response, open(companies_zip_file, 'wb') as out_file:
		shutil.copyfileobj(response, out_file)


def unzip():
	print('{time} Unzipping companies house zip'.format(time=datetime.now()))
	with zipfile.ZipFile(companies_zip_file, 'r') as zipfilename:
		zipfilename.extractall("data")


def company_count():
	"""Check how many rows to ingest"""
	row_count = 0
	with open(companies_csv_filepath, 'r', encoding='utf8') as csvin:
		companies = csv.reader(csvin)

		for company in companies:
			row_count += 1

	print('{time} Rows to ingest {x}'.format(x=row_count, time=datetime.now()))


def ingest():
	"""Ingest everything in the csv into the cluster"""
	# create the mappings in elasticsearch
	Company.init()

	row_count = 0
	with open(companies_csv_filepath, 'r', encoding='utf8') as csvin:
		companies = csv.DictReader(csvin, skipinitialspace=True)

		for row in companies:
			try:
				for key in row:
					if not row[key]:
						row[key] = None
				address = Address(care_of=row['RegAddress.CareOf'],
								  po_box=row['RegAddress.POBox'],
								  line1=row['RegAddress.AddressLine1'],
								  line2=row['RegAddress.AddressLine2'],
								  town=row['RegAddress.PostTown'],
								  county=row['RegAddress.County'],
								  country=row['RegAddress.Country'],
								  post_code=row['RegAddress.PostCode'])

				accounts = Accounts(ref_day=row['Accounts.AccountRefDay'],
									ref_month=row['Accounts.AccountRefMonth'] if row['Accounts.AccountRefMonth'] else None,
									next_due=row['Accounts.NextDueDate'],
									last_made_up=row['Accounts.LastMadeUpDate'],
									category=row['Accounts.AccountCategory'] if row['Accounts.AccountCategory'] else None)

				returns = Returns(next_due=row['Returns.NextDueDate'], last_made_up=row['Returns.LastMadeUpDate'])

				confirmation_statement = Returns(next_due=row['ConfStmtNextDueDate'], last_made_up=row['ConfStmtLastMadeUpDate'])

				mortgages = Mortgages(charges=row['Mortgages.NumMortCharges'],
									  outstanding=row['Mortgages.NumMortOutstanding'],
									  part_satisfied=row['Mortgages.NumMortPartSatisfied'],
									  satisfied=row['Mortgages.NumMortSatisfied'])

				limited_partnerships = LimitedPartnerships(general_partners=row['LimitedPartnerships.NumGenPartners'],limited_partners=row['LimitedPartnerships.NumLimPartners'])

				previous_name = [PreviousName(name=row['PreviousName_1.CompanyName'], date=row['PreviousName_1.CONDATE']),
								 PreviousName(name=row['PreviousName_2.CompanyName'], date=row['PreviousName_2.CONDATE']),
								 PreviousName(name=row['PreviousName_3.CompanyName'], date=row['PreviousName_3.CONDATE']),
								 PreviousName(name=row['PreviousName_4.CompanyName'], date=row['PreviousName_4.CONDATE']),
								 PreviousName(name=row['PreviousName_5.CompanyName'], date=row['PreviousName_5.CONDATE']),
								 PreviousName(name=row['PreviousName_6.CompanyName'], date=row['PreviousName_6.CONDATE']),
								 PreviousName(name=row['PreviousName_7.CompanyName'], date=row['PreviousName_7.CONDATE']),
								 PreviousName(name=row['PreviousName_8.CompanyName'], date=row['PreviousName_8.CONDATE']),
								 PreviousName(name=row['PreviousName_9.CompanyName'], date=row['PreviousName_9.CONDATE']),
								 PreviousName(name=row['PreviousName_10.CompanyName'], date=row['PreviousName_10.CONDATE'])]

				company = Company(name=row['CompanyName'],
								  number=row['CompanyNumber'],
								  registered_address=address,
								  category=row['CompanyCategory'],
								  status=row['CompanyStatus'],
								  country_of_origin=row['CountryOfOrigin'],
								  dissolution=row['DissolutionDate'],
								  incorporation=row['IncorporationDate'],
								  accounts=accounts,
								  returns=returns,
								  mortgages=mortgages,
								  SIC_code=[row['SICCode.SicText_1'], row['SICCode.SicText_2'], row['SICCode.SicText_3'], row['SICCode.SicText_4']],
								  limited_partnerships=limited_partnerships,
								  URI=row['URI'],
								  previous_name=previous_name,
								  confirmation_statement=confirmation_statement)
				company.save()
				row_count += 1
				if (row_count % 1000) == 0:
					print('{time} Ingested {x}'.format(x=row_count, time=datetime.now()))
			except UnicodeDecodeError:
				print('{time} unicode error'.format(time=datetime.now()))


	# create and save and article
	#article = Company(meta={'id': 42}, title='Hello world!', tags=['test'])
	#article.body = ''' looong text '''
	#article.published_from = datetime.now()
	#article.save()

	#article = Company.get(id=42)
	#print(article.is_published())

	# Display cluster health
	print(connections.get_connection().cluster.health())


if __name__ == "__main__":
	if not os.path.isfile(companies_zip_file):
		download()
	if not os.path.isfile(companies_csv_filepath):
		unzip()

	company_count()

	from elasticsearch_dsl import Index
	i = Index('companies')
	i.delete()

	ingest()
