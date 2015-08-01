import logging
import os, cgi
import csv, codecs, cStringIO
import cloudstorage as gcs
import webapp2
import string
import getpass
from google.appengine.ext import db
from google.appengine.api import app_identity
import logging
from models import *
logging.getLogger().setLevel(logging.DEBUG)

MAIN_PAGE_HTML = """\
<html>
  <body>
    <form action="/submit" method="post">
	  <h2>Integration Tool </h2>
	  <div>The tool provides a facility to transform a csv file in the GCS cloud storage into the following data upload-gcs-data for "Local Amenities Map".</div>
	  <ol>
		<li> Postcode </li>
		<li> Outcode </li>
		<li> General Practitioner (GP) </li>
		<li> Supermarket </li>
		<li> Train Station </li>
		<li> School </li>
	  </ol>
	  <div>Enter csv file name uploaded in GCS cloud storage (with .csv extension) : <input type="text" name="filename"><div><br/>
	  <div>Enter a model number to be considered for the csv upload : <input type="text" name="model"></div>
	  <br/>
      <div><input type="submit" value="Submit"></div>
    </form>
  </body>
</html>
"""
		
class MainPage(webapp2.RequestHandler):							   
	def get(self):
		self.response.write(MAIN_PAGE_HTML)
		
class TransformData(webapp2.RequestHandler):
	def post(self):
		bucket_name = os.environ.get('upload-gcs-data.appspot.com', app_identity.get_default_gcs_bucket_name())
		self.response.headers['Content-Type'] = 'text/plain'
		self.response.write('Demo GCS Application running from Version: '
                        + os.environ['CURRENT_VERSION_ID'] + '\n')
		self.response.write('Using bucket name: ' + bucket_name + '\n\n')
		bucket = '/' + bucket_name
		filename  = bucket + '/' + cgi.escape(self.request.get('filename'))
		modelNumber = cgi.escape(self.request.get('model'))
		if modelNumber == '1':
			self.AccessPostcodes(filename)
		elif modelNumber == '2':
			self.AccessOutcodes(filename)
		elif modelNumber == '3':
			self.AccessGP(filename)
		elif modelNumber == '4':
			self.AccessSupermarket(filename)
		elif modelNumber == '5':
			self.AccessTrainStation(filename)
		elif modelNumber == '6':
			self.AccessSchool(filename)
		else:
			self.response.write("Model number is not provided")
			
	def AccessGP(self,filename):
		try:
			gp_list = []
			gp_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				name = row['name']
				postcode = row['postcode'].upper().replace(' ','')
				address = row['address']
				query = GP.gql("WHERE postcode = :1", postcode)
				if query.count() !=0  :
					record = query.get()
					record.name = unicode(name, "utf-8")
					record.address = unicode(address, "utf-8")
					record.lat_long = point
					gp_upd_list.append(record)
				else:
					gp = GP(name=unicode(name, "utf-8"),
							address=unicode(address, "utf-8"),
							postcode=unicode(postcode, "utf-8"),
							lat_long = point)
					gp_list.append(gp)
					self.batchPut(gp_list, 10000)
			if len(gp_list) > 0 :
				self.batchPut(gp_list, 10000)
			if len(gp_upd_list) > 0 :
				self.batchPut(gp_upd_list, 10000)
			self.response.write('\n\nCreated or updated GP entities successfully.\n\n')
			gcs_file.close()

		except Exception, e: 
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')

	def AccessOutcodes(self,filename):
		try:
			outcode_list = []
			outcode_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				code= row['outcode'].upper().replace(' ','')
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				query = Outcode.gql("WHERE outcode = :1", code)
				if query.count() !=0  :
					record = query.get()
					record.lat_long = point
					outcode_upd_list.append(record)
				else:
					outcode = Outcode(outcode=code,
								  lat_long = point)
					outcode_list.append(outcode)
					self.batchPut(outcode_list, 10000)
			if len(outcode_list) > 0 :
				self.batchPut(outcode_list, 10000)
			if len(outcode_upd_list) > 0 :
				self.batchPut(outcode_upd_list, 10000)
			self.response.write('\n\nCreated or updated outcode entities successfully.\n\n')
			gcs_file.close()

		except Exception, e:
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')
								
	def AccessPostcodes(self, filename):
		try:
			postcode_list =  []
			postcode_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				code = row['postcode'].upper().replace(' ','')
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				query = Postcode.gql("WHERE postcode = :1", code)
				if query.count() !=0  :
					record = query.get()
					record.lat_long = point
					postcode_upd_list.append(record)
				else:
					postcode = Postcode(postcode=code,
								    lat_long = point)
					postcode_list.append(postcode)
			if len(postcode_list) > 0 :
				self.batchPut(postcode_list, 10000)
			if len(postcode_upd_list) > 0 :
				self.batchPut(postcode_upd_list, 10000)
			self.response.write('\n\nCreated or updated postcode entities successfully \n\n')
			gcs_file.close()
			
		except Exception, e: 
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')
								
	def AccessTrainStation(self,filename):
		try:
			train_list = []
			train_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				name = row['name']
				query = TrainStation.gql("WHERE name = :1", name)
				if query.count() !=0  :
					record = query.get()
					record.lat_long = point
					train_upd_list.append(record)
				else:
					trainStation = TrainStation(name=row['name'],
											lat_long = point)				
					train_list.append(trainStation)
					self.batchPut(train_list, 10000)
			if len(train_list) > 0 :
				self.batchPut(train_list, 10000)
			if len(train_upd_list) > 0 :
				self.batchPut(train_upd_list, 10000)
			self.response.write('\n\nCreated or updated train station entities successfully.\n\n')
			gcs_file.close()

		except Exception, e: 
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')
								
	def AccessSupermarket(self, filename):
		try:
			Supermarket_list = []
			Supermarket_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				name = row['name']
				postcode = row['postcode'].upper().replace(' ','')
				address = row['address']
				query = Supermarket.gql("WHERE postcode = :1", postcode)
				if query.count() !=0  :
					record = query.get()
					record.name = unicode(name, "utf-8")
					record.address = unicode(address, "utf-8")
					record.lat_long = point
					Supermarket_upd_list.append(record)
				else:
					sp = Supermarket(name = unicode(name, "utf-8"),
								 address = unicode(address, "utf-8"),
								 postcode = unicode(postcode, "utf-8"),
								 lat_long = point)
					Supermarket_list.append(sp)
					self.batchPut(Supermarket_list, 10000)
			if len(Supermarket_list) > 0 :
				self.batchPut(train_list, 10000)
			if len(Supermarket_upd_list) > 0 :
				self.batchPut(Supermarket_upd_list, 10000)
			self.response.write('\n\nCreated or updated supermarket entities successfully.\n\n')
			gcs_file.close()

		except Exception, e: 
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')

	def AccessSchool(self,filename):
		try:
			School_list = []
			School_upd_list = []
			gcs_file=gcs.open(filename)
			reader=csv.DictReader(gcs_file)
			for row in reader:
				if len(row['latitude']) != 0:
					lat = float(row['latitude'])
				else:
					lat = 0.00
				if len(row['longitude']) != 0:
					long= float(row['longitude'])
				else:
					long = 0.00
				coord = {'latitude': lat, 'longitude': long}	
				point = '{latitude}, {longitude}'.format(**coord)
				name = row['name']
				postcode = row['postcode'].upper().replace(' ','')
				address = row['address']
				query = School.gql("WHERE postcode = :1", postcode)
				if query.count() !=0  :
					record = query.get()
					record.name = unicode(name, "utf-8")
					record.address = unicode(address, "utf-8")
					record.lat_long = point
					School_upd_list.append(record)
				else:
					school = School(name = unicode(name, "utf-8"),
										address = unicode(address, "utf-8"),
										postcode = unicode(postcode, "utf-8"),
										lat_long = point)
					School_list.append(school)
					self.batchPut(School_list, 10000)
			if len(School_list) > 0 :
				self.batchPut(School_list, 10000)
			if len(School_upd_list) > 0 :
				self.batchPut(School_upd_list, 10000)
			self.response.write('\n\nCreated or updated school entities successfully.\n\n')
			gcs_file.close()

		except Exception, e: 
			logging.exception(e)
			self.response.write('\n\nThere was an error running the program! '
								'Please check the logs for more details.\n')

	def batchPut(self, entityList, batchSize=10000):
		putList = []
		count = len(entityList)
		while count > 0:
			batchSize = min(count,batchSize)
			putList = entityList[:batchSize]
			try:
				db.put(putList)
				entityList = entityList[batchSize:]
				count = len(entityList)
			except TooManyError,TooLargeError:
				batchSize = batchSize/2

app = webapp2.WSGIApplication([('/', MainPage),
('/submit', TransformData)
],debug=True)