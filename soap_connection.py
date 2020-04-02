import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import requests
import os

class SoapConection:

	headers = { 'Host': '212.36.75.178',
			'Accept-Encoding': '*',
			'Content-Type': 'text/xml',
			'SOAPAction': 'executeSQL',
			#'User-Agent': 'Mozilla/4.0 (compatible; Win32; WinHttp.WinHttpRequest.5)',
			'Charset': 'UTF-8',
			'Authorization': os.environ.get('Authorization')}

	def __init__(self, dbs, url, method, sql):
		self.dbs = dbs
		self.url = url
		self.method = method
		self.sql = sql
		#self.columns = columns


# Static method that sends the request to the web service
	def send_request(self):
		body = """<?xml version="1.0" encoding="UTF-8"?>
		<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"> \
		<SOAP-ENV:Body><ns1:""" + self.method + """ xmlns:ns1="urn:SOAPSQLServer" SOAP-ENV:encodingStyle="http://xml.apache.org/xml-soap/literalxml"> \
		<database xsi:type="xsd:string" SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">""" + self.dbs + """</database> \
		<sqlcmd xsi:type="xsd:string" SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"> \
		""" + self.sql + """ \
		</sqlcmd></ns1:executeSQL></SOAP-ENV:Body></SOAP-ENV:Envelope>
		"""
		body = body.replace('\n', '')
		body = body.replace('\t', '')
		
		# Send request
		self.request = requests.post(self.url, headers=self.headers, data=body)
		return self.request

# Save response in string format
	def get_response(self):
		self.response = self.send_request()
		# Get response
		response_xml_as_string = self.response.text
		self.root = ET.fromstring(response_xml_as_string)
		return self.root

# Create a dataframe with data retrieved from the web service
	def create_df(self):
		table = []
		# Loop for all the rows 
		for row in range(len(self.get_response().findall('.//r'))):
			aux_row = []
			# Loop for all the columns
			for col in self.get_response()[0][0][0][0][2][row]:
				aux_row.append(col.text)
			table.append(aux_row)
		self.df = pd.DataFrame(table,columns=['Codi', 'Estat', 'Direcció'])
		return self.df


def main():

	sql = "SELECT codigo, estcab, direcc FROM con_exp_ihb WHERE cabid BETWEEN 370 AND 380"
	dbs = "bagursa_3test"
	#targetObjectURI = "urn:SOAPSQLServer"
	method = "executeSQL"
	url="http://212.36.75.178/soap/servlet/rpcrouter"
	
	con = SoapConection(dbs, url, method, sql)
	response = con.get_response()
	
	data = con.create_df()
	print(data)


if __name__ == '__main__':
	main()
