import scrapy
from bs4 import BeautifulSoup
from termcolor import colored
import itertools
from itertools import product
import numpy as np
import pandas as pd 

""" 
	This spider scrapes Consumer Price Index (COI)
	data from the U.S. Bureau of Labor Statistics
"""

class CpiSpider(scrapy.Spider):
	name = "CPI"
	# start_requests is a generator fuction
	def start_requests(self):
		urls = ["https://www.bls.gov/news.release/cpi.t01.htm"]
		for url in urls:
			yield scrapy.Request(url=url, callback=self.parse)

	# parse parses the document accessed from the request
	def parse(self, response):
		pretty_page = BeautifulSoup(response.text, features="lxml")
		# Find the table class
		table = pretty_page.find('table', attrs={'class':'regular'})
		# Find the head of the table
		table_head = table.find('thead')
		# We can see from the html that the col names are nested so we need to combine them
		th_r = table_head.find_all('tr')
		if len(th_r) == 2:
			# Distinguish between the nested col names
			header1 = th_r[0].find_all('th')
			header2 = th_r[1].find_all('th')
			# Find the ids from header1 and the headers forom header2 so we can find common elements
			id_list = [tag['id'] for tag in header1]
			head1_text = [tag.text for tag in header1]
			header_list = [tag['headers'] for tag in header2]
			flat_header_list = list(itertools.chain.from_iterable(header_list))
			head2_text = [tag.text for tag in header2]
			# Find the common tags between the two headers
			inter_list = np.intersect1d(id_list, flat_header_list)
			# Find indices of different elements
			diff_list = [i for i, item in enumerate(id_list) if item not in flat_header_list]
			diff_text = []
			for d_ind in diff_list:
				diff_text.append(head1_text[d_ind])
			col_names = []
			for inter in inter_list:
				# Find the index for the common id and headers tags
				id_ind = [i for i, j in enumerate(id_list) if j == inter]
				header_ind = ind_list = [i for i, j in enumerate(flat_header_list) if j == inter]
				for ind in id_ind:
					text1 = head1_text[ind]
					for h_ind in header_ind:
						text2 = head2_text[h_ind]
						col_names.append(text1 + ': ' + text2)
			final_col_names = diff_text + col_names
		else:
			raise ValueError('Table Head Error: len(head)!=2')
		# Find the body of the table
		table_body = table.find('tbody')
		# Find the rows of the table (specifying class keeps us from getting sep rows)
		for t_row in table_body.find_all("tr", {'class':'sep'}): 
			t_row.decompose()
		rows = table_body.find_all('tr')
		# Iterate through the rows to pull data for each col 
		# Where we will append the data from the table
		data = []
		for row in rows:
			# There should only be one category per row
			category = row.find('th')
			cat_text = category.text.strip()
			# Get a list of the column values 
			cols = row.find_all('td')
    		# Strip the text from each HTML element
			cols = [ele.text.strip() for ele in cols]
			cols.insert(0, cat_text)
    		# Append the row data to our list
			data.append(cols) 
		# Make sure that all rows have the same col #
		it = iter(data)
		the_len = len(next(it))
		if not all(len(l) == the_len for l in it):
			raise ValueError('All rows are not the same length!')
		if len(final_col_names) == len(data[0]):
			df = pd.DataFrame(data, columns = final_col_names)
		else:
			raise ValueError('Column length does not match row length')
		df.to_csv('CPI_test_file.csv', header=True, index=False)
