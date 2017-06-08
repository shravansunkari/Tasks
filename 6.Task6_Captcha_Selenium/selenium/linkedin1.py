import unittest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
import time
import csv
import numpy as np
import pandas as pd

#Get data from csv
names=['Name','image_link','Company Name','Biography']
dataframe =pd.read_csv('/home/shravankumar/Desktop/global_conference_pastspeakers.csv', usecols=names)
	
name_list=dataframe['Name']
#print name_list
company_list=dataframe['Company Name']
#print company_list

#initialize selenium
driver = webdriver.Chrome()
user_name="email"
password="pass"
url='https://www.linkedin.com'
driver.get(url)


elem=driver.find_element_by_id('login-email')
elem.send_keys(user_name)

elem=driver.find_element_by_id('login-password')
elem.send_keys(password)

elem=driver.find_element_by_id('login-submit')
elem.send_keys(Keys.RETURN)

with open('linkedin_data.csv','w') as csvfile:
	writer=csv.writer(csvfile,quoting=csv.QUOTE_NONE,delimiter=';',lineterminator='\n')
	writer.writerow(['Name','Company Name','Linkedin_profile'])
	for names in range(0,len(name_list)):
		elem=driver.find_element_by_id('main-search-box')
		elem.clear()
		elem.send_keys(name_list[names]+" "+company_list[names])
		elem.send_keys(Keys.RETURN)
		try:
			e=driver.find_element_by_xpath("//div[@class='bd']/h3/a")
			profile_link=e.get_attribute('href')
			print name_list[names]+"       "+company_list[names]+"      "+profile_link
			writer.writerow([name_list[names],company_list[names],profile_link])
		except NoSuchElementException:
			pass
		time.sleep(1)
		
csvfile.close