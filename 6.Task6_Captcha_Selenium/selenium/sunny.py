from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException
import time
import urllib
import cv
import os.path
#import Image
#import tesseract
from pytesseract import image_to_string
from PIL import Image
#from tesseract import image_to_string

driver=webdriver.Firefox()
driver.get("http://rwss.ap.nic.in/pred/rws_login_frm.jsp")

#for username
elem=driver.find_element_by_name("userId")
elem.clear()
elem.send_keys("guest")

#for password
elem=driver.find_element_by_name("password1")
elem.clear()
elem.send_keys("guess")

#get capcha image
driver.save_screenshot('src.png')
elem=driver.find_element_by_xpath("//*[@id='AutoNumber14']//*/img")
loc=elem.location

image=cv.LoadImage('src.png',True)
out=cv.CreateImage((100,35),image.depth, 3)
cv.SetImageROI(image, (int(loc['x']),int(loc['y']),100,35))
cv.Resize(image,out)
cv.SaveImage('out.jpg',out)
print loc['x']
print loc['y']

capcha=image_to_string(Image.open(open("/home/srikanth/Desktop/out.jpg",'rb')))
elem=driver.find_element_by_name("number")
elem.clear()
print "Captcha",capcha
elem.send_keys(capcha)

#to press enter button
elem=driver.find_element_by_name("submit2")
#elem.send_keys(Keys.RETURN)

if(False):
	driver.get("http://sand.telangana.gov.in/TSSANDNETBANKING/Order/CustomerOrders.aspx")
	select = Select(driver.find_element_by_id("ccMain_ddlDistricts"))
	select.select_by_visible_text("KARIMNAGAR")
	time.sleep(1) 
	driver.find_element_by_id("ccMain_grdstock_rdselstckpoint_0").click()

	time.sleep(1)
	select = Select(driver.find_element_by_id("ccMain_ddlsandpurpose"))
	select.select_by_visible_text("Commercial")


	elem=driver.find_element_by_id("ccMain_txtVehicleNo")
	elem.send_keys("sdhfhks")


	select = Select(driver.find_element_by_id("ccMain_ddldeldistrict"))
	select.select_by_visible_text("SIDDIPET")
	time.sleep(1)
	#sleep 1 second util all mandals are loaded

	select = Select(driver.find_element_by_id("ccMain_ddldelmandal"))
	select.select_by_visible_text("Gajwel")
	time.sleep(1)
	#sleep 1 second util all villages are loaded 


	select = Select(driver.find_element_by_id("ccMain_ddldelvillage"))
	select.select_by_visible_text("Pragnapur")
	time.sleep(1)

if(False):
	print "Hi"
	#resource = urllib.urlopen("http://rwss.ap.nic.in/pred/Captcha/Cap_Img.jsp?refresh=N")
	#output = open("image.jpg","wb")
	#output.write(resource.read())
	#output.close()
	#urllib.urlretrieve("http://rwss.ap.nic.in/pred/Captcha/Cap_Img.jsp", "image.jpg")
	#capcha input here
	#capcha=getText('out.jpg')
	#script_dir = os.path.dirname(os.path.abspath(__file__))
	#im = Image.open(os.path.join(script_dir, 'Desert.jpg'))
	#Image.open(open("path/to/file", 'rb'))