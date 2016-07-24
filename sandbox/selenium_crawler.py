""" Scrape information from PlanDay """

from selenium import webdriver
from bs4 import BeautifulSoup
from webbrowser import open_new_tab


BROWSER_PATH = '/opt/homebrew-cask/Caskroom/firefox/42.0/Firefox.app'
#BROWSER_PATH = '~/Applications/Chrome\ Apps.localized/Default\ apdfllckaahabafndbhieahigkjlhalf.app'
BROWSER_PATH = '/usr/local/Cellar/chromedriver/2.20/bin/chromedriver'
PLANDAY_LOGIN_URL = 'https://valkfleet.planday.com'
browser = webdriver.Chrome(executable_path=BROWSER_PATH)
browser.get(PLANDAY_LOGIN_URL)
browser.find_element_by_id('Username').clear()
browser.find_element_by_id('Username').send_keys('loic.jounot')
browser.find_element_by_id('Password').clear()
browser.find_element_by_id('Password').send_keys('klaesgrw')
browser.find_element_by_id('submitButton').click()

import pdb; pdb.set_trace()
# print(browser.page_source)x
# with open('home.html', 'w+') as f:
#     f.write(browser.page_source)
#open_new_tab('home.html')
browser.find_element_by_link_text('Employees').click()
content = browser.page_source

soup = BeautifulSoup(content, 'html5lib')
fleets = soup.find_all(name='a', attrs={'class', 'l1'})
for fleet in fleets:
    print(fleet)

#browser.find_element_by_partial_link_text('river').click()
#//*[@id="ctl00_M_CM_CM_emps_HRMTreeviewControl_tree_employeeGroup35604"]