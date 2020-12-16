import requests
from bs4 import BeautifulSoup
from selenium import webdriver 

driver = webdriver.Chrome(r"C:\Users\Asus\Downloads\chromedriver_win32\chromedriver.exe")
url = ('http://www.bidan-delima.org/admin/modul/anggota/ajax_kabupaten.php?id=1')

response = requests.get(url)
response.text

soup = BeautifulSoup(response.content, 'html.parser')
nad = soup.find_all('select')
nad = nad[0]
nad

select_nad = nad.find_all('option')
del select_nad[0]
web = 'http://www.bidan-delima.org/admin/proses.php?filter=&textcari=&mode=&mid='

for all_select in select_nad:
    driver.get(web + all_select.get('value'))
    print(web + all_select.get('value'))
