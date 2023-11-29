from bs4  import BeautifulSoup
import requests

res = requests.get('https://www.bruegel.org/dataset/european-natural-gas-imports')

soup = BeautifulSoup(res.text, 'html.parser')
print(soup)
print(soup.find_all('a'))
