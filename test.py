from bs4  import BeautifulSoup
import requests

headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'}


res = requests.get('https://www.bruegel.org/dataset/european-natural-gas-imports', headers=headers)

soup = BeautifulSoup(res.text, 'html.parser')
print(soup.find('a', title='Download data')['href'])
print('-------------------------------------------------')
