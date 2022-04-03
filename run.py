from pipeline import Handler
from database import con
token = 'secret_WHETRF7v6E86YaJdaaFG9RyfRdwIwz9plsHBGAVYVug'
databaseId = '89799373e1bc4c459c146aa934a5b655' # общение с лидами

headers = {
    "Accept": "application/json",
    "Notion-Version": "2021-08-16",
    "Content-Type": "application/json",
    "Authorization": "Bearer " + token
}

read_url = f"https://api.notion.com/v1/databases/{databaseId}/query"

link = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'] # задаем ссылку на Гугл таблицы
my_creds = ServiceAccountCredentials.from_json_keyfile_name('gogol.json', link) # формируем данные для входа из нашего json файла
client = gspread.authorize(my_creds) # запускаем клиент для связи с таблицами

Handler(token, databaseId, headers, client,con).main()