import re
import pandas as pd
import requests

class Handler:

    def __init__(self, token, databaseId, headers, client, con):

        self.client = client
        self.token = token
        self.databaseId = databaseId
        self.headers = headers
        self.con = con

        self.df_notion = None
        self.df_postgre = None

    def _flatten(self, d, parent_key='', sep='/'):
        """Рекурсивная функция принимает словарь с вложенными словарями и
        разворачивает его в одноэтажный словарь с суммированными названиями ключей
        через разделитель sep"""

        items = []
        for k, v0 in d.items():

            # Обработка случая, когда очередной словарь вложен в list, и его нужно развернуть
            if isinstance(v0, list) and len(v0) != 0:
                v = v0[0]
            else:
                v = v0

            # Для первого запуска, инициируем ключ, для последующих - суммируем названия
            if parent_key == '':
                new_key = k
            else:
                new_key = parent_key + sep + k

            # Присваиваем итоговому словарю новую строку,или разворачиваем дальше
            if isinstance(v, dict):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_notion(self):
        read_url = f"https://api.notion.com/v1/databases/{self.databaseId}/query"
        payload = {"page_size": 100}

        # Выгружаем данные из Notion в json формате

        res = requests.request("POST", read_url, json=payload, headers=self.headers)
        json = res.json()
        col_flag = False  # Флаг записи заголовков в новый df
        for i in range(len(json['results'])):

            if isinstance(json['results'][i]['properties']['Аккаунт']['select'], dict):
                dict_flat = self._flatten(json['results'][i])

                # Из первой ненулевой строки формируем названия полей
                if col_flag == False:
                    df = pd.DataFrame(columns=dict_flat.keys())
                    col_flag = True

                df = df.append(dict_flat, ignore_index=True)

        tags = [
            'plain_text'
            , 'name'
            , 'date'
            , 'url'
            , 'date/start'
            , 'date/end'
            , 'date/time_zone'
            , 'Ответ/rich_text'
        ]

        reg = '.+' + '$|.+'.join(tags)

        # Удаляем лишние поля по суммарному регулярному выражению
        i = 0
        while i < len(df.columns):
            if not (re.match(reg, df.columns[i])) or re.match('.+FU.+', df.columns[i]):
                df.drop(labels=df.columns[i], axis=1, inplace=True)
                i -= 1
            i += 1

        # Оставляем информативную часть названия полей
        for i in range(len(df.columns)):
            col = df.columns[i].split('/')
            if col[len(col) - 2] == 'date':
                renamed = col[1] + '/' + col[len(col) - 1]
            else:
                renamed = col[1]
            df.rename(columns={df.columns[i]: renamed}, inplace=True)

        df = df.loc[df['Профиль'].notna()]
        self.df_notion = df

    def refresh_google_sheet(self):
        print(self.df_notion.columns)
        print('----------')
        print(self.df_postgre.columns)
        df_new = self.df_notion.merge(self.df_postgre, right_on='url', left_on='Профиль')
        df_new = df_new.applymap(lambda x: x if type(x) is object else str(x))

        lg_sheet = self.client.open('Гоголь').worksheet("lead_gen")
        lg_sheet.clear()
        lg_sheet.update([df_new.columns.values.tolist()] + df_new.values.tolist())

    def get_postgre(self):
        self.df_postgre = pd.read_sql('''select * from link.potential_clients where send_status = 'con_sended' ''', self.con)

    def main(self):
        self.get_notion()
        self.get_postgre()
        self.refresh_google_sheet()




