import pandas as pd
import numpy as np
import sqlparse
import re


class SQLParser():
    
    def __init__(self):
        
        # tab for SELECT statement
        self.select_tab = '\t' * 2
    
    # первым действием будем заменять все tab'ы в строке на пробелы
    def replace_tabs_from_string(self, sql_string):
        replace_tab = sqlparse.split(sql_string.replace('\n', ' '))
        pretty_string = " ".join(re.split("\s+", replace_tab[0], flags=re.UNICODE))
        return pretty_string

    # вторым - отформатируем строку с помощью sqlparse.format и запишем получившуюся строку в новую переменную
    # print(var) выведет pretty format по умолчанию созданный в sqlparse
    # далее будем работать с обработанной строкой:
    # заменять отступы, 1=1 rule, IN statement и прочие правила из sql guide

    # 1=1 rule
    def replace_where_stmnt(self, sql_string):
        sql_modify = sql_string.replace('WHERE', 'WHERE 1=1 \n AND')
        return sql_modify

    def replace_select_stmnt(self, sql_string):
        if 'SELECT DISTINCT' in sql_string:
            sql_modify = sql_string.replace('SELECT DISTINCT', f'SELECT DISTINCT \n {self.select_tab}')
        else:
            sql_modify = sql_string.replace('SELECT', f'SELECT \n {self.select_tab}')

        return sql_modify