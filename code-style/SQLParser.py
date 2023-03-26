import pandas as pd
import numpy as np
import sqlparse
import re
import warnings
import SQLFunctions as funcs
import SQLParserError
import logging
from datetime import datetime


class SQLParser:
    
    def __init__(self):
        
        self.missing_words = funcs.missing_words
        
        self.funcs_up = [i.upper() for i in funcs.all_funcs]
        self.keywords_up = [key for key, value in sqlparse.keywords.KEYWORDS.items()] + \
            [key for key, value in sqlparse.keywords.KEYWORDS_COMMON.items()] + \
            [key for key, value in sqlparse.keywords.KEYWORDS_HQL.items()] + \
            [key for key, value in sqlparse.keywords.KEYWORDS_MSACCESS.items()] + \
            [key for key, value in sqlparse.keywords.KEYWORDS_ORACLE.items()] + \
            [key for key, value in sqlparse.keywords.KEYWORDS_PLPGSQL.items()] + \
            [i.upper() for i in self.missing_words] + \
            self.funcs_up
        
        self.count_functions = 0
        self.window_funcs_not_found = 0
        self.create_table_not_found = 0
        
        self.link_sql_code_style_guide = 'https://wiki.tele2.ru/pages/viewpage.action?pageId=139127857&preview=/139127857/139128786/SQL%20StyleGuide.pdf'
        
        self.row_max_symbols_df = pd.DataFrame()
        
        # Gets or creates a logger
        self.logger = logging.getLogger(__name__)
        # set log level
        self.logger.setLevel(logging.INFO)
        
        # define file handler and set formatter
        #dt = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        #file_handler = logging.FileHandler(f'SQLParser-logfile-{dt}.log')
        #formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        #file_handler.setFormatter(formatter)
        
        # add file handler to logger
        #self.logger.addHandler(file_handler)
        
       
    def simple_print_sql(self, sql_string: str, **kwargs):
        '''
        Функция преобразует SQL код в соответствии с форматом sqlparse.
        На вход может принимать все аргументы метода sqlparse.format()
        '''
        return print(sqlparse.format(sql_string, **kwargs))
    
    
    def default_string(self, raw: str):
        '''
        Функция преобразует SQL код в соответствии с форматом sqlparse и преднастроенными параметрами.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        return sqlparse.format(
            raw, 
            reindent=True, 
            identifier_case='lower', 
            keyword_case='upper',
            indent_width=4,
            use_space_around_operators=True
        )
    
    
    def functions_upper_case_sub(self, raw: str) -> str:
        '''
        Если ключевое слово описывает функцию и написано в нижнем регистре,
        функция изменяет регистр ключевого слова на верхний.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        self.funcs_low = [i.lower() for i in self.funcs_up]
        assert len(self.funcs_low) == len(self.funcs_up), 'len(funcs_up) != len(funcs_low)'

        mapping = dict(zip(self.funcs_low, self.funcs_up))
        pattern = re.compile(r'\b('+'|'.join(mapping)+r')\b')
        
        self.count_functions += 1
        
        return pattern.sub(lambda m: mapping[m.group(0)], raw)
    
    
    def warning_wrong_words_warn(self, raw: str):
        '''
        Функция проверяет совместимость ключевых слов, указанных в словаре.
        Словарь заполняется вручную.
        '''
        
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        pattern = re.compile(r'[0-9_\w][_$#\w]*')
        mapping = {
            'sel': 'SELECT',
            'del': 'DELETE',
            'ins': 'INSERT'
        } # список может дополняться

        self.wrong_words = []
        for m in re.findall(pattern, raw):
            if m in mapping.keys():
                self.wrong_words.append(m)
        self.warning_wrong_words_logs = \
        f'''
        CodeStyle Warning in warning_wrong_words_warn(): В запросе найдено {len(self.wrong_words)} специфичных ключевых слов, которые не соответствуют правилам.
        (SQL Style Guide. Общие конструкции. 
        Ссылка: {self.link_sql_code_style_guide})
        '''
        self.count_functions += 1
        
        if self.wrong_words:
            return self.logger.warning(self.warning_wrong_words_logs)                
        else:
            return self.logger.info('warning_wrong_words_warn() - success')
        
    
    def whitespaces_sub(self, raw: str) -> str:
        '''
        Deprecated. Сделать коммит в репозиторий с функцией и без нее.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        pattern1 = re.compile(r'[0-9_\w][_$#\w]*\s+,')
        exceptions = ['! =', '> =', '< =']
        
        wrong_items1 = []
        for m in re.findall(pattern1, raw):
            wrong_items1.append(m)
        right_items1 = [re.sub(re.compile(r'\s+'), '', i) for i in wrong_items1]
        
        pattern2 = re.compile(r'\s*[^!]=\s*|\s*<[^>]\s*|\s*[^<]>\s*')
        wrong_items2 = []
        for m in re.findall(pattern2, raw):
            wrong_items2.append(m)
        right_items2 = [i.strip().replace('', ' ') for i in wrong_items2]
        
        self.wrong_items = wrong_items1 + wrong_items2
        self.right_items = right_items1 + right_items2
        
        keyword_mapping = dict(zip(self.wrong_items, self.right_items))
        #pattern3 = re.compile(r'\S*\([_$#\w\s\n,\.\'\"\p{L}]*\)\S*')#находит все круглые скобки и их содержимое; если нет указанных пробелов, то находит все знаки, стоящие вокруг скобок до ближайших пробелов
                                                                    #нужно исключить случаи, когда перед скобкой идет функция TO DO
        raw = pattern1.sub(lambda m: keyword_mapping[m.group(0)], raw)
        raw = pattern2.sub(lambda m: keyword_mapping[m.group(0)], raw)        
        
        script_by_rows = [tup for tup in raw.split('\n')]
        for row in script_by_rows:
            for ex in exceptions:
                if ex in row:
                    #print(row)
                    if '(' in row:
                        re_val = row.replace('(', '\(')
                        pattern = re.compile(f"{re_val}")
                    elif ')' in row:
                        re_val = row.replace(')', '\)')
                        pattern = re.compile(f"{re_val}")
                    else:
                        re_val = re.compile(f"{row}")
                        pattern = re.compile(f"{re_val}")
                    
                    right_values = row.replace(ex, ex.replace(' ', ''))
                    raw = pattern.sub(right_values, raw)
        return raw

        
    def where_clause_sub(self, raw: str) -> str:
        '''
        Функция корректирует выражение WHERE и добавляет "1 = 1" после ключевого слова.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        script_by_rows = [tup for tup in raw.split('\n')]
        wrong_items = []
        for row in script_by_rows:
            if 'WHERE' in row:
                if 'WHERE 1 = 1' not in row:
                    wrong_items.append(row)
        
        for i in wrong_items:
            re_val = i.replace('(', '\(').replace(')', '\)')
            pattern = re.compile(f'{re_val}')
            right_values = i.replace('WHERE ', 'WHERE 1 = 1\n    AND ')
            raw = pattern.sub(right_values, raw)
        
        self.count_functions += 1
        
        return raw
        
        
    def group_rules_warn(self, raw: str):
        '''
        Функция проверяет буквенное наименование колонок в выражениях GROUP BY, HAVING, ORDER BY.
        '''
        
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'

        pattern = re.compile(r'GROUP\sBY\b\s[\d\s?\n?,]+|ORDER\sBY\b\s[\d\s?\n?,]+|HAVING\s[\d\s?\n?,]+')
        self.wrong_string = []
        for m in re.findall(pattern, raw):
            self.wrong_string.append(m)
        
        self.group_rules_logs = \
        f'''
        CodeStyle Warning in group_rules_warn(): При использовании GROUP BY, HAVING, ORDER BY необходимо использовать буквенное наименование колонок. 
        (SQL Style Guide.Общие конструкции. Ссылка: {self.link_sql_code_style_guide})
        '''
        
        self.count_functions += 1
        
        if self.wrong_string:
            return self.logger.warning(self.group_rules_logs)
        else:
            return self.logger.info('group_rules_warn() - success')
        
        
    def row_max_symbols_warn(self, raw: str):
        '''
        Функция проверяет максимальное количество символов в строке - не более 80.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        self.too_many_symbols = [tup for tup in enumerate(raw.split('\n')) if len(tup[1]) > 80]
        self.row_max_symbols_df = pd.DataFrame(self.too_many_symbols, columns=['row_number', 'row_value'])
        self.row_max_symbols_df['fact_length'] = [len(row) for row in self.row_max_symbols_df['row_value']]
        self.row_max_symbols_df.set_index('row_number', inplace=True)
        row_max_symbols_df_global = self.row_max_symbols_df
        self.row_max_symbols_logs = \
        f'''
        CodeStyle Warning in row_max_symbols_warn(): Максимальное количество символов в строке – не более 80. (SQL Style Guide. Общие конструкции. 
        Ссылка: {self.link_sql_code_style_guide}). 
        Перечень строк с ошибками сохранен в объекте row_max_symbols_df_global.
        '''
        
        self.count_functions += 1
        
        if self.too_many_symbols:
            return self.logger.warning(self.row_max_symbols_logs)
        else:
            return self.logger.info('row_max_symbols_warn() - success')
    
        
    def create_table_warn(self, raw: str):
        '''
        Функция проверяет наличие конструкции CREATE TABLE AS SELECT … WITH NO DATA.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'

        self.wrong = []
        self.pattern = re.compile(r'with no data', re.IGNORECASE)
        for m in re.findall(self.pattern, raw):
            self.wrong.append(m)
        
        self.create_table_logs = \
        f'''
        CodeStyle Warning in create_table_warn(): Использование CREATE TABLE AS SELECT … WITH NO DATA в финальной версии скрипта не допускается. 
        (SQL Style Guide. Общие конструкции. Ссылка: {self.link_sql_code_style_guide})
        '''
        
        self.count_functions += 1
        
        if self.wrong:
            return self.logger.warning(self.create_table_logs)
        else:
            return self.logger.info('create_table_warn() - success')
        
        
    def or_in_a_row_warn(self, raw: str):
        '''
        Функция проверяет скрипт на наличие нескольких подряд идущих условий OR.
        '''
        
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        # отбираются все строки, где используется OR
        or_statements = [(ind, val) for ind, val in enumerate(raw.split('\n')) if 'OR ' in val]
        # только те выражения с OR, где есть знак "=" (т.к. в IN не попадут выражения со знаками <>)
        eq_statements = [i for i in or_statements if '=' in i[1]]

        # оставим само выражение, без оператора
        split_or = [(ind, val.split('OR ')[1]) for ind, val in eq_statements]

        # с обеих сторон от "=" могут стоять одинаковые значения (поля), поэтому разобьем выражение на две части и соединим в один столбец
        split_eq1 = [(ind, val.split('=')[0]) for ind, val in split_or]
        split_eq2 = [(ind, val.split('=')[1]) for ind, val in split_or]

        self.concat_lists = pd.concat(
            [pd.DataFrame(split_eq1, columns=['row_number', 'or_parts']), 
            pd.DataFrame(split_eq2, columns=['row_number', 'or_parts'])])

        # считаем кол-во одинаковых частей по обе стороны от "=" и оставляем только те строки, где строк больше 1
        self.group = self.concat_lists.groupby('or_parts').count()
        self.more_1 = self.group[self.group['row_number'] > 1].reset_index()
        self.or_in_a_row_logs = \
        f'''
        CodeStyle Warning in or_in_a_row_warn(): Найдено {self.more_1.shape[0]} подряд идущих условий OR. Рекомендуется замена условий на выражение IN (x, y, z). 
        (SQL Style Guide. WHERE/ON. Ссылка: {self.link_sql_code_style_guide})
        '''
        
        self.count_functions += 1
        
        if len(self.more_1) > 0:
            self.or_in_a_row_warn_df = self.concat_lists[self.concat_lists['or_parts'].isin(self.more_1['or_parts'].tolist())]
            return self.logger.warning(self.or_in_a_row_logs)
        else:
            return self.logger.info('or_in_a_row_warn() - success')
        
    
    def join_rule_sub(self, raw: str) -> str:
        '''
        Функция проверяет корректность написания "JOIN ... ON" выражения.
        Если условие только одно, то допускается написание наравне с оператором JOIN, если условий более одного, 
        то их необходимо перенести на новую строку (совместно с ON) и использовать скобки.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        script_by_rows = [tup for tup in raw.split('\n') if len(tup) > 0]
        
        join_keywords = ['JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS']
        join_end = ['WHERE', 'ORDER BY', 'GROUP BY'] 
        
        self.join_groups = []
        counter = 0
        for ind in range(0, len(script_by_rows)-1):
            
            # если строка начинается с join, то запоминаем ее индекс
            if script_by_rows[ind].startswith(tuple(join_keywords)) and counter == 0:
                counter = ind
                # если следующая строка так же join, то записываем текущую строку и обнуляем счетчик
                if script_by_rows[ind+1].startswith(tuple(join_keywords)):
                    self.join_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
                    continue
            
            # если следующая строка join и счетчик не 0, то записываем диапазон строк, счетчик = 0
            if script_by_rows[ind+1].startswith(tuple(join_keywords)) and counter != 0:
                self.join_groups.append(script_by_rows[counter:ind+1])
                counter = 0
                
            # если впереди другие операторы (не join) и счетчик не равен 0 (значит join заканчивается), то записываем диапазон
            if script_by_rows[ind+1].startswith(tuple(join_end)) and counter != 0:
                self.join_groups.append(script_by_rows[counter:ind+1])
                counter = 0
                
            # если счетчик не равен 0, в запросе несколько условий и запрос заканчивается
            if len(script_by_rows) == ind + 2 and counter != 0:
                self.join_groups.append(script_by_rows[counter:ind+2])
     
        for group in self.join_groups:
            if len(group) > 1:
                for ind in range(0, len(group)):
                    try:
                        # если пользователь проставил скобки
                        if 'ON (' in group[ind]:
                            count_spaces = 4 * ' '
                            re_val = group[ind].replace('(', '\(')
                            pattern = re.compile(f'{re_val}')
                            right_val = group[ind].replace('ON (', '\n' + count_spaces + 'ON (\n' + count_spaces * 2)
                            raw = pattern.sub(right_val, raw, 1)
                            
                            # поиск закрывающей скобки
                            for i in range(1, len(group)):
                                count_spaces = 4 * ' '
                                if ')' in group[i]:
                                    re_val = group[i].replace(')', '\)')
                                    pattern = re.compile(f'{re_val}')
                                    right_val = count_spaces * 2 + group[i].strip().replace(')', '\n' + count_spaces + ')')
                                else:
                                    re_val = group[i].replace(')', '\)').replace('(', '\(')
                                    pattern = re.compile(f'{re_val}')
                                    right_val = count_spaces * 2 + group[i].strip()
                                raw = pattern.sub(right_val, raw, 1)
                        
                        
                        if '(' in group[ind]:
                            re_val = group[ind].replace('(', '\(')
                            pattern = re.compile(f'{re_val}')
                        elif ')' in group[ind]:
                            re_val = group[ind].replace(')', '\)')
                            pattern = re.compile(f'{re_val}')
                        else:
                            pattern = re.compile(f'{group[ind]}')
                        
                        # если скобки не проставлены
                        if group[ind].startswith(tuple(join_keywords)):
                            count_spaces = 4 * ' '
                            right_values1 = group[ind].replace('ON ', '\n' + count_spaces + 'ON (\n' + count_spaces)
                            raw = pattern.sub(right_values1, raw, 1)
                        
                        if 'AND ' in group[ind] or 'OR ' in group[ind]:
                            if ind == len(group) - 1:
                                if 'AND ' in group[ind]:
                                    count_space = 4 * ' '
                                    right_val = group[ind].replace('AND', count_space + 'AND')
                                    right_val = right_val + '\n' + count_space + ')'
                                    raw = pattern.sub(right_val, raw, 1)
                                if 'OR ' in group[ind]:
                                    count_space = 4 * ' '
                                    right_val = group[ind].replace('OR', count_space + 'OR')
                                    right_val = right_val + '\n' + count_space + ')'
                                    raw = pattern.sub(right_val, raw, 1)
                            else:
                                if 'AND' in group[ind]:
                                    count_space = 4 * ' '
                                    right_val = group[ind].replace('AND', count_space + 'AND')
                                    raw = pattern.sub(right_val, raw, 1)
                                if 'OR' in group[ind]:
                                    count_space = 4 * ' '
                                    right_val = group[ind].replace('OR', count_space + 'OR')
                                    raw = pattern.sub(right_val, raw, 1)
                    except:
                        raise SQLParserError("Something went wrong in " '{!r}'.format(group[ind]))
        
        self.count_functions += 1
        
        return raw
    
    
    def where_and_or_rule_sub(self, raw: str, debug_flag = True) -> str:
        '''
        Если внутри условия AND используется условие OR, то следует писать каждое с новой строки с дополнительным отступом, 
        замыкающую скобку писать наравне с открывающим оператором AND.
        '''
        if debug_flag == False:
            assert isinstance(raw, str), 'Аргумент raw должен быть типа string'

            script_by_rows = [tup for tup in raw.split('\n') if len(tup) > 0]
            where_keywords = ['WHERE ', 'WHERE '] # в tuple нужно передать больше одного значения
            where_end_group_by = ['GROUP BY ', 'ORDER BY ']    

            where_groups = []
            counter = 0
            for ind in range(0, len(script_by_rows)-1):

                # если строка начинается с where, то запоминаем ее индекс
                if script_by_rows[ind].startswith(tuple(where_keywords)):
                    counter = ind
                    continue

                # если впереди group by, то записываем группу и обнуляем счетчик
                if script_by_rows[ind+1].startswith(tuple(where_end_group_by)) :
                    where_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
                    continue

                # если кол-во строк равно индексу (с поправкой) и счетчик не равен 0 (значит запрос заканчивается на выражении where), то записываем группу
                if len(script_by_rows)-2 == ind and counter != 0:
                    where_groups.append(script_by_rows[counter:])
                    counter = 0

            counter = 0
            and_groups = []
            for group in where_groups:
                for ind in range(0, len(group)-1):
                    if 'AND (' in group[ind] and counter == 0:
                        counter = ind
                    if 'AND (' in group[ind+1] and counter != 0:
                        and_groups.append(group[counter:ind+1])
                        counter = 0
                    if len(group)-2 == ind and counter != 0:
                        and_groups.append(group[counter:])

            for group in and_groups:
                for ind in range(0, len(group)):
                    if 'AND (' in group[ind]:
                        re_val = group[ind].replace('(', '\(').replace(')', '\)')
                        pattern = re.compile(f'{re_val}')
                        spaces = ' ' * 8
                        right_val = group[ind].replace('AND (', 'AND (\n' + spaces)#.strip()
                        raw = pattern.sub(right_val, raw, 1)
                    #if len(group) - 1 > ind > 0:
                    #    re_val = group[ind].replace('(', '\(').replace(')', '\)')
                    #    pattern = re.compile(f'{re_val}')
                    #    spaces = '' * 8
                    #    right_val = spaces + group[ind]#.strip()
                    #    raw = pattern.sub(right_val, raw, 1)
                    if len(group) - 1 == ind:
                        #print(len(group) - 1, ind)
                        re_val = group[ind].replace('(', '\(').replace(')', '\)')
                        pattern = re.compile(f'{re_val}')
                        spaces = ' ' * 4
                        start_spaces = '' * 8
                        right_val = group[ind][:-1] + '\n' + spaces + ')'#.strip()
                        raw = pattern.sub(right_val, raw)
        else:
            pass
        
        self.count_functions += 1
        
        return raw
    
    
    def join_and_or_rule_sub(self, raw: str) -> str:
        '''
        Для вложенных условий в JOIN функция корректно расставляет скобки и отступы.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        script_by_rows = [tup for tup in raw.split('\n')]
        
        join_keywords = ['JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS']
        join_end = ['WHERE', 'ORDER BY', 'GROUP BY']    
        
        self.join_groups_or = []
        counter = 0
        for ind in range(0, len(script_by_rows)-1):
            
            # если строка начинается с join, то запоминаем ее индекс
            if script_by_rows[ind].startswith(tuple(join_keywords)) and counter == 0:
                counter = ind
                # если следующая строка так же join, то записываем текущую строку и обнуляем счетчик
                if script_by_rows[ind+1].startswith(tuple(join_keywords)):
                    self.join_groups_or.append(script_by_rows[counter:ind+1])
                    counter = 0
                    continue
            
            # если следующая строка join и счетчик не 0, то записываем диапазон строк, счетчик = 0
            if script_by_rows[ind+1].startswith(tuple(join_keywords)) and counter != 0:
                self.join_groups_or.append(script_by_rows[counter:ind+1])
                counter = 0
                
            # если впереди другие операторы (не join) и счетчик не равен 0 (значит join заканчивается), то записываем диапазон
            if script_by_rows[ind+1].startswith(tuple(join_end)) and counter != 0:
                self.join_groups_or.append(script_by_rows[counter:ind+1])
                counter = 0
        
        for group in self.join_groups_or:
            for ind in range(0, len(group)):
                if "(" in group[ind] and ('AND' in group[ind] or 'OR' in group[ind]):
                    re_val = group[ind].replace("(", "\(")
                    pattern = re.compile(f"{re_val}")
                    #count_spaces = len(group[ind+1]) - len(group[ind+1].lstrip())
                    count_spaces = 8 * ' '
                    right_val = group[ind].replace('(', '(\n' + count_spaces)
                    raw = pattern.sub(right_val, raw)
                if ')' in group[ind] and ('AND' in group[ind] or 'OR' in group[ind]):
                    re_val = group[ind].replace(')', '\)')
                    pattern = re.compile(f'{re_val}')
                    #count_spaces = len(group[ind-1]) - len(group[ind-1].lstrip())
                    count_spaces = 8 * ' '
                    right_val = group[ind].replace(')', '\n' + count_spaces + ')')
                    raw = pattern.sub(right_val, raw)
        
        self.count_functions += 1
        
        return raw
    
    
    def as_rule_warn(self, raw: str):
        '''
        Функция проверяет корректное использование ключевого слова AS в JOIN и функциях.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        self.count_functions += 1
        
        script_by_rows = [tup for tup in raw.split('\n')]
        
        as_keywords = ['SELECT', 'FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS']
        as_end = ['WHERE', 'ORDER BY', 'GROUP BY', 'HAVING']
        as_positions = ['FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS']
        
        self.as_groups = []
        counter = 0
    
        # разметка скрипта, выбор только тех частей, где может быть выражение AS
        for ind in range(0, len(script_by_rows)-1):
            
            # если строка начинается с as_keywords, то запоминаем ее индекс
            if script_by_rows[ind].startswith(tuple(as_keywords)) and counter == 0:
                counter = ind
                # если следующая строка так же as_keywords - записываем текущую строку и обнуляем счетчик
                if script_by_rows[ind+1].startswith(tuple(as_keywords)):
                    self.as_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
                    continue
            
            # если следующая начинается с as_keywords, и счетчик не 0, то записываем диапазон строк, счетчик = 0
            if script_by_rows[ind+1].startswith(tuple(as_keywords)) and counter != 0:
                self.as_groups.append(script_by_rows[counter:ind+1])
                counter = 0
                
            # если впереди другие операторы (не as_keywords) и счетчик не равен 0, то записываем диапазон
            if script_by_rows[ind+1].startswith(tuple(as_end)) and counter != 0:
                self.as_groups.append(script_by_rows[counter:ind+1])
                counter = 0
    
        # выделение строк запросов для определения кол-ва join-ов в запросе и их принадлежности к конкретному запросу
        # опираемся на ключевое слова SELECT, как разграничитель запросов
        cnt = 0
        select_group = []
        for group_index in range(1, len(self.as_groups)):
            for m in self.as_groups[group_index-1]:
                if 'SELECT' in m and cnt == 0:
                    cnt = group_index            
            for n in self.as_groups[group_index]:
                if 'SELECT' in n and cnt >= 1:
                    select_group.append(self.as_groups[cnt-1:group_index])
                    cnt = 0
            if group_index == len(self.as_groups)-1 and cnt != 0:
                select_group.append(self.as_groups[cnt-1:group_index+1])
        
        self.as_rule_1_logs = \
        f'''
        CodeStyle Warning in as_rule_warn(): AS необходимо обязательно применять при использовании функций. (SQL Style Guide. Общие конструкции. 
        Ссылка: {self.link_sql_code_style_guide})
        '''
        
        self.as_rule_2_logs = \
        f'''
        CodeStyle Warning in as_rule_warn(): Для указания нового наименования таблиц/столбцов необходимо явно прописывать ключевое слово AS. 
        Наименование должны быть осмыслены и сформированы в рамках единого стиля скрипта/проекта. (SQL Style Guide. Общие конструкции. 
        Ссылка: {self.link_sql_code_style_guide})
        '''
        
        # реализация правил
        functions = [i.upper() for i in funcs.all_funcs]
        for query in select_group:
            #print(query)
            for row in range(0, len(query)):
                #print(query[row])
                for keyword in query[row]:
                    string = keyword.strip()
                    #print(string)
                    if string.startswith(tuple(functions)) and 'AS' not in string:
                        self.logger.warning(self.as_rule_1_logs)
                    # тест elif - if
                    elif string.startswith(tuple(as_positions)) and len(query) > 2 and 'AS' not in string:
                        self.logger.warning(self.as_rule_2_logs)
                    else:
                        return self.logger.info('as_rule_warn() - success')


    def column_name_rule_warn(self, raw: str):
        '''
        Функция проверяет наличие alias'ов при использовании нескольких таблиц в запросе.
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        script_by_rows = [tup for tup in raw.split('\n')]
        
        point_keywords = ['SELECT', 'FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS', 'WHERE', 'ORDER BY', 'GROUP BY', 'HAVING']
        point_end = ['WHERE', 'ORDER BY', 'GROUP BY', 'HAVING']
        exclude_point_positions = ['FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'CROSS']
        
        self.point_groups = []
        counter = 0
        
        # разметка скрипта, выбор только тех частей, где может быть выражение AS
        for ind in range(0, len(script_by_rows)-1):
            
            # если строка начинается с point_keywords, то запоминаем ее индекс
            if script_by_rows[ind].startswith(tuple(point_keywords)) and counter == 0:
                counter = ind
                # если следующая строка так же point_keywords - записываем текущую строку и обнуляем счетчик
                if script_by_rows[ind+1].startswith(tuple(point_keywords)):
                    self.point_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
            
            # если следующая начинается с point_keywords, и счетчик не 0, то записываем диапазон строк, счетчик = 0
            if script_by_rows[ind+1].startswith(tuple(point_keywords)) and counter != 0:
                self.point_groups.append(script_by_rows[counter:ind+1])
                counter = 0
    
        # выделение строк запросов для определения кол-ва join-ов в запросе и их принадлежности к конкретному запросу
        # опираемся на ключевое слова SELECT, как разграничитель запросов
        cnt = 0
        select_group = []
        for group_index in range(1, len(self.point_groups)):
            for m in self.point_groups[group_index-1]:
                if 'SELECT' in m and cnt == 0:
                    cnt = group_index            
            for n in self.point_groups[group_index]:
                if 'SELECT' in n and cnt >= 1:
                    select_group.append(self.point_groups[cnt-1:group_index])
                    cnt = 0
            if group_index == len(self.point_groups)-1 and cnt != 0:
                select_group.append(self.point_groups[cnt-1:group_index+1])
        
        # выделяем выражения IN, чтобы исключить их из проверки на наличие точки
        in_stmnt = []
        in_cnt = 0
        for query in select_group:
            if len(query) > 2:
                for row in range(0, len(query)):
                    for keyword_ind in range(0, len(query[row])):
                        if ' IN (' in query[row][keyword_ind].strip():
                            in_cnt = keyword_ind
                        if ')' in query[row][keyword_ind].strip() and in_cnt != 0:
                            in_stmnt.append(query[row][in_cnt:keyword_ind+1])
                            in_cnt = 0
    
        #print(select_group)
        exceptions = ['ON (', ')', 'WHERE', 'AND (', 'OR (', 'WHERE 1 = 1']
        to_warnings = []
        for query in select_group:
            #print(query)
            if len(query) > 2:
                for row in range(0, len(query)):
                    #print(query[row])
                    for keyword in query[row]:
                        no_spaces_string = keyword.strip()
                        if no_spaces_string not in exceptions \
                            and not no_spaces_string.startswith(tuple(exclude_point_positions)) \
                            and ' *' not in no_spaces_string:
                            if '.' not in no_spaces_string and keyword not in tuple(sum(in_stmnt, [])) and len(no_spaces_string) > 0:
                                to_warnings.append(no_spaces_string)
        
        self.column_name_rule_logs = \
        f'''
        CodeStyle Warning in column_name_rule_warn(): Если в запросе участвуют несколько таблиц, то при указании наименования столбца необходимо явно указывать 
        какой таблице принадлежит столбец. Проверьте строки {to_warnings}. (SQL Style Guide. Подзапросы. Ссылка: {self.link_sql_code_style_guide})
        '''
        
        self.count_functions += 1
        
        if to_warnings:
            return self.logger.warning(self.column_name_rule_logs)
        else:
            return self.logger.info('column_name_rule_warn() - success')
    
    
    def window_functions_sub(self, raw: str) -> str:
        '''
        Функция исправляет отступы в оконных функциях. Количество отступов можно регулировать. 
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        self.count_functions += 1
        
        if ') OVER (' in raw:
            script_by_rows = [tup for tup in raw.split('\n')]
            
            wind_keywords = ['SELECT', 'SELECT']
            wind_end = ['FROM']
            
            wind_groups = []
            counter = 0
            
            # разметка скрипта, выбор только тех частей, где могут быть оконные функции
            for ind in range(0, len(script_by_rows)-1):
                
                # если строка начинается с as_keywords, то запоминаем ее индекс
                if script_by_rows[ind].startswith(tuple(wind_keywords)) and counter == 0:
                    counter = ind
                    
                # если впереди другие операторы (не as_keywords) и счетчик не равен 0, то записываем диапазон
                if script_by_rows[ind+1].startswith(tuple(wind_end)) and counter != 0:
                    wind_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
            
            # в select ищем оконную функцию по ключевой комбинации ") OVER ("
            for group in wind_groups:
                if len(group) > 1:
                    #print(group)
                    counter = 0
                    for ind in range(0, len(group)):
                        if ') OVER (' in group[ind]:
                            counter = ind
                            find_window = group[counter:]
                            #print(d)
                            break
            
            # Разбиваем оконные функции на группы - каждая группа это оконная функция и столбцы (если есть), 
            # выбранные пользователем, идущие после нее (включая функции)
            only_window = []
            counter = 0
            for j in range(0, len(find_window)-1):
                #print(d[j])
                if ') OVER (' in find_window[j+1]:
                    only_window.append(find_window[counter:j+1])
                    counter = j+1
                    #print(group[ind])
                if j == len(find_window) - 2:
                    #print(d[j+1])
                    only_window.append(find_window[counter:j+2])
            
            # отступы
            func_indent = ' ' * 7
            part_indent = ' ' * 52
            main_sort_indent = ' ' * 15
            post_sort_indent = ' ' * 20
            one_level_sort = ' ' * 26
            other_fields_indent = ' ' * 7
            
            
            # функция под замену скобок в паттернах
            for wind in only_window:
                #print(wind)
                start_flag = 0
                order_flag = 0
                end_of_wind_flag = 0
                for ind in range(0, len(wind)):
                    #print(f'wind: {ind} :', wind[ind])
                    if ')' in wind[ind] or '(' in wind[ind]:
                        re_val = wind[ind].replace(')', '\)').replace('(', '\(')
                        pattern = re.compile(f'{re_val}')
                        right_val = wind[ind].strip()
                    else:
                        re_val = wind[ind]
                        pattern = re.compile(f'{re_val}')
                        right_val = wind[ind].strip()
        
                    # каждая группа начинается с оконной функции, в одной группе - одна оконка, зажигаем start_flag
                    if ') OVER (' in wind[ind]:
                        right_val = func_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
                        start_flag = 1
                    
                    # условие для нескольких сортировок
                    if start_flag == 1 and end_of_wind_flag == 0 and order_flag == 1 and ') OVER (' not in wind[ind]:
                        right_val = post_sort_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
                    
                    # если сортировка по одному полю, то пишем все в одну строчку по умолчанию (можно поставить one_level_sort)
                    if start_flag == 1 and 'ORDER BY' in wind[ind] and ') AS ' in wind[ind]:
                        right_val = main_sort_indent + wind[ind].strip() #one_level_sort
                        raw = pattern.sub(right_val, raw, 1)
                        order_flag = 1
                    
                    # если встретилась сортировка по нескольким полям, то зажигаем order_flag
                    if start_flag == 1 and 'ORDER BY' in wind[ind] and ') AS ' not in wind[ind]:
                        right_val = main_sort_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
                        order_flag = 1
                    
                    # условие для последней строчки оконной функции с несколькими сортировками
                    if start_flag == 1 and order_flag == 1 and ') AS ' in wind[ind] and '(' not in wind[ind] and 'ORDER BY' not in wind[ind]:
                        right_val = post_sort_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
                        end_of_wind_flag = 1
                        continue
                    
                    # отступ для полей, которые идут после окончания оконной функции
                    if start_flag == 1 and 'ORDER BY' not in wind[ind] and end_of_wind_flag == 1:
                        re_val = wind[ind].replace(')', '\)').replace('(', '\(')
                        pattern = re.compile(f'{re_val}')
                        right_val = other_fields_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
                    
                    # условие для нескольких партиций
                    if start_flag == 1 and end_of_wind_flag == 0 and order_flag == 0 and ') OVER (' not in wind[ind]:
                        right_val = post_sort_indent + wind[ind].strip()
                        raw = pattern.sub(right_val, raw, 1)
        else:
            self.window_funcs_not_found = 1
        return raw
        
    def create_table_sub(self, raw: str) -> str:
        '''
        Функция исправляет отступы при создании таблиц в терадате. 
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        
        self.count_functions += 1
        
        if 'CREATE' in raw:
            
            script_by_rows = [tup for tup in raw.split('\n')]
            
            create_keywords = ['CREATE multiset TABLE', 'CREATE multiset VOLATILE TABLE', 'CREATE MULTISET VOLATILE TABLE']
            create_end = ['INSERT', 'CREATE', ]
            
            create_groups = []
            counter = 0
        
            for ind in range(0, len(script_by_rows)):
            
                # если строка начинается с create_keywords, то запоминаем ее индекс
                if script_by_rows[ind].startswith(tuple(create_keywords)) and counter == 0:
                    counter = ind
                
                # если впереди другие операторы (не create_keywords) и счетчик не равен 0, то записываем диапазон
                if ';' in script_by_rows[ind] and counter != 0:
                    create_groups.append(script_by_rows[counter:ind+1])
                    counter = 0
            #print(create_groups)
            # Для корректной работы регулярных выражений необходимо, что не было повторяющихся строчек кода (точь в точь)
            # в противном случае отступы будут проставлены некорректно
            spaces = 4 * ' '
            for group in create_groups:
                for ind in range(0, len(group)):
                    if len(group[ind]) > 0:
                        re_val = group[ind].replace(')', '\)').replace('(', '\(')
                        pattern = re.compile(f'{re_val}')
                        if '(' in group[ind] and group[ind].startswith(tuple(create_keywords)):
                            right_val = group[ind].strip().replace('(', '(\n' + spaces)
                            raw = pattern.sub(right_val, raw, 1)
                        elif '))' in group[ind] and (';' in group[ind] or ' INDEX ' in group[ind]):
                            right_val = spaces + group[ind].strip().replace(')) ', ')\n' + spaces + ')\n')
                            raw = pattern.sub(right_val, raw, 1)
                        elif 'COMMIT PRESERVE ROWS;' in group[ind]:
                            right_val = group[ind].strip()
                            raw = pattern.sub(right_val, raw, 1)
                        elif 'SET unicode' in group[ind] and 'CHARACTER' in group[ind-1]:
                            right_val = spaces + group[ind-1].strip() + ' ' + group[ind].strip()
                            raw = pattern.sub(right_val, raw, 1)
                            # удаляем предыдущую строчку
                            re_val = spaces + group[ind-1].strip().replace(')', '\)').replace('(', '\(')
                            pattern = re.compile(f'{re_val}' + '\n')
                            right_val = ''
                            raw = pattern.sub(right_val, raw, 1)
                        else:
                            re_val = group[ind].replace(')', '\)').replace('(', '\(')
                            pattern = re.compile(f'{re_val}')
                            right_val = spaces + group[ind].strip()
                            raw = pattern.sub(right_val, raw, 1)
        else:
            self.create_table_not_found = 1
        return raw

    
    def indent_for_with_stmnt_sub(self, raw: str) -> str:
        '''
        Функция для добавления отступов в каждую строчку тела запроса, написанного в выражении WITH:
        WITH table_name as (
            < тело
            случайного
            запроса >
        ) - добавляется 4 пробела 
        '''
        assert isinstance(raw, str), 'Аргумент raw должен быть типа string'
        script_by_rows = [tup for tup in raw.split('\n') if len(tup) > 0]
        spaces = 4 * ' '
        for row in script_by_rows:
            re_val = row.replace(')', '\)').replace('(', '\(').replace('*', '\*')
            pattern = re.compile(f'{re_val}')
            right_val = spaces + row
            raw = pattern.sub(right_val, raw)
        return raw

    
    def run_code_style(self, raw: str, return_pretty_view=True, print_logs=False, with_statement=False):
        '''
        Функция запускает остальные функции в правильном порядке. Для выражения WITH запускается альтренативная ветка проверки. 
        '''
        '''
        Substitutions
        '''
        temp = self.default_string(raw)
        if print_logs==True:
            print('Start run_code_style() function')
        
        self.upper_case = self.functions_upper_case_sub(temp)
        if print_logs==True and self.upper_case:
            print(f'{self.count_functions} - functions_upper_case_sub() - done')
        
        
        self.where_clause = self.where_clause_sub(self.upper_case)
        
        self.where_and_or_rule = self.where_and_or_rule_sub(self.where_clause)
        if print_logs==True and self.where_and_or_rule:
            print(f'{self.count_functions} - where_and_or_rule_sub - done')
        
        self.window_functions = self.window_functions_sub(self.where_and_or_rule)
        if print_logs==True:
            if self.window_funcs_not_found == 0:
                print(f'{self.count_functions} - window_functions_sub() - done')
            else:
                print(f'{self.count_functions} - Window functions were not found. Return raw.')
        
        self.create_table = self.create_table_sub(self.window_functions)
        if print_logs==True:
            if self.create_table_not_found == 0:
                print(f'{self.count_functions} - create_table_sub() - done')
            else:
                print(f'{self.count_functions} - Create functions were not found. Return raw.')
        
        self.join_rule = self.join_rule_sub(self.create_table)
        if print_logs==True and self.join_rule:
            print(f'{self.count_functions} - join_rule_sub() - done')
        
        self.join_and_or_rule = self.join_and_or_rule_sub(self.join_rule)
        if print_logs==True and self.join_and_or_rule:
            print(f'{self.count_functions} - join_and_or_rule_sub() - done')
        
        if with_statement == True and self.join_and_or_rule:
            self.indent_for_with_stmnt = self.indent_for_with_stmnt_sub(self.join_and_or_rule)
            
        if print_logs==True:
            print('All substitutions are well done')
        
        # счетчики для логов
        self.count_functions = 0
        self.window_funcs_not_found = 0
        self.create_table_not_found = 0
        
        '''
        Warnings
        '''
        self.as_rule_warn(self.join_and_or_rule)
        self.create_table_warn(self.join_and_or_rule)
        self.column_name_rule_warn(self.join_and_or_rule)
        self.group_rules_warn(self.join_and_or_rule)
        self.or_in_a_row_warn(self.join_and_or_rule)
        self.row_max_symbols_warn(self.join_and_or_rule)
        self.warning_wrong_words_warn(self.join_and_or_rule)
        if print_logs==True:
            print('All warnings are well done')
        
        # returns
        if return_pretty_view == True and with_statement == False:
            return print(self.join_and_or_rule)
        elif return_pretty_view == False and with_statement == False:
            return self.join_and_or_rule
        elif return_pretty_view == False and with_statement == True:
            return self.indent_for_with_stmnt
        
        
class WithStatementStyle(SQLParser):
    '''
    Класс для обработки выражения WITH. Код разделяет строки с названиями cte и тело (код в скобках). Тело редактируется классом SQLParser,
    а названия cte - регулярными выражениями. На выходе последовательно объединяем все части выражения.
    '''
    def __init__(self):
        super().__init__()
    
    
    def split_with_statement(self, raw: str) -> tuple:
        '''
        Функция для разбиения кода на названия - объект name_of_with_block, и тела - объект inside_with. 
        Последний select в выражении WITH обрабатывается отдельно и объединяется с inside_with в объект full_code. 
        Функция возвращает tuple из этих объектов (name_of_with_block и full_code).
        '''
        self.with_split_blocks = [i for i in raw.split('),')]
        self.inside_with = []
        self.name_of_with_block = []
        for block in self.with_split_blocks:
            block_by_row = [i for i in block.split('\n') if len(i) > 0]
            self.name_of_with_block.append(block_by_row[0])

            select_block = []
            for sel in block_by_row[1:]:            
                if '(SELECT' in sel:
                    replace_sel = sel.replace('(', '')
                    select_block.append(replace_sel)
                else:
                    select_block.append(sel)
            self.inside_with.append(select_block)

        self.correct_last_block = []
        last_block = self.inside_with[-1]
        counter = 0
        for ind in range(0, len(last_block)-1):
            if 'SELECT' in last_block[ind] and counter == 0:
                counter = ind
                flg = 1

            if 'SELECT' in last_block[ind+1] and flg == 1:
                self.correct_last_block.append(last_block[counter:ind+1])
                counter = 0
                flg = 0

            if ind == len(last_block)-2:
                self.correct_last_block.append(last_block[counter:ind+2])

        self.correct_last_block[0][-1] = self.correct_last_block[0][-1].replace(')', '')
        self.full_code = ['\n'.join(i) for i in self.inside_with[:-1] + self.correct_last_block]
        return self.name_of_with_block, self.full_code
    
    
    def run_code_style_for_with(self, raw: str, return_pretty_view=True, ) -> str:
        
        temp = self.default_string(raw)
        self.head = self.split_with_statement(temp)[0]
        self.raw_bodies = self.split_with_statement(temp)[1]
        
        self.body = []
        for i in self.raw_bodies:
            self.body.append(self.run_code_style(i, return_pretty_view=False, with_statement=True))
        with_block = ''.join(
            [
                a.strip() + ' (' + b + '\n),' + 2*'\n'
                for a, b in zip(self.head, self.body[:-1])
            ]
        )[0:-3] + 2*'\n'
        last_select = self.run_code_style(self.body[-1], return_pretty_view=False)
        self.full = with_block + last_select
        
        if return_pretty_view==True:
            return print(self.full)
        else:
            return self.full
    
    
class HandlerSQL(WithStatementStyle):
    '''
    Основной класс, цель которого определить алгоритм, по которому будет просиходить обработка кода.
    '''
    def __init__(self):
        super().__init__()
        
    def _private_method(self, raw: str):
        
        self.using_algorithm = None
        self.keyword = 'WITH '
        if self.keyword in raw:
            self.using_algorithm = WithStatementStyle()
        else:
            self.using_algorithm = SQLParser()
        return self.using_algorithm
            
    def run(self, raw: str):
        self.res = self._private_method(raw)
        if self.keyword in raw:
            self.res.run_code_style_for_with(raw)
        else:
            self.res.run_code_style(raw)
        return print(self.res.default_string(raw))