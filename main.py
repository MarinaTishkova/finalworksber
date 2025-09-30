import jaydebeapi
from unicodedata import decimal
import pandas as pd
import time
import os


conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                            'jdbc:oracle:thin:de3tn/farmermaggot@de-oracle.chronosavant.ru:1521/deoracle',
                            ['de3tn','farmermaggot'],
                            'ojdbc8.jar')

curs = conn.cursor()

curs.execute('''ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS' ''')




# инициализация и чтение файлов для passport_blacklist (LETO_STG_PASS_BL)
def init_read_passports_stg(filePath):
    try:
        curs.execute(''' 
        create table LETO_STG_PASS_BL (
            "date" date,
            passport varchar(100)
        )
        ''')
        print('LETO_STG_PASS_BL создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_PASS_BL не создана')

    df = pd.read_excel(filePath)
    df["date"] = df["date"].astype(str)

    try:
        curs.executemany('''
        insert into LETO_STG_PASS_BL (
            "date",
            passport
        ) values (
            to_date(?, 'YYYY-MM-DD HH24:MI:SS'), ?
        )
        ''', df.values.tolist())
        print('LETO_STG_PASS_BL ЗАПОЛНЕНА')
    except Exception as e:
        print(str(e))

# создание и инициализация фактической таблицы для passport_blacklist (LETO_DWH_FACT_PASS_BL)
def init_passports():
    try:
        curs.execute('''
        create table LETO_DWH_FACT_PASS_BL (
            passport_num varchar(128),
            entry_dt date)
            ''')
        print('LETO_DWH_FACT_PASS_BL создана')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_FACT_PASS_BL не создана')

    try:
        curs.execute('''
            insert into LETO_DWH_FACT_PASS_BL (
                passport_num,
                entry_dt
            ) select
                passport,
                "date"
            from LETO_STG_PASS_BL
            ''')
        print('LETO_STG_PASS_BL ЗАПОЛНЕНА')
    except Exception as e:
        print(str(e))


# инициализация и чтение файлов для terminals (LETO_STG_TERMINALS)
def init_read_terminals_stg(filePath):
    try:
        curs.execute(''' 
        create table LETO_STG_TERMINALS (
            terminal_id varchar(100),
            terminal_type varchar(100),
            terminal_city varchar(100),
            terminal_address varchar(100)
        )
        ''')
        print('LETO_STG_TERMINALS создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_TERMINALS не создана')

    df = pd.read_excel(filePath)

    try:
        curs.executemany('''
        insert into LETO_STG_TERMINALS (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        ) values (
            ?,?,?,?
        )
        ''', df.values.tolist())
        print('LETO_STG_TERMINALS ЗАПОЛНЕНА')
    except Exception as e:
        print(str(e))

# создание исторической таблицы LETO_DWH_DIM_TERMINALS_HIST
def init():
    try:
        curs.execute('''
            create table LETO_DWH_DIM_TERMINALS_HIST(
                terminal_id varchar(128),
                terminal_type varchar(128),
                terminal_city varchar(128),
                terminal_address varchar(128),
                effective_from date default sysdate,
                effective_to date default to_date('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
                deleted_flg integer default 0 check(deleted_flg in (1,0))
            )
        ''')

        print('Исторническая таблица LETO_DWH_DIM_TERMINALS_HIST только что создана')
    except Exception as e:
        print('Исторническая таблица LETO_DWH_DIM_TERMINALS_HIST только что не создана')
        print(str(e))

    try:
        curs.execute('''
            create view LETO_STG_TERMINALS_VIEW as
                select
                    terminal_id, terminal_type, terminal_city, terminal_address
                from LETO_DWH_DIM_TERMINALS_HIST
                where sysdate between effective_from and effective_to
        ''')
    except Exception as e:
        print(str(e))

# создание таблицы с новыми записями LETO_STG_TERMINALS_NEW
def createTableNewRows():
    try:
        curs.execute('''
            create table LETO_STG_TERMINALS_NEW as
            select
                t1.*
            from LETO_STG_TERMINALS t1
            left join LETO_STG_TERMINALS_VIEW t2
            on t1.terminal_id = t2.terminal_id
            where t2.terminal_id is null
        ''')
        print('Новые записи в таблице LETO_STG_TERMINALS_NEW')
    except Exception as e:
        print(str(e))
        print('Новые записи не в таблице LETO_STG_TERMINALS_NEW')

# создание таблицы с удаленными записями LETO_STG_TERMINALS_DELETE
def createTableDeleteRows():
    try:
        curs.execute('''
            create table LETO_STG_TERMINALS_DELETE as
                select
                    t1.*
                from
                LETO_STG_TERMINALS_VIEW t1
                left join LETO_STG_TERMINALS t2
                on t1.terminal_id = t2.terminal_id
                where t2.terminal_id is null
        ''')
    except Exception as e:
        print(str(e))

# создание таблицы с измененными записями LETO_STG_TERMINALS_CHANGED
def createTableChangedRows():
    try:
        curs.execute('''
            create table LETO_STG_TERMINALS_CHANGED as
                select
                    t1.*
                from LETO_STG_TERMINALS t1
                inner join LETO_STG_TERMINALS_VIEW t2
                on t1.terminal_id = t2.terminal_id
                and
                    (
                        t1.terminal_address <> t2.terminal_address or
                        t1.terminal_type <> t2.terminal_type or
                        t1.terminal_city <> t2.terminal_city
                    )
        ''')
    except Exception as e:
        print(str(e))

# обновление исторической таблицы
def updateTerminalsHist():
    # deleted records
    curs.execute('''
        update LETO_DWH_DIM_TERMINALS_HIST
            set effective_to = sysdate - (1/(24*60*60)),
            deleted_flg = 1
            where terminal_id in (select terminal_id from LETO_STG_TERMINALS_DELETE)
            and effective_to = '2999-12-31 23:59:59'
    ''')


    # modified records
    curs.execute('''
        update LETO_DWH_DIM_TERMINALS_HIST
            set effective_to = sysdate - 1/(24*60*60)
            where terminal_id in (select terminal_id from LETO_STG_TERMINALS_CHANGED)
            and effective_to = '2999-12-31 23:59:59'
        ''')

    # new records
    curs.execute('''
        insert into LETO_DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        ) select
            terminal_id,
            terminal_city,
            terminal_type,
                terminal_address
        from LETO_STG_TERMINALS_NEW
    ''')


    
    curs.execute('''
        insert into LETO_DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        ) select
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
            from LETO_STG_TERMINALS_CHANGED
        ''')



# инициализация и чтение файлов для transaction (LETO_STG_TRANSACTIONS)
def init_read_transaction_stg(filePath):
    try:
        curs.execute(''' 
        create table LETO_STG_TRANSACTIONS (
            transaction_id varchar(100),
            transaction_date date,
            amount decimal(10,2),
            card_num varchar(100),
            oper_type varchar(100),
            oper_result varchar(100),
            terminal varchar(100)
        )
        ''')
        print('LETO_STG_TRANSACTIONS создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_TRANSACTIONS не создана')

    df = pd.read_csv(filePath, sep=";", decimal = ',')

    try:
        curs.executemany('''
        insert into LETO_STG_TRANSACTIONS (
            transaction_id,
            transaction_date,
            amount,
            card_num,
            oper_type,
            oper_result,
            terminal
        ) values (
            ?,?,?,?,?,?,?
        )
        ''', df.values.tolist())
        print('LETO_STG_TRANSACTIONS ЗАПОЛНЕНА')
    except Exception as e:
        print(str(e))


# создание и инициализация фактической таблицы для transactions (LETO_DWH_FACT_TRANSACTIONS)
def init_transactions():
    try:
        curs.execute('''
        create table LETO_DWH_FACT_TRANSACTIONS(
            trans_id varchar(128),
            trans_date date,
            amt decimal(10,2),
            card_num varchar(128),
            oper_type varchar(128),
            oper_result varchar(128),
            terminal varchar(128)
            )
        ''')
        print('LETO_DWH_FACT_TRANSACTIONS создана')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_FACT_TRANSACTIONS не создана')

    try:
        curs.execute('''
        insert into LETO_DWH_FACT_TRANSACTIONS (
            trans_id,
            trans_date,
            amt,
            card_num,
            oper_type,
            oper_result,
            terminal
        ) select
            transaction_id,
            transaction_date,
            amount,
            card_num,
            oper_type,
            oper_result,
            terminal
        from LETO_STG_TRANSACTIONS
        ''')
        print('LETO_DWH_FACT_TRANSACTIONS ЗАПОЛНЕНА')
    except Exception as e:
        print(str(e))


# удаление таблиц
def deleteTmpTables():
    tables = ['LETO_STG_PASS_BL', 'LETO_DWH_FACT_PASS_BL',
    'LETO_STG_TERMINALS','LETO_STG_TERMINALS_NEW','LETO_STG_TERMINALS_DELETE','LETO_STG_TERMINALS_CHANGED',
    'LETO_DWH_FACT_TRANSACTIONS', 'LETO_STG_TRANSACTIONS']
    for table in tables:
        try:
            curs.execute(f'drop table {table}')
            print(f'Таблица {table} удалена')
        except Exception as e:
            print(str(e))


# просмотр таблиц
def showTable(tableName):
    print('_'*10+tableName+10*'_')
    curs.execute(f'select * from {tableName}')
    for row in curs.fetchall():
        print(row)


def clientTable():
    try:
        curs.execute('drop table LETO_DWH_DIM_CLIENTS')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('''
            create table LETO_DWH_DIM_CLIENTS(
                client_id varchar(128) primary key,
                last_name varchar(128),
                first_name varchar(128),
                patronymic varchar(128),
                date_of_birth date,
                passport_num varchar(128),
                passport_valid_to date,
                phone varchar(128),
                create_dt date,
                update_dt date
            )
        ''')
        print('LETO_DWH_DIM_CLIENTS создана')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_CLIENTS не создана')


    try:
        curs.execute('''
                insert into LETO_DWH_DIM_CLIENTS (
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone, create_dt, update_dt
            ) select
                client_id, last_name, first_name, patronymic, date_of_birth,
                passport_num, passport_valid_to, phone, create_dt,
                update_dt from bank.clients
            ''')
        print('LETO_DWH_DIM_CLIENTS заполнена')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_CLIENTS пустая')


def cardsTable():
    try:
        curs.execute('drop table LETO_DWH_DIM_CARDS')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('''
            create table LETO_DWH_DIM_CARDS(
                card_num varchar(128) primary key,
                account_num varchar(128),
                create_dt date,
                update_dt date
            )
        ''')
        print('LETO_DWH_DIM_CARDS создана')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_CARDS не создана')

    try:
        curs.execute('''
                insert into LETO_DWH_DIM_CARDS (
                    card_num, account_num, create_dt, update_dt
                ) select
                    card_num, account, create_dt, update_dt from bank.cards
            ''')
        print('LETO_DWH_DIM_CARDS заполнена')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_CARDS пустая')


def accountTable():
    try:
        curs.execute('drop table LETO_DWH_DIM_ACCOUNTS')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('''
            create table LETO_DWH_DIM_ACCOUNTS(
                account_num varchar(128) primary key,
                valid_to date,
                client varchar(128),
                create_dt date,
                update_dt date
            )
        ''')
        print('LETO_DWH_DIM_ACCOUNTS создана')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_ACCOUNTS не создана')

    try:
        curs.execute('''
                insert into LETO_DWH_DIM_ACCOUNTS (
                account_num, valid_to, client, create_dt, update_dt
            ) select
                account, valid_to, client, create_dt, update_dt from bank.accounts
            ''')
        print('LETO_DWH_DIM_ACCOUNTS заполнена')
    except Exception as e:
        print(str(e))
        print('LETO_DWH_DIM_ACCOUNTS пустая')



def fraud_contract():
    try:
        curs.execute('drop table LETO_STG_CONTRACT_NOT_VALID')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop view LETO_STG_CARDS_NOT_VALID')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop view LETO_STG_PASSPORT_FRAUD_VIEW0')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('drop view LETO_STG_PASSPORT_FRAUD_VIEW')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop table LETO_REP_FRAUD')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('''
            create table LETO_STG_CONTRACT_NOT_VALID as
                select
                    t1.account_num, t2.passport_num, t2.phone,
                    t2.last_name || ' ' || t2.first_name
                    || ' ' || t2.patronymic as fio, t1.valid_to
                from LETO_DWH_DIM_ACCOUNTS t1
                left join LETO_DWH_DIM_CLIENTS t2
                on t1.client = t2.client_id
                where t2.passport_valid_to > t1.valid_to
        ''')
        print('LETO_STG_CONTRACT_NOT_VALID создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_CONTRACT_NOT_VALID не создана')


    try:
        curs.execute('''
            create view LETO_STG_CARDS_NOT_VALID as
                select
                    rtrim(t1.card_num) as card_num,
                    t2.passport_num,
                    t2.fio,
                    t2.phone
                from LETO_DWH_DIM_CARDS t1
                inner join LETO_STG_ACCOUNT_NOT_VALID t2
                on t1.account_num = t2.account_num
        ''')
        print('LETO_STG_CARDS_NOT_VALID создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_CARDS_NOT_VALID не создана')


    try:
        curs.execute('''
            create view LETO_STG_PASSPORT_FRAUD_VIEW0 as
                select
                    t2.fio, t2.passport_num as passport, t2.phone,
                    t1.trans_date as event_dt
                from LETO_DWH_FACT_TRANSACTIONS t1
                inner join LETO_STG_CARDS_NOT_VALID t2
                on t1.card_num = t2.card_num
        ''')
        print('LETO_STG_PASSPORT_FRAUD_VIEW0 создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_PASSPORT_FRAUD_VIEW0 не создана')



    try:
        curs.execute('''
                create view LETO_STG_PASSPORT_FRAUD_VIEW as
                    select 
                    distinct *
                    from LETO_STG_PASSPORT_FRAUD_VIEW0
            ''')
        print('LETO_STG_PASSPORT_FRAUD_VIEW создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_PASSPORT_FRAUD_VIEW не создана')



    try:
        curs.execute('''
            create table LETO_REP_FRAUD(
                event_dt date,
                passport varchar(128),
                fio varchar(364),
                phone varchar(128),
                event_type varchar(128),
                report_dt date default current_timestamp
            )
        ''')
        print('LETO_REP_FRAUD создана')
    except Exception as e:
        print(str(e))
        print('LETO_REP_FRAUD не создана')


    try:
        curs.execute('''
                insert into LETO_REP_FRAUD (
                    event_dt,
                    passport,
                    fio,
                    phone,
                    event_type
                ) select
                    event_dt,
                    passport,
                    fio,
                    phone,
                    'contract'
            from LETO_STG_PASSPORT_FRAUD_VIEW
            ''')
        print('LETO_REP_FRAUD заполнена')
    except Exception as e:
        print(str(e))
        print('LETO_REP_FRAUD пустая')



def fraud_passport():
    try:
        curs.execute('drop table LETO_STG_CLIENT_NOT_VALID')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop view LETO_STG_ACCOUNT_NOT_VALID')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop view LETO_STG_CARDS_NOT_VALID')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop view LETO_STG_PASSPORT_FRAUD_VIEW0')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')

    try:
        curs.execute('drop view LETO_STG_PASSPORT_FRAUD_VIEW')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')


    try:
        curs.execute('drop table LETO_REP_FRAUD')
        print('таблица удалена')
    except Exception as e:
        print(str(e))
        print('таблица не существует')




    try:
        curs.execute('''
            create table LETO_STG_CLIENT_NOT_VALID as
                select
                    t1.client_id,
                    t1.passport_num,
                    t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
                    t1.phone,
                    t1.passport_valid_to
                from LETO_DWH_DIM_CLIENTS t1
                inner join
                LETO_DWH_FACT_PASS_BL t2 on
                t1.passport_num = t2.passport_num or t2.entry_dt > t1.passport_valid_to
        ''')
        print('LETO_STG_CLIENT_NOT_VALID создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_CLIENT_NOT_VALID не создана')



    try:
        curs.execute('''
                create view LETO_STG_ACCOUNT_NOT_VALID as
                    select
                        t1.account_num,
                        t2.passport_num,
                        t2.fio,
                        t2.phone
                    from LETO_DWH_DIM_ACCOUNTS t1
                    inner join LETO_STG_CLIENT_NOT_VALID t2
                    on t1.client = t2.client_id
                    where t2.passport_valid_to is not null
            ''')
        print('LETO_STG_ACCOUNT_NOT_VALID создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_ACCOUNT_NOT_VALID не создана')



    try:
        curs.execute('''
                create view LETO_STG_CARDS_NOT_VALID as
                    select
                        rtrim(t1.card_num) as card_num,
                        t2.passport_num,
                        t2.fio,
                        t2.phone
                    from LETO_DWH_DIM_CARDS t1
                    inner join LETO_STG_ACCOUNT_NOT_VALID t2
                    on t1.account_num = t2.account_num
            ''')
        print('LETO_STG_CARDS_NOT_VALID создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_CARDS_NOT_VALID не создана')


    try:
        curs.execute('''
                create view LETO_STG_PASSPORT_FRAUD_VIEW0 as
                    select
                        t2.fio,
                        t2.passport_num as passport,
                        t2.phone,
                        t1.trans_date as event_dt
                    from LETO_DWH_FACT_TRANSACTIONS t1
                    inner join LETO_STG_CARDS_NOT_VALID t2
                    on t1.card_num = t2.card_num
                    
            ''')
        print('LETO_STG_PASSPORT_FRAUD_VIEW0 создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_PASSPORT_FRAUD_VIEW0 не создана')


    try:
        curs.execute('''
                create view LETO_STG_PASSPORT_FRAUD_VIEW as
                    select 
                    distinct *
                    from LETO_STG_PASSPORT_FRAUD_VIEW0
            ''')
        print('LETO_STG_PASSPORT_FRAUD_VIEW создана')
    except Exception as e:
        print(str(e))
        print('LETO_STG_PASSPORT_FRAUD_VIEW не создана')


    try:
        curs.execute('''
            create table LETO_REP_FRAUD(
                event_dt date,
                passport varchar(128),
                fio varchar(364),
                phone varchar(128),
                event_type varchar(128),
                report_dt date default current_timestamp
            )
        ''')
        print('LETO_REP_FRAUD создана')
    except Exception as e:
        print(str(e))
        print('LETO_REP_FRAUD не создана')

    try:
        curs.execute('''
                insert into LETO_REP_FRAUD (
                    event_dt,
                    passport,
                    fio,
                    phone,
                    event_type
                ) select
                    event_dt,
                    passport,
                    fio,
                    phone,
                    'passport'
            from LETO_STG_PASSPORT_FRAUD_VIEW
            ''')
        print('LETO_REP_FRAUD заполнена')
    except Exception as e:
        print(str(e))
        print('LETO_REP_FRAUD пустая')





def dayreport_01032021():
    # файлы для загрузки
    pass_files = ['passport_blacklist_01032021.xlsx']
    term_files = ['terminals_01032021.xlsx']
    trans_files =['transactions_01032021.txt']

    # просмотр файлов
    print(pass_files)
    print(term_files)
    print(trans_files)

    # вызов функции удаления таблиц
    deleteTmpTables()

    # вызов функций passport_blacklist
    for file in pass_files:
        init_read_passports_stg(file)
        init_passports()
        #showTable('LETO_DWH_FACT_PASS_BL')
        time.sleep(1)

    # вызов функций term_files
    for file in term_files:
        init_read_terminals_stg(file)
        init()
        createTableNewRows()
        createTableDeleteRows()
        createTableChangedRows()
        updateTerminalsHist()
        #showTable('LETO_DWH_DIM_TERMINALS_HIST')
        time.sleep(1)

    # вызов функций trans_files
    for file in trans_files:
        init_read_transaction_stg(file)
        #showTable('LETO_STG_TRANSACTIONS')
        init_transactions()
        #showTable('LETO_DWH_FACT_TRANSACTIONS')
        time.sleep(1)

    clientTable()
    cardsTable()
    accountTable()

    fraud_contract()
    fraud_passport()
    time.sleep(3)


def dayreport_02032021():
    # файлы для загрузки
    pass_files = ['passport_blacklist_02032021.xlsx']
    term_files = ['terminals_02032021.xlsx']
    trans_files =['transactions_02032021.txt']

    # просмотр файлов
    print(pass_files)
    print(term_files)
    print(trans_files)

    # вызов функции удаления таблиц
    deleteTmpTables()

    # вызов функций passport_blacklist
    for file in pass_files:
        init_read_passports_stg(file)
        init_passports()
        #showTable('LETO_DWH_FACT_PASS_BL')
        time.sleep(1)

    # вызов функций term_files
    for file in term_files:
        init_read_terminals_stg(file)
        init()
        createTableNewRows()
        createTableDeleteRows()
        createTableChangedRows()
        updateTerminalsHist()
        #showTable('LETO_DWH_DIM_TERMINALS_HIST')
        time.sleep(1)

    # вызов функций trans_files
    for file in trans_files:
        init_read_transaction_stg(file)
        #showTable('LETO_STG_TRANSACTIONS')
        init_transactions()
        #showTable('LETO_DWH_FACT_TRANSACTIONS')
        time.sleep(1)

    clientTable()
    cardsTable()
    accountTable()

    fraud_contract()
    fraud_passport()
    time.sleep(3)


def dayreport_03032021():
    # файлы для загрузки
    pass_files = ['passport_blacklist_03032021.xlsx']
    term_files = ['terminals_03032021.xlsx']
    trans_files =['transactions_03032021.txt']

    # просмотр файлов
    print(pass_files)
    print(term_files)
    print(trans_files)

    # вызов функции удаления таблиц
    deleteTmpTables()

    # вызов функций passport_blacklist
    for file in pass_files:
        init_read_passports_stg(file)
        init_passports()
        #showTable('LETO_DWH_FACT_PASS_BL')
        time.sleep(1)

    # вызов функций term_files
    for file in term_files:
        init_read_terminals_stg(file)
        init()
        createTableNewRows()
        createTableDeleteRows()
        createTableChangedRows()
        updateTerminalsHist()
        #showTable('LETO_DWH_DIM_TERMINALS_HIST')
        time.sleep(1)

    # вызов функций trans_files
    for file in trans_files:
        init_read_transaction_stg(file)
        #showTable('LETO_STG_TRANSACTIONS')
        init_transactions()
        #showTable('LETO_DWH_FACT_TRANSACTIONS')
        time.sleep(1)

    clientTable()
    cardsTable()
    accountTable()

    fraud_contract()
    fraud_passport()
    time.sleep(3)







# вызов функции по дням

dayreport_01032021()

dayreport_02032021()

dayreport_03032021()


