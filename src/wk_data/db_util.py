"""
Created on Wed Jul 12 17:16:46 2017

Modified on 20170717

@author: Xin-Ji Liu

Modified on 20211227

@author: lx
"""


from sqlalchemy import create_engine
from sqlalchemy import types
import datetime
import cx_Oracle  # user guide: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def create_oracle_engine(config, dbloc):
    if dbloc == 'wktz':
        dsn_config = config['dsn']['wkdsn']
        user = config['wktz']['user']
        password = config['wktz']['password']
    elif dbloc == 'cms':
        dsn_config = config['dsn']['wind']
        user = config['cms']['user']
        password = config['cms']['password']
    # print("db_util", user, password)
    else:
        raise ValueError("dbloc not found")

    ip, port, service_name = dsn_config['ip'], dsn_config['port'], dsn_config['service_name']

    # oracle+cx_oracle://user:pass@hostname:port[/dbname][?service_name=<service>[&key=value&key=value...]]
    engine = create_engine(
        f"oracle+cx_oracle://{user}:{password}@{ip}:{port}/?service_name={service_name}&encoding=UTF-8&nencoding=UTF-8")
    return engine


def create_oracle_connection(config, dbloc):
    pass


def make_dsn(dsn_config):
    # print("dsn", dsn_config)
    ip, port, service_name = dsn_config['ip'], dsn_config['port'], dsn_config['service_name']
    return cx_Oracle.makedsn(ip, port, service_name=service_name)


class DBComOrc:
    def __init__(self, dbloc, config):
        """
        initialization
        """
        self.dbloc = dbloc
        self._config = config

    def dbconn(self):
        """
        database connecting
        """
        if self.dbloc == 'wktz':
            dsn = make_dsn(self._config['dsn']['wkdsn'])
            user = self._config['wktz']['user']
            password = self._config['wktz']['password']
        elif self.dbloc == 'cms':
            dsn = make_dsn(self._config['dsn']['wind'])
            user = self._config['cms']['user']
            password = self._config['cms']['password']
        # print("db_util", user, password)
        else:
            raise ValueError("dbloc not found")

        self.conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        self.cursor = self.conn.cursor()

    def dbdisconn(self):
        """
        database disconnecting
        """
        self.cursor.close()
        self.conn.close()

    def odbcrowtolist(self, odbcrow):
        """
        support function: tranfer odbcrow format to list
        """
        odbclist = []
        error_id = 0
        error_reason = ''
        try:
            for i in range(0, len(odbcrow)):
                if len(odbcrow[0]) > 1:
                    tmplist = []
                    for j in range(0, len(odbcrow[0])):
                        tmplist.append(odbcrow[i][j])
                    odbclist.append(tmplist)
                else:
                    odbclist.append(odbcrow[i][0])
        except:
            error_id = -1
            error_reason = 'failed converting odbrow to list'
        return (odbclist, error_id, error_reason)

    def cursorexecute(self, sql):
        """
        support function execute sql sentence with cursor curesor_name and it will return data with format list
        """
        result = []
        error_id = 0
        error_reason = ''
        #        try:
        self.cursor.execute(sql)
        if sql[0:6] == 'select':
            tmpresult = self.cursor.fetchmany(100000)  # use fetchmany rather than fetchall
            while tmpresult:
                (tmpresult, error_id, error_reason) = self.odbcrowtolist(tmpresult)
                result.extend(tmpresult)
                # print('--- downloaded '+str(len(result)) + ' rows ---') 
                tmpresult = self.cursor.fetchmany(100000)
                # print('successfully executing sql sentences: ' + sql)
        else:
            pass
        #        except:
        #            error_id = -1
        #            error_reason = 'failed executing sql sentences ' + sql
        #            print('failed executing sql sentences: ' + sql)
        return (result, error_id, error_reason)

    def sqlexecute(self, sql):
        """
        execute sql sentence
        """
        self.dbconn()
        result, error_id, error_reason = self.cursorexecute(sql)
        if sql[0:6] != 'select':
            self.conn.commit()
        self.dbdisconn()
        return result, error_id, error_reason

    def ins_df(self, df, tabname):
        """
        insert data frame to database
        """
        error_id = -1
        error_reason = ''
        config = self._config['db']['wktz']
        user = config['user']
        password = config['password']
        ip = config['ip']
        port = config['port']
        db = config['db']

        engine_path = f"oracle+cx_oracle://{user}:{password}@{ip}:{port}/{db}"
        engine = create_engine(engine_path)
        try:
            dtyp = {c: types.VARCHAR(df[c].str.len().max()) for c in df.columns[df.dtypes == 'object'].tolist()}
            df.to_sql(tabname, engine, if_exists='append', index=False, chunksize=100, dtype=dtyp)
            error_reason = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + \
                           ' successfully writing data frame to database, length: ' + str(len(df))
            print(error_reason)
            error_id = 0
        except:
            error_reason = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") + \
                           ' failed writing data frame to database'
            print(error_reason)
        finally:
            engine.dispose()
        return error_id, error_reason


