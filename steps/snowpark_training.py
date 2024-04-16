from snowflake.snowpark import Session
import sf_environment as env
import time


POS_TABLES = ['country', 'franchise', 'location', 'menu', 'truck', 'order_header', 'order_detail']
CUSTOMER_TABLES = ['customer_loyalty']
TABLE_DICT = {
    "pos": {"schema": "RAW_POS", "tables": POS_TABLES},
    "customer": {"schema": "RAW_CUSTOMER", "tables": CUSTOMER_TABLES}
}
class LoadData:
    def __init__(self,session):
        self.session = session
    def load_raw_table(self):
        pass
    def load_all_raw_tables(self):
        sql = self.session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()
        print('Qry Executed')
        for s3dir, data in TABLE_DICT.items():
            tnames = data['tables']
            sch = data['schema']
            for item in tnames:
                #print(f"{item} loading...")
                if item in ['order_header', 'order_detail']:
                    for yr in ['2019','2020','2021']:
                        self.load_raw_table(item=item,s3dir=s3dir,sch=sch,year=yr)
                else:
                    self.load_raw_table(item=item,s3dir=s3dir,sch=sch)
                        
    def load_raw_table(self,item:str,s3dir:str,sch:str,year:str = None):
        self.session.use_schema(sch)
        if year is None:
            location = "@external.frostbyte_raw_stage/{}/{}".format(s3dir, item)
        else:
            print('\tLoading year {}'.format(year)) 
            location = "@external.frostbyte_raw_stage/{}/{}/year={}".format(s3dir, item, year)
        df = self.session.read.option("compression", "snappy").parquet(location)
        df.copy_into_table(item)
        print(f"{item} loaded")

if __name__ =='__main__':
    print(__name__)
    sp_session = Session.builder.configs(options=env.connection_paramters).create()
    s = sp_session.sql('USE DATABASE HOL_DB').collect()
    print(s)
    data_load = LoadData(session=sp_session)
    data_load.load_all_raw_tables()
    
    print('Session got closed')


