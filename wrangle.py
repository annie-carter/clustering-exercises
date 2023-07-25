
import pandas as pd
import numpy as np
import os
from env import hostname, user, password
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler,QuantileTransformer
import warnings
warnings.filterwarnings("ignore")

  #---------- ACQUIRE -----------

def get_connection(db, user=user, hostname =hostname, password=password):
    ''' The below functions were created to acquire the 2017 Zillow data from CodeUp database and make a SQL query to meet project goals 
        removing unnecessary information.
    '''
    return f'mysql+pymysql://{user}:{password}@{hostname}/{db}'


def get_zillow_data():
    
    filename = 'zillow.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)    
    else:    
        url = get_connection('zillow')   
        sql = '''SELECT *

        FROM properties_2017
        FULL JOIN predictions_2017 USING (parcelid)
        LEFT JOIN airconditioningtype USING (airconditioningtypeid)
        LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
        LEFT JOIN buildingclasstype USING (buildingclasstypeid)
        LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
        LEFT JOIN propertylandusetype USING (propertylandusetypeid)
        LEFT JOIN storytype USING (storytypeid)
        LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
        WHERE transactiondate <= '2017-12-31';
        '''   
        df = pd.read_sql(sql, url)        
        df.to_csv(filename, index=False)    
        return df


def get_mall_data(sql):
    sql = 'select * from customers'
    url = get_connection('mall_customers')
    mall = pd.read_sql(sql, url, index_col='customer_id')
    return mall
    
    # Check if the file exists
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # SQL query to retrieve the required data
        sql_query = '''
        SELECT
            prop.*,
            predictions_2017.logerror,
            predictions_2017.transactiondate,
            air.airconditioningdesc,
            arch.architecturalstyledesc,
            build.buildingclassdesc,
            heat.heatingorsystemdesc,
            landuse.propertylandusedesc,
            story.storydesc,
            construct.typeconstructiondesc
        FROM properties_2017 prop
        JOIN (
            SELECT parcelid, MAX(transactiondate) max_transactiondate
            FROM predictions_2017
            GROUP BY parcelid
        ) pred USING(parcelid)
        JOIN predictions_2017 ON pred.parcelid = predictions_2017.parcelid
                              AND pred.max_transactiondate = predictions_2017.transactiondate
        LEFT JOIN airconditioningtype air USING (airconditioningtypeid)
        LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid)
        LEFT JOIN buildingclasstype build USING (buildingclasstypeid)
        LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid)
        LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid)
        LEFT JOIN storytype story USING (storytypeid)
        LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid)
        WHERE prop.latitude IS NOT NULL
          AND prop.longitude IS NOT NULL
          AND predictions_2017.transactiondate LIKE '2017%%'
        '''
        # Retrieve data from the database and save it as a CSV file
        df = pd.read_sql(sql_query, get_connection('zillow'))
        df.to_csv(filename, index=False)
        return df
