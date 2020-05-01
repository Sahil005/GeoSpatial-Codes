#!/usr/bin/env python
# coding: utf-8

# In[3]:


#!/usr/bin/env python
# coding: utf-8

# In[57]:


import snowflake.connector as sf
import snowflake.connector
import pandas as pd
import geopandas as gpd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)
#from graphics import point
from graphics import *
import geopandas
from datetime import datetime
from shapely.geometry import Point
import sys
sys.path.append('C:/Users/User/Desktop')
import SF_CRED
user=SF_CRED.SF_USER
pwd=SF_CRED.SF_PASSWORD
db=SF_CRED.DB
schema=SF_CRED.FIVETRAN_SCHEMA
warehouse=SF_CRED.BI_WH
role=SF_CRED.ROLE




# In[ ]:



# In[12]:


conn = snowflake.connector.connect(user=user,password=pwd,account='am62076.ap-southeast-2',role=role,warehouse=warehouse,database=db)
sql = '''SELECT a.store,
                  first_active_date,
                  last_active_date,
                  c.lat AS STORE_LAT,
                  c.lng AS STORE_LNG,
                  c.name,l.city_name
         FROM "PC_STITCH_DB"."FIVETRAN1_BI"."STORE_TRACK_ACTIVE_INACTIVE" a
         left join
             (SELECT *
            FROM PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.LOCATION
            WHERE lat<> '') c
           ON lower(a.store)=lower(c.name)
          left join PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.CITY l on l.CITY_ID = c.city_id
          where store_LAT is not null or store_lng is not null'''
#conn.cursor().execute(sql)

df = pd.read_sql(sql,con=conn)
#print(df)
df[['STORE_LNG', 'STORE_LAT']] = df[['STORE_LNG', 'STORE_LAT']].apply(pd.to_numeric)
#gdf = geopandas.GeoDataFrame(df,geometry=geopandas.points_from_xy(x=df.STORE_LNG, y=df.STORE_LAT))

geometry = [Point(xy) for xy in zip(df.STORE_LNG,df.STORE_LAT)]
df = df.drop(['STORE_LNG', 'STORE_LAT'], axis=1)
crs = {'init': 'epsg:4326'}
gdf = GeoDataFrame(df, crs=crs, geometry=geometry)
#print(gdf)


filename = 'Store_data.shp'
gdf.to_file(driver = 'ESRI Shapefile', filename = filename) 


# In[6]:


gdf.head()


# In[7]:


len(gdf)


# In[ ]:


gdf1.info()


# In[63]:


conn = snowflake.connector.connect(user=user,password=pwd,account='am62076.ap-southeast-2',role=role,warehouse=warehouse,database=db)
query2 = """select * from 
(SELECT st.*,row_number() over (partition BY lat,lng ORDER BY haversine(lat,lng,store_lat,store_lng)) AS rank
   FROM
     (SELECT a.*,
             b.name AS nearest_store,
             b.store_lat,
             b.store_lng,
             b.first_active_date,
             b.last_active_date,
             round(haversine(a.lat,a.lng,b.store_lat,b.store_lng),2) AS distance,
             (CASE WHEN to_date(lead_date) between to_date(b.first_active_date) AND to_date(b.last_active_date) then b.store END) AS Nearest_s             
      FROM
        (SELECT tt.lead_id,
                lat,
                lng,
                lead_date,
                b.pub_appt_id,
                c.store_name AS insp_store,
                count(*),
                mark,
                lead_App_Type
         FROM
           (SELECT max(lead_id) AS lead_id,
                   lat,
                   lng,
                   count(lead_id)AS num_leads_latlng,
                   max(cast(created_at AS timestamp_ntz)) AS lead_date,
                   t.mark,
                   t.lead_App_Type
            FROM
              (SELECT lead_id,
                      lat,
                      lng,
                      CREATED_AT,
                      'Leads' AS mark,
                      lead_type AS lead_App_Type
               FROM PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.LEAD
                       WHERE LEAD_TYPE not like '%one%'
                             AND lat is not null
                             AND to_date(CREATED_AT)>='2019-08-01'
               UNION 
               SELECT l.LEAD_ID,
                      a.user_lat,
                      a.user_lng,
                      l.CREATED_AT,
                      'Appointments' AS mark,
                      b.Appointment_Type AS lead_App_Type
               FROM PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.LEAD l
               left join PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.APPOINTMENT_GEO_DATA a
                         ON l.LEAD_NUMBER = a.pub_appt_id
               left join PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.APPOINTMENT b
                         ON l.lead_number=b.pub_appt_id
                         WHERE LEAD_TYPE not like '%one%'
                          AND user_lat is not null
                          AND to_date(a.CREATED_AT)>='2019-08-01')t
            GROUP BY 2,
                     3,
                     6,
                     7)tt
         left join PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.APPOINTMENT b
           ON tt.LEAD_id = b.lead_ID
         left join PC_STITCH_DB.MONGO_INSENGINEAPI_PROD_INSENGINEAPI_PROD.INSPECTION_LOG c
           ON c.APPOINTMENT_ID = b.PUB_APPT_ID
           WHERE num_leads_latlng = 1
             AND lat <= 37
             AND lat >= 8
             AND lng <= 97
             AND lng >= 68
         GROUP BY 1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  8,
                  9
         ORDER BY 7 DESC)a
      left join
          (SELECT a.store,
                  first_active_date,
                  last_active_date,
                  c.lat AS store_lat,
                  c.lng AS store_lng,
                  c.name,l.city_name
         FROM "PC_STITCH_DB"."FIVETRAN1_BI"."STORE_TRACK_ACTIVE_INACTIVE" a
         left join
             (SELECT *
            FROM PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.LOCATION
            WHERE lat<> '') c
           ON lower(a.store)=lower(c.name)
          left join PC_STITCH_DB.WEBSITE_DEALERENGINE_PROD.CITY l on l.CITY_ID = c.city_id) b
        ON haversine(a.lat,a.lng,b.store_lat,b.store_lng) <= 1000000
     where lower(nearest_store) not like '%hi-%') st
     where nearest_s is not null)
   WHERE rank =1"""
print(query2)
df1 = pd.read_sql(query2,con=conn)
df.head()


# In[61]:


df1[['LNG', 'LAT']] = df1[['LNG', 'LAT']].apply(pd.to_numeric)
#dateTimeObj = df1['LEAD_DATE']
#df1['LEAD_DATE'] = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
#df1['LEAD_DATE'] = df1[['LEAD_DATE']].apply(pd.datetime.strftime("%d-%b-%Y (%H:%M:%S.%f)"))
#df1[['LEAD_DATE']] = df1[['LEAD_DATE']].apply(pd.to_object)
#gdf = geopandas.GeoDataFrame(df,geometry=geopandas.points_from_xy(x=df.STORE_LNG, y=df.STORE_LAT))


geometry = [Point(xy) for xy in zip(df1.LNG,df1.LAT)]
#df1 = df1.drop(['LNG', 'LAT'], axis=1) #you are deleting the lat lng and just taking the geometry with you. but for text purposes you need lat lng as well so dont drop
crs = {'init': 'epsg:4326'}
gdf1 = GeoDataFrame(df1, crs=crs, geometry=geometry)
gdf1['LEAD_DATE'] = gdf1.apply(lambda row: str(row['LEAD_DATE']),axis=1)
#print(gdf1)

#schema = {
#    'geometry': 'Point',
 #   'properties': {
  #      'LEAD_ID': 'int64',
    #    'NUM_LEADS_LATLNG': 'Int64',
    #    'LEAD_DATE': 'str',
     #   'MARK': 'str',
     #   'LEAD_APP_TYPE': 'str',
#}}

filename1 = 'lead_data_shapefile4.shp'
gdf1.to_file(driver = 'ESRI Shapefile', filename = filename1) 

