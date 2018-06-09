import requests
import json
import time
import urllib
import csv
import os
import sys
import datetime
import MySQLdb
import subprocess
import warnings
import logging
# -*- coding: utf-8 -*-

__author__ = 'abhinandan'

warnings.filterwarnings('ignore')

#log properties
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logpath = '/home/ubuntu/log/automationlog/amazon/amazon_'+str(datetime.datetime.today().strftime('%Y-%m-%d'))+'.log'
directory = os.path.dirname(logpath)
if not os.path.exists(directory):
    os.makedirs(directory)

# create a file handler
handler = logging.FileHandler(logpath)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

#---------------------Check UPC from Mysql----------------------------------
def checkUPCMySQL(db,asinfilewrite,asintable,asin):
    upc="0";
    step1cursor = db.cursor()
    step1cursor.execute("SELECT upc,count(upc) as rowcount FROM "+asintable+" where ASIN='"+asin+"'")
    row = step1cursor.fetchone()
    if row[0] != None:
        upc=row[0]
    else:
        upc=checkAINUPC(asin,asinfilewrite)
        if upc == None:
            upc="0"
    step1cursor.close()

    return upc
#-----------------------------------------------------------------------------


#---------------------Check UPC from API---------------------------------------

def checkAINUPC(asin,asinfilewrite):
    upc="0"
    try:
        upcrequest = requests.get("http://production12.getpriceapp.com/v2/price/asin-to-upc/?asin="+urllib.quote(asin.encode('utf-8')))
        jsonUPC = json.loads(upcrequest.content)
        asin= jsonUPC['asin']
        upc = jsonUPC['upc']
        if upc!= "":
            data = [[asin, upc]]
            asinfilewrite.writerows(data)
        else:
            upc="0"
    except:
        pass
    return upc;

#-------------------------------------------------------------------------------


#---------------------Load data into MySQL Staging------------------------------
def load_mysql_staging(dbconn,table_name,filepath):
    truncatetable_cursor = dbconn.cursor()
    loaddate_cursor = dbconn.cursor()
     #Truncate Table
    truncatetable_cursor.execute("truncate "+table_name)
    truncatetable_cursor.close();
    dbconn.commit()
    #Load Data into table
    loaddate_cursor.execute("load data local infile '"+filepath+"' into table "+table_name+" fields terminated by ',' SET FIELD_19 = NOW()")
    loaddate_cursor.close()
    dbconn.commit()
    print("Data Load into Staging table : "+table_name)
    logger.info("Data Load into Staging table : "+table_name)
#--------------------------------------------------------------------------------


#--------------------Load Data into ASIN UPC table-------------------------------
def load_mysql_asinupc(dbconn,table_name,filepath):
    loaddate_cursor = dbconn.cursor()
    #Load Data into table
    loaddate_cursor.execute("load data local infile '"+filepath+"' into table "+table_name+" fields terminated by ',' SET AUDITTIME = NOW()")
    loaddate_cursor.close()
    dbconn.commit()
    print("Data Load into ASINtoUPC table : "+table_name)
    logger.info("Data Load into ASINtoUPC table : "+table_name)
#---------------------------------------------------------------------------------


#--------------------Parse JSON to CSV--------------------------------------------
def parseAmazonJSON(sourcefile,destinationfile):
    print("Amazon JSON To CSV Started")
    logger.info("Amazon JSON To CSV Started on file: "+str(destinationfile))
    #with open(os.getcwd()+"/items_amazon.com.jl") as f:
    with open(sourcefile) as f:
      lines = f.readlines()
    c = csv.writer(open(destinationfile,"w"))
    for line in lines:

        data = json.loads(line)

        braedcum = ""
        imagelinks = ""
        col1=""
        col2=""
        col3=""
        col4=""
        col5=""
        col6=""
        col7=""
        col8=""
        col9=""
        col10=""
        col11=""
        col12=""
        col13=""
        col14=""
        col15=""
        col16=""
        col17=""


        if 'category' in data:
            col1= data['category'].replace("\n", "")
        if 'asin' in data:
            col2 =  data['asin'].replace("\n", "")
        if 'reviews_count' in data:
            col3 = data['reviews_count'].replace("\n", "")
        if 'list_price' in data:
            col4 = data['list_price'].replace("\n", "")
        if 'description' in data:
            col5 = data['description'].replace(",","-").replace("\n"," ")
        if 'title' in data:
            col6 = data['title'].replace(",","-").replace("\n"," ")
        if 'url' in data:
            col7 = data['url'].replace("\n", "")
        if 'price' in data:
            col8 = data['price'].replace("\n", "")
        if 'rating' in data:
            col9 = data['rating'].replace("\n", "")
        if '_type' in data:
            col10 = data['_type'].replace("\n", "")
        if 'source_url' in data:
            col11 = data['source_url'].replace("\n", "")
        if 'prime_eligible' in data:
            col12 = data['prime_eligible'].replace("\n", "")
        if 'category_breadcrumb' in data:
            for catb in data['category_breadcrumb']:
                braedcum+=catb+' | '

        if 'images' in data:
            for images in data['images']:
                imagelinks+=images+' | '
            col13 = braedcum.replace("\n", "")
            col14 = imagelinks.replace("\n", "")
        if 'stock_info' in data:
            col15 = data['stock_info'].replace("\n", "")
        if 'seller' in data:
            col16 = data['seller'].replace("\n", "")
        if 'upc' in data:
            col17 = data['upc'].replace("\n", "")

        c.writerow([col1.replace(",","-").encode("utf-8"),col2.replace(",","-").encode("utf-8"),col3.replace(",","-").encode("utf-8"),col4.replace(",","-").encode("utf-8"),col5.replace(",","-").encode("utf-8"),col6.replace(",","-").encode("utf-8"),col7.replace(",","-").encode("utf-8"),col8.replace(",","-").encode("utf-8"),col9.replace(",","-").encode("utf-8"),col10.replace(",","-").encode("utf-8"),col11.replace(",","-").encode("utf-8"),col12.replace(",","-").encode("utf-8"),col13.replace(",","-").encode("utf-8"),col14.replace(",","-").encode("utf-8"),col15.replace(",","-").encode("utf-8"),col16.replace(",","-").encode("utf-8"),col17.replace(",","-").encode("utf-8")]);

#------------------------------------------------------------------------------------------------------


def addUPCCSV(destinationfile,updatedcsvfile,asinfilewrite,dbconn,asintable):
    with open(destinationfile, "rb") as f:
        c = csv.writer(open(updatedcsvfile,"w"))
        reader = csv.reader(f)
        for row in reader:
         row.append(checkUPCMySQL(dbconn,asinfilewrite,asintable,row[1]))
         print(row)
         c.writerow(row);




#------------------Delete Files From EC2-------------------------
def delete_local_files(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
    else:
        print("Sorry, I can not remove %s file." % filepath)
#-----------------------------------------------------------------


#-------------------------Create Target Table---------------------
def create_target_table(db,retailer_no,staging_table_name,target_table_name):
  logger.info("target table creation started , from staging table: "+staging_table_name+" to target table "+target_table_name)
  try:
        # prepare a cursor object using cursor() method
        step1cursor = db.cursor()
        step2cursor = db.cursor()
        step3cursor = db.cursor()
        step4cursor = db.cursor()
        step5cursor = db.cursor()
        step6cursor = db.cursor()
        step7cursor = db.cursor()


        #Trancate Query
        print "Truncate target table Started"
        logger.info("Truncate target table Started")
        step5="Truncate "+target_table_name
        print("Truncate target table query: "+step5)
        logger.info("Truncate target table query: "+step5)
        step5cursor.execute(step5)
        db.commit()
        step5cursor.close()


        #Step 1 query
        print "Step 1 Query Started"
        logger.info("Step 1 Query Started")
        step1="Insert into UPC_COMMON_ID_MAP2 Select TGT.COMMON_ID, SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID, now() from (SELECT FIELD_18 AS UPC,$Y AS RETAILER_CODE,FIELD_2 AS ITEM_ID FROM $X WHERE FIELD_18 <> 'UPC' AND FIELD_18 IS NOT NULL AND FIELD_18 <> ' ' AND FIELD_18 REGEXP '^[0-9]+$' GROUP BY UPC, RETAILER_CODE) SRC JOIN UPC_COMMON_ID_MAP2 TGT ON SRC.UPC = TGT.UPC and TGT.RETAILER_CODE <> $Y and TGT.RETAILER_CODE is null group by 1,2,3,4"
        step1=step1.replace("$Y",retailer_no).replace("$X",staging_table_name)
        print "Step 1 Query: "+step1
        logger.info("Step 1 Query: "+step1)
        step1cursor.execute(step1)
        step1cursor.close()
        db.commit()


        #step2 Query
        print "Step 2 Query Started"
        logger.info("Step 2 Query Started")
        step2="Truncate UPC_COMMON_ID_MAP_W3"
        print("Step 2 Query: "+step2)
        logger.info("Step 2 Query: "+step2)
        step2cursor.execute(step2)
        db.commit()
        step2cursor.close()

        #Step 3 Query
        print("Step 3 Query Started")
        logger.info("Step 3 Query Started")
        step3="Insert into UPC_COMMON_ID_MAP_W3 (UPC,RETAILER_CODE,ITEM_ID,AUDITTIME) Select SRC.UPC, SRC.Retailer_code, SRC.ITEM_ID, now() from (SELECT FIELD_18 AS UPC, $Y AS RETAILER_CODE, FIELD_2 AS ITEM_ID FROM $X WHERE FIELD_18 <> 'UPC' AND FIELD_18 IS NOT NULL AND FIELD_18 <> ' '  AND FIELD_18 REGEXP '^[0-9]+$' GROUP BY UPC, RETAILER_CODE) SRC LEFT JOIN UPC_COMMON_ID_MAP2 TGT ON SRC.UPC = TGT.UPC where TGT.UPC is null"
        step3=step3.replace("$Y",retailer_no).replace("$X",staging_table_name)
        print("Step 3 Query: "+step3)
        logger.info("Step 3 Query: "+step3)
        step3cursor.execute(step3)
        db.commit()
        step3cursor.close();

        #Step 4 Query
        print("Step 4 Query Started")
        logger.info("Step 4 Query Started")
        step4="Insert into UPC_COMMON_ID_MAP2 Select common_id + (Select max(common_id) from UPC_COMMON_ID_MAP2), upc, retailer_code, ITEM_ID, now() from UPC_COMMON_ID_MAP_W3;"
        print("Step 4 Query: "+step4)
        logger.info("Step 4 Query: "+step4)
        step4cursor.execute(step4)
        db.commit()
        step4cursor.close()

        #step5 Query
       # print "Step 5 Query Started"
       # step5="Truncate "+target_table_name
       # print("Step 5 Query: "+step5)
       # step5cursor.execute(step5)
       # db.commit()
       # step5cursor.close()

        #Step 6 Query
        #step6="create table $Z select a.FIELD_12 as ITEM_ID,a.FIELD_18 as TITLE,a.FIELD_20 as UPC,a.FIELD_7 as PRODUCT_URL,a.FIELD_6 as IMAGE_URL,a.FIELD_7 as AFFILIATE_URL,b.common_id as PRICE_ID,a.FIELD_11 + ' ' AS PRICE1,a.FIELD_5 + ' ' AS PRICE2,a.FIELD_9 as PRODUCT_CATEGORY,a.FIELD_19 as PRODUCT_CONDITION,a.FIELD_23 as TOP_RATED_SELLER,a.FIELD_24 as SELLER_RATING_PERCENTAGE from $X a, UPC_COMMON_ID_MAP2 b where a.FIELD_20 = b.upc and a.FIELD_19='Used' and b.retailer_code = $Y"
        step6="insert into $Z select a.FIELD_2 as ITEM_ID,a.FIELD_6 as TITLE,a.FIELD_5 as DESCRIPTION,a.FIELD_18 as UPC,a.FIELD_7 as PRODUCT_URL,SUBSTRING_INDEX(a.FIELD_14,'|',1)  as IMAGE_URL,CONCAT(a.FIELD_7,'&tag=getpriceapp-20')  as AFFILIATE_URL,b.common_id as PRICE_ID,SUBSTRING(a.FIELD_8,2)  + ' ' AS PRICE1,a.FIELD_5 + ' ' AS PRICE2,a.FIELD_9 as RATING,replace(a.FIELD_3,'-','') as REVIEW, a.FIELD_13 as CATEGORY_BREADCRUMB , now() as TIMEAUDIT from $X a, UPC_COMMON_ID_MAP2 b where a.FIELD_18 = b.upc and b.retailer_code = $Y"
        step6=step6.replace("$Y",retailer_no).replace("$X",staging_table_name).replace("$Z",target_table_name)
        print("Step 6 Query: "+step6)
        logger.info("Step 6 Query: "+step6)
        step6cursor.execute(step6)
        db.commit()
        step6cursor.close()

        #Step 7 Query
        print("Step 7 Started")
        step7="select count(*) from "+target_table_name
        step7cursor.execute(step7)
        print("Step 7 Query "+step7)
        result = step7cursor.fetchone()
        step7cursor.close();

        print("\nData Loaded into Table : "+target_table_name + " Total Row Count "+str(result[0]))
        logger.info("\nData Loaded into Table : "+target_table_name + " Total Row Count "+str(result[0]))
  except:
      pass

#--------------------------------------------------------------------------------------------------------------------




#------------------------------Download File From S3 -----------------------------------------------------------------

def download_file_s3(downloadloc,filename):
    os.chdir(downloadloc)
    logger.info(downloadloc)
    try:
        delete_local_files(downloadloc+"/"+filename)
    except:
        pass
    subprocess.call(['s3cmd', 'get', 's3://price-data-engineering/'+str(datetime.datetime.today().strftime('%Y-%m-%d'))+'/Amazon/'+filename])
   # subprocess.call(['s3cmd', 'get', 's3://price-data-engineering/2017-08-12/Amazon/'+filename])
    logger.info("S3 location: "+'s3://price-data-engineering/'+str(datetime.datetime.today().strftime('%Y-%m-%d'))+'/Amazon/'+filename)
#----------------------------------------------------------------------------------------------------------------------


#---------------------------------Check Amazon File is in S3 or Not ---------------------------------------------------

def list_s3objects():
    from boto.s3.connection import S3Connection
    conn = S3Connection('AKIAJZQU3GLH7LWFXCVA','O3KAcbi43Fwo+jqp3IdGq2eYrNwoq9uhsbnxRq84')
    bucket = conn.get_bucket('price-data-engineering')
    for key in bucket.list(prefix=str(datetime.datetime.today().strftime('%Y-%m-%d'))+'/Amazon/', delimiter='/'):
        return os.path.basename(key.name.encode('utf-8'))

#----------------------------------------------------------------------------------------------------------------------


#--------------------------------------Send mail Function -------------------------------------------------------------

def sendmail(mailsubject):
    filelog = open(logpath, 'r')
    filecontent = filelog.read()
    url = "http://production12.getpriceapp.com/data/notify/"
    d= {"body":str(filecontent),
    "subject":str(mailsubject)}
    r = requests.post(url,data=d)

#----------------------------------------------------------------------------------------------------------------------


#----------------------------------Delete Offer -------------------------------------------------------------------

def delete_offer(conn,target_table,retailer_no,retailer_name):

         delete_json= "{ \"script\": \"ctx._source.r.remove(\\\""+retailer_name+"\\\")\" }";

         step1cursor = conn.cursor()
         step2cursor = conn.cursor()

         select_sql = "select a.COMMON_ID,a.UPC,a.RETAILER_CODE,a.ITEM_ID,now() from (select MAPTABLE.COMMON_ID,MAPTABLE.UPC, MAPTABLE.RETAILER_CODE,MAPTABLE.ITEM_ID from PRICE_DOT_COM_STAGING.UPC_COMMON_ID_MAP2 MAPTABLE LEFT OUTER JOIN  "+ str(target_table) + " as TGT ON MAPTABLE.UPC = TGT.UPC WHERE TGT.UPC IS NULL and MAPTABLE.RETAILER_CODE = " + str(retailer_no)+ ") as a where not exists  (Select common_id, upc, retailer_code, item_id from PRICE_DOT_COM_STAGING.OFFER_DELETE b where a.upc = b.upc and a.retailer_code = " + str(retailer_no) + " and DATEDIFF(b.AUDITTIME, now())>=1)"
         insert_sql = "insert into PRICE_DOT_COM_STAGING.OFFER_DELETE "+select_sql;
         logger.info("Delete offer record inserted into offer delete table")
         step2cursor.execute(insert_sql)
         step2cursor.close()
         conn.commit();
         step1cursor.execute(select_sql)
         results = step1cursor.fetchall()
         for row in results:
             eslink="http://search-price-data-test-y3hr4n5juhg7atdr6nwr2h2nnm.us-east-1.es.amazonaws.com/product_index/product/"+str(row[1])+"/_update"
             response = requests.post(eslink,data=delete_json)
             print(response)

#-------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    timestamp = int(time.time())
    filenames3=list_s3objects()
    print(filenames3)
    if filenames3 == None:
        logger.info("File not found in s3 Location: "+str(datetime.datetime.today().strftime('%Y-%m-%d'))+'/Amazon/')
        sendmail("Amazon Load Failed : File not found in S3")
    downloadloc="/home/ubuntu/DUMP"
    sourcefile="/home/ubuntu/DUMP/"+str(filenames3)
    destinationfile="/home/ubuntu/DUMP/items_amazon_new.csv"
    updatedcsvfile="/home/ubuntu/DUMP/items_amazon_new_upc.csv"
    staging_table="PRICE_DOT_COM_STAGING.STG_AMAZON_W"
    target_table="PRICE_DOT_COM_TARGET.AMAZON_DAILY"
    retailerno="91"
    asinupctable="PRICE_DOT_COM_STAGING.ASIN_TO_UPC"
    #Jar path Location
    jarpath="/home/ubuntu/Automation/amazonauto/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"

    #s3filename = list_s3objects()
    download_file_s3(downloadloc,filenames3)


    #MySQL RDS Database Connction
    db = MySQLdb.connect("crawler.ccnwaqixfpyj.us-east-1.rds.amazonaws.com","crawler","price123","PRICE_DOT_COM_STAGING" , local_infile = 1 )

    #ASINtoUPC file
    asinupcpath="/home/ubuntu/DUMP/amazon_asinupc_"+str(timestamp)+".csv"
    asinfile=open(asinupcpath, 'w')
    asinfilewrite=csv.writer(asinfile, delimiter=',')


    #Parse Data
    parseAmazonJSON(sourcefile,destinationfile)

    #Get UPC From Mysql
    addUPCCSV(destinationfile,updatedcsvfile,asinfilewrite,db,asinupctable)

    #load data into mysql Staging
    load_mysql_staging(db,staging_table,updatedcsvfile)
    #load data into ASIN to UPC table
    load_mysql_asinupc(db,asinupctable,asinupcpath)
    #Target table Creation
    create_target_table(db,retailerno,staging_table,target_table)
    #Delete Source file from ec2
    delete_local_files(destinationfile)
    #Target table Creation
    delete_local_files(sourcefile)
    #Target table Creation
    delete_local_files(asinupcpath)
    #Delete Offer
    delete_offer(db,target_table,retailerno,'amazon')
    logger.info("ES Push Started")
    db.close()
    subprocess.call(['java', '-jar', '-Xmx2024m', jarpath, 'amazon', target_table, 'amazon'])
    logger.info("ES Push Complete")
    filelog = open(logpath, 'r')
    filecontent = filelog.read()
    mailsubject="Amazon daily load completed"
    url = "http://production12.getpriceapp.com/data/notify/"
    d= {"body":str(filecontent),
    "subject":str(mailsubject)}
    r = requests.post(url,data=d)
    sys.exit(0)

