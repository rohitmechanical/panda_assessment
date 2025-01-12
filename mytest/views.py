from django.shortcuts import render
from django.http import JsonResponse
from rest_framework.views import APIView
import mysql.connector,pandas
from sqlalchemy import create_engine
import sqlite3,json
from rest_framework_simplejwt.authentication import JWTTokenUserAuthentication
from rest_framework.permissions import IsAuthenticated

# Create your views here.



def slab(value):
  if value >80:
    return 'high'
  elif value >60 and value < 80:
    return 'medium'
  elif value > 40 and value < 60:
    return 'low'
  else:
    return 'below 40'


class pandas_tosql(APIView):
    authentication_classes=[JWTTokenUserAuthentication]
    permission_classes=[IsAuthenticated]
    def post(self,request):
        # if i want to use mysql 

        # create connection to mysql
        # conn=mysql.connector.connect(
        #     database='test',
        #     user='root',
        #     password='',
        #     host='localhost')

        # if want to use db.sqlite3
        try:
            conn=sqlite3.connect('db.sqlite3')
            print("sql connection established successfully")
        except Exception as e:
            print(f"error :{str(e)}")
        raw_data=pandas.read_csv('mytest/dataa.csv')
        table=raw_data[0:35]
        table['Date'] = pandas.to_datetime(table['Date'], format='%d-%m-%Y')
        table['Inward']=table['Inward'].astype('int32')
        table['Accepted']=table['Accepted'].astype('int32')
        table['Avoidable Return']=table['Avoidable Return'].astype('int32')
        table['Month']=table['Date'].dt.month 
        table['Year']=table['Date'].dt.year
        month_grp=table.groupby('Month').agg(
            Total_Bank_Counts=('Code','nunique'),
            Total_Inward=('Inward','sum'),
            Total_Accepted=('Accepted','sum'),
            Total_Avoidable_Return=('Avoidable Return','sum')
        )
        year_grp=table.groupby('Year').agg(
            Total_Bank_Counts=('Code','count'),
            Total_Inward=('Inward','sum'),
            Total_Acceptance=('Avoidable Return','sum'),
            Total_Avoidable_returns=('Avoidable Return','sum')
        )
        year_grp

        month_grp.to_sql(name='monthgrp',con=conn,schema='db.sqlite3',if_exists='replace',index=False)
        year_grp.to_sql(name='yeargrp',con=conn,schema='db.sqlite3',if_exists='replace',index=False)
        conn.close()  # Close the connection
        return JsonResponse({'status':"success",'message':f"data has been saved with table name monthgrp and yeargrp, you can get all data from get request"},status=200)

    def get(self,request):
        query='select * from monthgrp'
        try:
            conn=sqlite3.connect('db.sqlite3')
            print("connect established successfully")
        except Exception as e:
            print("error",str(e))  # Close the connection
        get_data=pandas.read_sql_query(query,con=conn)
        inJson=get_data.to_dict(orient='records')

        # new dataframe from sample.csv

        # Load data
        smple = pandas.read_csv('mytest/sample.csv')

        # Apply slab function to categorize the 'slab' column based on 'Approved %'
        smple['slab'] = smple['Approved %'].apply(slab)

        # Create pivot table
        pivot_table = pandas.pivot_table(
            smple,
            index="Code",
            columns="slab",
            aggfunc="size",
            fill_value=0
        ).reset_index()


        # Convert the pivot table to JSON format
        pivot_table_json = pivot_table.to_dict(orient="records")
        conn.close()
        return JsonResponse({"status":"success","data":inJson,"slab_summary":pivot_table_json},status=200)