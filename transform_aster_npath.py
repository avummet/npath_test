#!/usr/bin/python3.5
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 12:40:09 2018

@author: ananth
"""
import psycopg2

def db_connect(sql):
    try:
        conn=psycopg2.connect(host = "localhost",database ="aster",port = 5432,user="postgres",password = "postgres")
    except KeyError as e:
        print(e)
    
    try:
        cur = conn.cursor()
        outresult = "COPY ({0}) TO STDOUT WITH CSV HEADER". format(sql)
        
        with open('/home/ananth/Documents/unused/aster_npath_res.txt','w') as outf:
            cur.copy_expert(outresult,outf)
        conn.close()
    except KeyError as e:
        pass

def main():
    
    sql = """
            WITH clks AS (
                           SELECT unique_visitors,
                             array_agg(distinct (pagename)) as path
                          FROM 
                             aster_npath_1
                                    group by 
                                       unique_visitors
                        ),

          clks_num_gmt AS 
                       (
                        SELECT 
                             unique_visitors,
                             max(visit_num) over (partition by unique_visitors,visit_num,visit_start_time_gmt
                             order by date_time,cast(visit_page_num as integer) desc) as visit_num_cnt,
                             visit_start_time_gmt,
                             row_number() over (partition by unique_visitors,visit_num,visit_start_time_gmt
                             order by date_time,cast(visit_page_num as integer)) as rn
                        from 
                            aster_npath_1),
 
        clks_path AS  (SELECT 
                            unique_visitors,
                            path,
                            count(distinct(A)) as rnk
                       FROM 
                             (SELECT unique_visitors,path,unnest(path) as A FROM clks ) as foo 
                      GROUP BY 
                            unique_visitors,path
                   ),
      
       clks_cnt AS  (SELECT unique_visitors,
                            visit_num_cnt as visit_num,
                            visit_start_time_gmt
                    FROM 
                         clks_num_gmt
                         WHERE rn =1 
                     )

           SELECT A.unique_visitors,path,rnk,B.visit_num,B.visit_start_time_gmt FROM 
                clks_path A
                INNER JOIN clks_cnt B
           ON A.unique_visitors=B.unique_visitors
        """

#Call the datbase caonect function    
    db_connect(sql)
    

if __name__ == "__main__":
    main()
                 
