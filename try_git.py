#%%
# Relevant Modules
import os
import pandas as pd
import datetime
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from envelopes import Envelope
#%%
# Relevant variables
current_date = (datetime.now()).strftime('%Y-%m-%d')
prev_date = (datetime.today() - relativedelta(days=1)).strftime('%Y-%m-%d')
#%%
#define team leaders names, emils and regions
team_leader_ = 'Murungi Alphonsine'

team_leader_emails = 'alphonsine.murungi@engie.com'

Rsm_name = ['Mufata Jean','Albert Urimube']
Rsm_emails = ['paul.mufata@engie.com','albert.urimube@engie.com']
Regions = ['Northern','Eastern']

#%%
Query = ("""
WITH RankedChargedUntil AS (
    SELECT
        fp.customer_id,
        lad.charged_until,
        ROW_NUMBER() OVER (PARTITION BY fp.customer_id ORDER BY lad.snapshot_at DESC) AS RowNum
    FROM
        credit.cf_missed_first_payments fp
        LEFT JOIN credit.loan_accounts_daily lad ON lad.customer_id = fp.customer_id
    WHERE
        fp.country ILIKE 'rw'
        AND lad.snapshot_at = current_date - interval '1 day'
)

select *, 
case when agent_assigned_region in ('Southern-1','Southern-2') then 'southern' 
when agent_assigned_region in ('Northern-1','Northern-2') then 'northern'
when agent_assigned_region in ('Eastern-1','Eastern-2') then 'eastern'
when agent_assigned_region in ('Central-1','Central-2') then 'central' else agent_assigned_region end as region_use

from
(
SELECT
    fp.customer_id,
    cs.customer_name,
    cs.primary_phone_number,
--     cs.other_phone_numbers AS second_phone_number,
    DATE(fp.daily_payment_due_utc_w_grace) AS affecting_date,
    fp.missed_first_daily_payment AS affecting_state,
    CAST(fp.installment_amount AS DECIMAL(10, 2)) AS nstallment_amount,
    cd.area1 AS dsitrict,
    cd.area2 AS Sector,
    fp.product_type,
    cd.application_by_contractor_name AS agent,
        sam.region AS agent_assigned_Region,
    sam.district AS agent_assigned_district,
    sam.sales_agent_id, ml.email 
FROM
    credit.cf_missed_first_payments fp
    LEFT JOIN sensitive.customers_sensitive cs ON cs.customer_id = fp.customer_id
    LEFT JOIN eea.customer_details cd ON cd.customer_id = fp.customer_id
    LEFT JOIN ops.contractors co ON co.contractor_id = cd.application_by_contractor_id
    LEFT JOIN sensitive.phone_decoder pd ON pd.phone_number_encrypted = co.phone_number_encrypted
    LEFT JOIN RankedChargedUntil rcu ON rcu.customer_id = fp.customer_id AND rcu.RowNum = 1
    LEFT JOIN rw.rw_sales_agent_mapping sam ON sam.sales_agent_id = cd.application_by_contractor_id
    LEFT JOIN ug.masterlist_telesales_rw as ml on sam.sales_agent_id = ml.User_id
WHERE
    fp.country ILIKE 'rw'
     AND last_day(fp.daily_payment_due_utc_w_grace) =last_day(current_date)
GROUP BY
    fp.missed_first_monthly_payment,
    fp.monthly_payment_due_utc_w_grace,
    fp.customer_id,
    cs.customer_name,
    cs.primary_phone_number,
    cs.other_phone_numbers,
    fp.daily_payment_due_utc_w_grace,
    fp.source,
    fp.installment_amount,
    cd.area2,
    cd.area3,
    cd.area1,
    fp.product_type,
    cd.application_by_contractor_name,
    sam.region,
    sam.district,
    fp.missed_first_daily_payment, sam.sales_agent_id, ml.email)
    
      where affecting_date >= DATE_TRUNC('month',affecting_date)
      and affecting_date<= DATE_TRUNC('month',affecting_date) + interval '15 days'
""")
#%%
# Email functions for welcome calls list
#starting with the failure email
def sendErrorEmail(reason): 
    envelope = Envelope(
        from_addr=("aws-mail@paygee.com", "Business Strategy"),
        to_addr= ['regan.kyeyune@engie.com'],#,'bwete.joseph@engie.com','yusuf.kiwanuka@engie.com','ernest.ainemukama@engie.com'],#,'joseph.bwete@engie.com','stephen.okuttu@engie.com','yusuf.kiwanuka@engie.com'],
        #cc_addr=  ['alex.niyonsenga@engie.com', 'bwete.joseph@engie.com'] ,
        subject='MFP List (Rwanda)- Failure',
        html_body= """<html lang="en">
        <head>
        </head>
        <body>
        <p>Hi Team,<br /> The MFP list Extraction process has failed due to  <strong>{0}</strong>.</p>
        <p>Regards,<br />Business Strategy</p>
        </body>
        </html>""".format(reason)
           )
# Send the envelope using an ad-hoc connection...
    envelope.send('smtp.gmail.com', login='aws-mail@paygee.com',port = 587,
                    password='tpnsgkwgkiprcndy', tls=True)

#then successful mail function
def successemail(name):
    
   
    envelope = Envelope(
        from_addr=("aws-mail@paygee.com", "Business Strategy"),
        to_addr= 'regan.kyeyune@engie.com',
        # to_addr= ['joseph.bwete@engie.com','babra.achikane@engie.com'],
        # cc_addr=['joseph.bwete@engie.com','philippe.robert@engie.com','patrick.tusingwire@engie.com','solomon.ogwal@engie.com'],
        #cc_addr=['alex.niyonsenga@engie.com','alphonsine.murungi@engie.com','alphonse.batumanyeho@engie.com','anabelle.irakoze@engie.com','regan.kyeyune@engie.com'],#,'joseph.bwete@engie.com'],
        # 'alex.niyonsenga@engie.com','alphonsine.murungi@engie.com','alphonse.batumanyeho@engie.com',

        subject=f"MFP List as at {current_date}",
        html_body= f"""<html lang="en">
        <head>
        </head>
        <body>
        <p>Hello {name},<br />
        Please receive attached MFP list for your action and review.
         </p>
        <p>Regards,<br />Business Strategy</p>
        </body>
        </html>"""
          )
    #envelope.add_attachment(att)

    envelope.send('smtp.gmail.com', login='aws-mail@paygee.com',
                password='tpnsgkwgkiprcndy', tls=True)
    
#%%
# def successemail2(to,name,att):
    
   
#     envelope = Envelope(
#         from_addr=("aws-mail@paygee.com", "Business Strategy"),
#         to_addr= to,
#         cc_addr=['alphonse.batumanyeho@engie.com','anabelle.irakoze@engie.com','regan.kyeyune@engie.com'],#,'joseph.bwete@engie.com'],
#         # 'alex.niyonsenga@engie.com','alphonse.batumanyeho@engie.com',
#         subject=f"MFP List as at {current_date}",
#         html_body= f"""<html lang="en">
#         <head>
#         </head>
#         <body>
#         <p>Hello {name},<br />
#         Please receive attached your region's MFP list for your action and review.
#          </p>
#         <p>Regards,<br />Business Strategy</p>
#         </body>
#         </html>"""
#           )
#     envelope.add_attachment(att)

#     envelope.send('smtp.gmail.com', login='aws-mail@paygee.com',
#                 password='tpnsgkwgkiprcndy', tls=True)
#%%

# results = session.execute(Query)
# data1 = pd.DataFrame(results.fetchall(), columns=results.keys())
# #%%
# data1.to_excel('/github/workspace/test.xlsx')
# #%%
# data2 = '/github/workspace/test.xlsx'

#%%

try:
    
    # data3 = data2[['customer_id','customer_name','primary_phone_number','affecting_date','affecting_state','nstallment_amount',
    #                 'dsitrict','sector','product_type','agent','agent_assigned_region','agent_assigned_district',
    #                 'sales_agent_id']]
    
    # data3 = data3[data3['region_use']=='Telesales']
    # user_id = data3['sales_agent_id'].tolist()
    # names = data3['agent'].tolist()
    # email = data3['email'].tolist()


    # for a,b,c in zip(user_id,email,names):
    #     data4 = data3
    #     data4 = data4[data4['sales_agent_id']==a]
    #     #save the dataframe in an excel file
    #     data4.to_excel(f'/Users/regan.kyeyune/Python_Projects/Rwanda Lists/Welcome call list excel/{c}.xlsx',index=False)
    #     att1 = f'/Users/regan.kyeyune/Python_Projects/Rwanda Lists/Welcome call list excel/{c}.xlsx'

    c = 'regan'
    successemail(name=c)
    
    print(f"MFP List sent to {c}")

except Exception as e:
    print(e)
    sendErrorEmail(e)

#%%
#send call list
# try:
#     #create a pandas dataframe
#     for a,b,c in zip(Regions, Rsm_name, Rsm_emails):
#         data1 = data2
#         data3 = data1[data1['region_use']==a]
#         data4 = data3[['customer_id','customer_name','primary_phone_number','affecting_date','affecting_state','nstallment_amount',
#                     'dsitrict','sector','product_type','agent','agent_assigned_region','agent_assigned_district',
#                     'sales_agent_id']]
#         data4.to_excel(f'/Users/regan.kyeyune/Python_Projects/Rwanda Lists/Welcome call list excel/{a} MFP List.xlsx',index=False)
#         att1 = f'/Users/regan.kyeyune/Python_Projects/Rwanda Lists/Welcome call list excel/{a} MFP List.xlsx'
#         successemail(name = b, att = att1)   
#         print(f"MFP List Sent to {b}")

# except Exception as e:
#     print(e)
#     sendErrorEmail(e)
# # %%
