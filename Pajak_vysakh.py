#Aut: Vysakh Chandran. 
#email: vysakh@gmail.com
import os,time,sys
import boto3
import json
import argparse
import requests
from datetime import datetime, timedelta
s3 = boto3.resource('s3')
bucket = s3.Bucket('online-pajak-hr-ops-technical-exercice')
DD_API_KEY = os.environ['DD_API_KEY']

#Date Validation 
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: Provide the date in 'YYYY-MM-DD' format"
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser( description= 'Daily Transaction Insights puller and logger program ')
parser.add_argument("-d","--date",default=datetime.today(),help="Provide the start date in YYYY-MM-DD format", type=valid_date)
parser.add_argument('--log', dest='log', action='store_true', help='send the output to log monitoring tool')
parser.add_argument('--nolog', dest='log', action='store_false', help= 'Print the metrics on standard output for debugging purpose')
parser.set_defaults(log=True)
if len(sys.argv)==1:
    parser.print_help(sys.stderr)
    sys.exit(1)
args = parser.parse_args()
 

def main(date,log):
    today=date.strftime('%Y-%m-%d')
    yesterday = (date - timedelta(days=1)).strftime('%Y-%m-%d')
#Pulling Transaction ids from S3
    today_tids= strip_transactionids(today)
    yesterday_tids= strip_transactionids(yesterday)
#Comparing yesterdays and todays Transaction and identifying new and continued transactions
    new_trans= len(set(today_tids).difference(set(yesterday_tids)))
    cont_trans = len(today_tids) - new_trans
   
# Switch for displaying on screen or to loggin service.
    if not log:
        print(json.dumps({'metric':'business.a_process.transaction_new','value':new_trans,'timestamp':int(time.time())}))
        print(json.dumps({'metric':'business.a_process.transaction_lost','value':cont_trans,'timestamp':int(time.time())}))
    else:
        logging_service('business.a_process.transaction_new',new_trans)
        logging_service('business.a_process.transaction_lost',cont_trans)

#Configurable logging service
def logging_service(metric,trans):
    url = 'https://api.datadoghq.com/api/v1/series'
    headers = {'content-type': 'application/json',
'DD-API-KEY': DD_API_KEY
}
    data = {
  'series': [
    {
      'metric': metric,
      'points': [
        [
          int(time.time()),
          trans
        ]
      ]
    }
  ]
}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    print(r.content)

#stripping trascations from s3. No local save.
def strip_transactionids(day):
    tids=[]
    for obj in bucket.objects.filter(Prefix=day):
        body = json.loads(obj.get()['Body'].read())
        tids.append(body["transaction_id"])
    if not tids: print("Warning:", day, "is not present on s3 bucket,Aborting..");exit(1) 
    return tids


if __name__ == '__main__':
    main(args.date,args.log)