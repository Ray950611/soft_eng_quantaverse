import pandas as pd
import sys
import zipfile
from collections import defaultdict
from joblib import Parallel, delayed
import multiprocessing
def bridge_detection_by_date(date,rate):
    '''
    Input:
    date - a timestamp variable indicating the date for which all transactions will be examined
    rate - float, the max service fee ratio defined by the user
    Output:
    ML_transactions - list of transactions detected as being a possible bridging transaction during that specific day, each being represented by a list of information.
    '''
    ML_transactions=[]
    data = df[df['TIMESTAMP']==date]
    senders =list(set(data['SENDER']))
    receivers =list(set(data['RECEIVER']))
    intermediates = list(set(senders).intersection(receivers))
    for person in intermediates:
        receive_record = data[data['RECEIVER']==person]
        send_record = data[data['SENDER']==person]
        for index,receive in receive_record.iterrows():
            amt = receive['AMOUNT']
            for index,send in send_record.iterrows():
                if float(amt-send['AMOUNT'])/amt>0 and float(amt-send['AMOUNT'])/amt<=rate and receive['SENDER']!=person and send['RECEIVER']!=person and receive['SENDER']!=send['RECEIVER']:
                    
                    ML_transactions.append([receive['TRANSACTION'],send['TRANSACTION'],date,amt,send['AMOUNT'],receive['SENDER'],person,send['RECEIVER']])
    return ML_transactions
def combine(l1,l2):
    '''
    Input:
    l1,l1 - lists
    Output:
    A list that is the concatenated list of l1 and l2.
    '''
    return l1+l2
def add_record(new,dic):
    '''
    Input:
    new - list, represents a new record of a possible bridging transaction.
    dic - dictionary, the defaultdict created to hold counts for all suspicious entities.
    Output:
    None, only the dic is modified in this function, all entities involved in the new record will have one more count
    to their respective counts of suspicious transactions.
    '''
    A = new[5]
    B = new[6]
    C = new[7]
    dic[A]+=1
    dic[B]+=1
    dic[C]+=1
    return None
#The command line arguments
fee_rate = float(sys.argv[1])
output_transactions = sys.argv[2]
output_entities = sys.argv[3]
#Read the dataset file
zf = zipfile.ZipFile('./data/transactions.zip') 
df = pd.read_csv(zf.open('transactions.csv'),sep='|')
#Select all distinct transaction dates
dates = list(set(df['TIMESTAMP']))
#Used for parallel computing
num_cores = multiprocessing.cpu_count()
m = len(dates)/100
total_records = []
#Parallel computing all possible bridging transactions and append to total_records
for i in range(m+1):
    if i!=m:
        sys.stdout.write('Processing dates '+str(100*i+1)+' to '+str(100*i+100)+'\n')
        #Call bridge_detection_by_date function with multithreads to speed up the process
        results = Parallel(n_jobs=num_cores)(delayed(bridge_detection_by_date)(date,rate=fee_rate) for date in dates[i*100:(i+1)*100])
        result = reduce(combine,results)
        total_records+=result        
    else:
        sys.stdout.write('Processing dates '+str(100*m+1)+' to '+str(len(dates))+'\n')
        results = Parallel(n_jobs=num_cores)(delayed(bridge_detection_by_date)(date,rate=fee_rate) for date in dates[m*100:])
        result = reduce(combine,results)
        total_records+=result
#convert to pandas dataframe and write to output file defined by the user
bridge_transactions = pd.DataFrame(total_records,columns=['Inward Transaction ID','Outward Transaction ID','Timestamp','Amount In','Amount Out','Sender ID','Bridge ID','Receiver ID'])
f=open(output_transactions,'w')
bridge_transactions.to_string(f,index=False)
f.close()
#Hold suspicious entities transaction counts
suspicious = defaultdict(int)
#Call add_record with parallel computing
_=map(lambda x:add_record(x,dic=suspicious),total_records)
suspicious_entity = []
#Reorder axis for sorting
for key in suspicious:    
    suspicious_entity.append([suspicious[key],key])
suspicious_entity = pd.DataFrame(sorted(suspicious_entity,reverse=True),columns=['Count of Suspicious Transactions','Suspicious Entity ID'])
#Write suspicious entities and their transaction counts to user defined output file
g=open(output_entities,'w') 
suspicious_entity.to_string(g,index=False)
g.close()