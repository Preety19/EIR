from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import calendar
import datetime
from datetime import datetime as dt
import warnings
import glob
import swifter
warnings.filterwarnings("ignore")

today = str(dt.now().date())
conf = SparkConf()
conf.setAppName('EIR')
conf.setMaster('spark://CRSPL-L-2207117.kalyptorisk.com:7077')



def DataValidation(DATA):
    B = DATA["CashFlow.Date"].tolist()
    RA = DATA["Repayment.Amount"].tolist()
    CI = DATA["Contractual.Interest.Rate"].tolist()
    First_date = min(B)
    if (DATA.loc[DATA["CashFlow.Date"] == First_date, "Disbursment.Amount"] >= 0) or (DATA["Disbursment.Amount"].sum() + DATA["Fee.Charge"].sum() >= 0):
        raise Exception("Disbursement Amount including processing fee cannot be nigative")  
    if all(RA == 0):
        raise Exception("Repayment Amount seems to be incorrect")    
    if all(CI <= 0 or CI >= 100):
        raise Exception("Interest should be between 0 and 100")  
    else:
        return DATA

def DateSeq(start_date,end_date,delta):
    return_dict={}
    date_list = list(pd.date_range(start_date, end_date, freq=delta))
    filler = "Cashflow.Date"
    for i in date_list:
        return_dict[str(i.date())] = filler
    return return_dict  

def Month_end(df):
    df['Month_End_Dates'] = df['CashFlow.Date'].swifter.apply(lambda x: x.to_period('M').to_timestamp('M'))
    df['Month_End_Dates'] = df['Month_End_Dates'].astype(str)
    return df    

def date_preprocess(OrignalDF):
    OrignalDF['CashFlow.Date'] = OrignalDF['CashFlow.Date'].dt.date

    OrignalDF['Month_End_Dates'] = pd.to_datetime(OrignalDF['Month_End_Dates'])

    OrignalDF['Month_End_Dates'] = OrignalDF['Month_End_Dates'].dt.date

    OrignalDF['CashFlow.Date'].fillna(value=OrignalDF['Month_End_Dates'],inplace=True) 
    OrignalDF['Date_Type'].fillna('Cashflow.Date',inplace=True) 
    
    OrignalDF = OrignalDF.sort_values(by='CashFlow.Date')    
    OrignalDF.iloc[:,2].fillna(value=0,inplace=True)
    OrignalDF.iloc[:,3].fillna(value=0,inplace=True)
    OrignalDF.iloc[:,0].fillna(value=OrignalDF.iloc[-1,0],inplace=True)
    OrignalDF.iloc[:,8].fillna(value=OrignalDF.iloc[-1,8],inplace=True)
    OrignalDF.iloc[:,9].fillna(value=OrignalDF.iloc[-1,9],inplace=True)   
    return OrignalDF

def EIR(INT_RTE, Total_Repayment, Principle_disbu, Principle_prepay, Proces_fee, Day_count_Basis, No_of_days, retu_type):
    P = [0] * len(Principle_disbu)
    A = [0] * len(Principle_disbu)
    E = [0] * len(Principle_disbu)

    P[0] = (-1 * Principle_disbu[0]) - Proces_fee[0]
    E[0] = (P[0] * INT_RTE) / Day_count_Basis[0] * (No_of_days[0])
    A[0] = E[0]

    for i in range(1, len(Principle_disbu)):
        if Total_Repayment[i] == 0:
            P[i] = P[i-1] - Proces_fee[i] - Principle_disbu[i] - Principle_prepay[i]
            E[i] = ((P[i-1]) * INT_RTE) / Day_count_Basis[i] * (No_of_days[i])
            A[i] = A[i-1] + E[i]
        else:
            E[i] = ((P[i-1]) * INT_RTE) / Day_count_Basis[i] * No_of_days[i]
            A[i] = 0
            P[i] = P[i-1] - (Proces_fee[i] + Principle_disbu[i] + Principle_prepay[i]) - (Total_Repayment[i] - A[i-1] - E[i])
    if retu_type == 1:
        return pd.DataFrame({"P": P, "E": E, "A": A})
    else:
        return P[-1]

def adjusted_intrest(INT_RTE, Total_Repayment, Principle_disbu, Principle_prepay, Proces_fee, Day_count_Basis, No_of_days, retu_type):
    int_r = INT_RTE # 0.18
    retr = EIR(INT_RTE,Total_Repayment,Principle_disbu,Principle_prepay,Proces_fee,Day_count_Basis
                              ,No_of_days,retu_type)
    X = 0
    M = 0
    for i in range (3,15):
        while retr < 0:
            M = int_r
            int_r  =  int_r + X/(10**i)
            retr = EIR(int_r,Total_Repayment,Principle_disbu,Principle_prepay,Proces_fee,Day_count_Basis
                              ,No_of_days,retu_type)
            X += 1

        int_r = M + (X-1)/(10**i)
        retr = EIR(int_r,Total_Repayment,Principle_disbu,Principle_prepay,Proces_fee,Day_count_Basis
                              ,No_of_days,retu_type)
        #print(f'Last retr value is {retr}')
    return (int_r)

def df_preprocessing(df):
    
    df.columns = df.columns.str.replace('[#,@,&,\t,\n,\0,\,/]', '')

    new_cols = ['ACCOUNT_NUMBER', 'CASH_FLOW_DATE','PRINCIPAL_AMOUNT', 'INTEREST_AMOUNT', 'REPAYMENT_AMOUNT','DISBURSED_AMOUNT', 'PARTIAL_AMOUNT', 'FEE_CHARGE','CONTRACTUAL_INTEREST_RATE', 'DAY_COUNT_BASIS', 'CURRENCY_ID', 'ASSESSMENT_DATE']
    df = df.reindex(columns = new_cols)
    df['DAY_COUNT_BASIS'].astype(float)
        
    df.columns = ["Loan.Account", "CashFlow.Date", "Principal.Amount", "Interest.Amount", "Repayment.Amount", "Disbursment.Amount", "Partial.Full.Paid.off", "Fee.Charge", "Contractual.Interest.Rate", "Day_count_Basis", "Currency.ID","Assessment_date"]

    Acc_no = df["Loan.Account"].unique()

    Date_array = DateSeq(df['CashFlow.Date'].min(),df['CashFlow.Date'].max(),'M')

    df = Month_end(df)

    df['Date_Type'] = df['Month_End_Dates'].map(Date_array)

    df = date_preprocess(df)
    return df, Acc_no

def calculation(df, unique_acc):
    account_eir = []
    for m in range(len(unique_acc)):

        CBS_subset = df[df['Loan.Account'] == unique_acc[m]]
        CBS_subset = CBS_subset.reset_index(drop = True)
        intrest_rate = CBS_subset.loc[0, 'Contractual.Interest.Rate'] / 100
        # Principle 

        CBS_subset['Principal'] = abs(CBS_subset['Disbursment.Amount'])
        CBS_subset['Principal'].fillna(0)

        CBS_subset = CBS_subset.reset_index(drop = True)

        subset_shape = CBS_subset.shape[0]                ####Row count of subset date 

        for i in range(1, subset_shape):
            if CBS_subset['Repayment.Amount'][i] == 0:
                CBS_subset['Principal'][i] = CBS_subset['Principal'][i] + CBS_subset['Principal'][i-1] - CBS_subset['Partial.Full.Paid.off'][i]
            else:
                CBS_subset['Principal'][i] = CBS_subset['Principal'][i] + CBS_subset['Principal'][i-1] - CBS_subset['Partial.Full.Paid.off'][i] - CBS_subset['Principal.Amount'][i]

        P1 = pd.melt(CBS_subset.loc[:, ['CashFlow.Date', 'Month_End_Dates']], value_name='Date_Type', id_vars=None)
        P1 = P1.reset_index(drop = True)
        P1.columns = ['Date_Type','Date']

        P1['Month'] = P1['Date'].dt.strftime('%m %Y M')
        P1 = P1.drop_duplicates()

        P1 = P1.sort_values(['Date','Date_Type'],ascending=[True,False])
        P1 = P1.groupby('Month').apply(lambda x: x.assign(rank=x.reset_index().index+1))

        P1_1 = P1.loc[P1['Date_Type'] != 'Month_End_Dates',:]
        CBS_RAW_cols = CBS_subset.iloc[:,[1,9,12,13,4,5,6,7,8]]
        CBS_RAW_cols.columns = ['Date', 'Day_count_Basis', 'Date_Type_CBS', 'Principal', 'Repayment.Amount', 'Disbursment.Amount', 'Partial.Full.Paid.off', 'Fee.Charge', 'Contractual.Interest.Rate']

        P1_2 = pd.merge(P1_1, CBS_RAW_cols, on='Date', how='left')

        P1_3 = P1.loc[P1['Date_Type'] == 'Month_End_Dates',:]

        P11 = pd.concat([P1_2, P1_3], axis=0).reset_index(drop=True)

        P11 = P11.sort_values(by='Date')    


        P11 = P11.reset_index(drop = True)

        P11.iloc[:, [4,6]] = P11.iloc[:, [4,6]].fillna(method='pad')

        P11['Date_Type_CBS'].fillna(value = 'Month_End_Dates', inplace = True)

        P11.loc[(P11['Date_Type_CBS'] == 'Date_Filler') & (P11['Date_Type'] == 'Month_End_Dates'), 'Date_Type_CBS'] = 'Month_End_Dates'
        P11.loc[(P11['Date_Type_CBS'] == 'CashFlow.Date') & (P11['Date_Type'] == 'Month_End_Dates'), 'Date_Type_CBS'] = 'Month_End_Dates'

        P11.fillna(0, inplace = True) 
        P11 = P11.iloc[: , 1:]    
        P11_rows = P11.shape[0]
        P11['Period'] = np.nan   

        for i in range(1, P11_rows):
            
            if P11['Month'][i] == P11['Month'][i-1]:
                if P11['Date_Type_CBS'][i-1] != 'Date_Filler':
                    if P11['Date_Type_CBS'][i] == P11['Date_Type_CBS'][i-1]:
                        P11['Period'][i] = P11['Date'][i] - P11['Date'][i-1]
                    else:
                        P11['Period'][i] = P11['Date'][i] - P11['Date'][i-1] + datetime.timedelta(days=1)
                else: 
                    P11['Period'][i] = P1['Date'][i] - P11['Date'][i-1]          
            else:

                if P11['Date_Type_CBS'][i] == 'Date_Filler':
                    P11['Period'][i] = P11['Date'][i] - P11['Date'][i-1]         
                else:
                    P11['Period'][i] = P11['Date'][i] - P11['Date'][i-1] - datetime.timedelta(days=1)

        P11['Period'].fillna(0, inplace = True)

        P11['Period'] = pd.to_timedelta(P11['Period'])
        P11['Period_Days'] = P11['Period'].dt.days

        for i in range(1, P11_rows):
            if P11['Date_Type_CBS'][i] == "Date_Filler":
                P11['Period'][i] = P11['Period'][i-1]

        P11['Expense'] = np.nan
        P11['Expense'][0] = (P11['Principal'][0] * intrest_rate) / P11['Day_count_Basis'][0] * P11['Period_Days'][0]
        for i in range(1, P11_rows):
            P11['Expense'][i] = (P11['Principal'][i-1] * intrest_rate) / P11['Day_count_Basis'][i] * P11['Period_Days'][i]



        revised_intrest_rate = adjusted_intrest(INT_RTE = intrest_rate ,Total_Repayment = P11['Repayment.Amount'],Principle_disbu = P11['Disbursment.Amount']
                                        ,Principle_prepay = P11['Partial.Full.Paid.off'],Proces_fee = P11['Fee.Charge'],
                                         Day_count_Basis = P11['Day_count_Basis']
                                        ,No_of_days = P11['Period_Days'],retu_type = 4)
        print(revised_intrest_rate)

        returns = EIR(INT_RTE = revised_intrest_rate ,Total_Repayment = P11['Repayment.Amount'],Principle_disbu = P11['Disbursment.Amount']
                                        ,Principle_prepay = P11['Partial.Full.Paid.off'],Proces_fee = P11['Fee.Charge'],
                                         Day_count_Basis = P11['Day_count_Basis']
                                        ,No_of_days = P11['Period_Days'],retu_type = 1)

        P11['EIR_Principal']=returns['P']
        P11['EIR_Expense']=returns['E']
        P11['EIR_AIR']=returns['A']

        Y = P11.groupby(['Month'])['rank'].max().reset_index()

        distinct_months = list(Y['Month'].unique())
        distinct_rank = list(Y['rank'])

        dates = []
        period1_l = []
        period2_l = []
        period3_l = []
        period4_l = []
        period5_l = []
        period6_l = []
        expense_l = []
        expense_eir_l = []
        air_l = []
        air_eir_l = []

        for i in range(len(distinct_months)): 
            
            filter = P11[P11['Month'] == distinct_months[i]]
            date =  filter.loc[filter['Date_Type_CBS'].isin(['Cashflow.Date', 'Date_Filler']), 'Date'].max()
            period1 =  filter.loc[filter['rank'].isin([1]), 'Period_Days'].max()
            period2 =  filter.loc[filter['rank'].isin([2]), 'Period_Days'].max()
            period3 =  filter.loc[filter['rank'].isin([3]), 'Period_Days'].max()
            period4 =  filter.loc[filter['rank'].isin([4]), 'Period_Days'].max()
            period5 =  filter.loc[filter['rank'].isin([5]), 'Period_Days'].max()
            period6 =  filter.loc[filter['rank'].isin([6]), 'Period_Days'].max()
            expense =  filter['Expense'].sum()
            eir =  filter['EIR_Expense'].sum()
            air =  filter.loc[~filter['rank'].isin([distinct_rank[0]]) & filter['Repayment.Amount'].isin([0]),'Expense'].sum()   
            air_eir = filter.loc[~filter['rank'].isin([distinct_rank[0]]) & filter['Repayment.Amount'].isin([0]),'EIR_Expense'].sum()   

            dates.append(date)
            period1_l.append(period1)
            period2_l.append(period2)
            period3_l.append(period3)
            period4_l.append(period4)
            period5_l.append(period5)
            period6_l.append(period6)
            expense_l.append(expense)
            expense_eir_l.append(eir)
            air_l.append(air)
            air_eir_l.append(air_eir)


        Y['Cash_Flow_Date'] = dates
        Y['Period 1'] = period1_l
        Y['Period 2'] = period2_l
        Y['Period 3'] = period3_l
        Y['Period 4'] = period4_l
        Y['Period 5'] = period5_l
        Y['Period 6'] = period6_l
        Y['Y_Expense'] = expense_l
        Y['Y_Expense_EIR'] = expense_eir_l
        Y['Y_AIR'] = air_l
        Y['Y_AIR_EIR'] = air_eir_l

        temp_df = pd.merge(CBS_subset,Y,how='left',left_on='CashFlow.Date',right_on='Cash_Flow_Date')

        P11.reset_index(drop=True,inplace=True)

        P11_subset = P11[P11['Date_Type_CBS'].isin(['Cashflow.Date','Date_Filter'])]

        final_df = pd.merge(P11_subset[['Date','EIR_Principal']],temp_df,how='left',left_on='Date',right_on='CashFlow.Date')

        final_df['Intrest'] = final_df['Y_Expense_EIR'] - final_df['Y_AIR_EIR']

        final_df['Intrest'].fillna(value=0,inplace=True)

        final_df['Intrest_Amount'] = 0

        for i in range(1,len(final_df)):
            final_df['Intrest_Amount'][i] = final_df['Intrest'][i] + final_df['Y_AIR_EIR'][i]
            if final_df['Date_Type'][i] == "Date_Filler":
                final_df['Y_AIR'][i] = final_df['Y_Expense'][i] + final_df['Y_AIR'][i-1]
                final_df['Y_AIR_EIR'][i] = final_df['Y_Expense_EIR'][i] + final_df['Y_AIR_EIR'][i-1]
            else:
                final_df['Y_AIR'][i] = final_df['Y_AIR'][i] 
                final_df['Y_AIR_EIR'][i] = final_df['Y_AIR_EIR'][i] 
        final_df['Principal Installment'] = final_df['Repayment.Amount'] - final_df['Intrest_Amount']

        for i in range(len(final_df)):
            
            if final_df['Repayment.Amount'][i] == 0:
                final_df['Principal Installment'][i] = 0
                final_df['Intrest_Amount'][i] = 0

        final_df['Effective_Rate'] = revised_intrest_rate

        final_df['LoanAccount'] = CBS_subset['Loan.Account']
        account_eir.append(final_df)
    return account_eir

if __name__ == '__main__':
    
    spark = SparkSession.builder.config(conf=conf).config("spark.jars","/home/preety/jars/ojdbc8.jar").getOrCreate()
    jdbcDF = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@172.26.100.12:1521/offsite").option("dbtable", f"sathapana_ifrs_dev.KRM_TX_REPAYMENT_CASHFLOW").option("user", "sathapana_ifrs_dev").option("password", "sathapana").option("fetchsize","5000").option("Driver","oracle.jdbc.driver.OracleDriver").load()
    
    jdbcDF.write.parquet(f'account_{today}')
    
    
    df = pd.read_parquet(glob.glob(f'account_{today}/*.parquet'), engine='fastparquet')
    df_result, unique_acc = df_preprocessing(df)
    final_df = calculation(df_result, unique_acc)
    print(final_df)
    