from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import glob
import sqlite3

# function
def clean_df(df):
    # Convert the 'issue_d' column to datetime format
    df['issue_d'] = pd.to_datetime(df['issue_d'], format="%d/%m/%Y")

    # Convert specific columns to category dtype
    cat_column = ['home_ownership', 'income_category', 'term', 'application_type', 
                  'interest_payments', 'loan_condition', 'grade', 'region']
    
    for col in cat_column:
        if col in df.columns:
            df[col] = df[col].astype('category')

    return df

def fetch_clean():
    # pwd
    # /home/{user}/airflow/dags
    database = '/db/loan.db'
    conn = sqlite3.connect(database)
    files = glob.glob("/loan_2014/*.csv")

    df_list = []

    for file in files:
        trx = pd.read_csv(file)
        trx_clean = clean_df(trx)

        last_update = pd.read_sql_query("""
                  SELECT issue_d
                  FROM loan
                  ORDER BY issue_d DESC
                  LIMIT 1
                  """, con = conn)
    
        last_timestamp = last_update['issue_d'].to_string(index=False)

        trx_clean_update = trx_clean[trx_clean['issue_d'] > last_timestamp]

        if trx_clean_update.shape[0] != 0:
            df_list.append(trx_clean_update)
    
    return df_list

def report_generator(df_list):
    for df in df_list:
        # Mengambil periode bulan, untuk nama file
        periode = df['issue_d'].dt.to_period('M').unique()[0].strftime('%Y-%m')

        # Generate a crosstab that counts the number of loans for each grade
        freq_grades = pd.crosstab(index=df['grade'],
                                  columns='Number of Borrowers',
                                  values=df['loan_amount'],  # Using loan_amount just to have a column to count over
                                  aggfunc='count')
        freq_grades_sorted = freq_grades.sort_values(by='Number of Borrowers', ascending=False)

        # Generate a crosstab that sums loan_amount for each loan_condition
        total_loan_conditions = pd.crosstab(index=df['loan_condition'],
                                            columns='Total Loan Amount',
                                            values=df['loan_amount'],
                                            aggfunc='sum')
        total_loan_conditions_sorted = total_loan_conditions.sort_values(by='Total Loan Amount', ascending=False)

        # Generate a crosstab that counts the number of loans by region
        loan_by_region = pd.crosstab(index=df['region'],
                                     columns='Number of Loans',
                                     values=df['loan_amount'],  # Using loan_amount just to have a column to count over
                                     aggfunc='count')
        loan_by_region_sorted = loan_by_region.sort_values(by='Number of Loans', ascending=False)


        # Generate a crosstab that counts the number of loans by income category
        loans_by_income = pd.crosstab(index=df['income_category'],
                                      columns='Number of Loans by Income Category',
                                      values=df['loan_amount'],  # Using loan_amount just to have a column to count over
                                      aggfunc='count')
        loans_by_income_sorted = loans_by_income.sort_values(by='Number of Loans by Income Category', ascending=False)


        # Generate a crosstab that counts the number of loans by combined income categories and grades
        loans_by_income_grade = pd.crosstab(index=[df['income_category'], df['grade']],
                                            columns='Number of Loans by Income Category and Grade',
                                            values=df['loan_amount'],  # Using loan_amount just to have a column to count over
                                            aggfunc='count')
        loans_by_income_grade_sorted = loans_by_income_grade.sort_values(by='Number of Loans by Income Category and Grade', ascending=False)



        # Menyimpan ke dalam file excel
        with pd.ExcelWriter(f'report/{periode}.xlsx') as writer:
            freq_grades_sorted.to_excel(writer, sheet_name='Loan Count by Grade')
            total_loan_conditions_sorted.to_excel(writer, sheet_name='Total Loan Amount by Condition')
            loan_by_region_sorted.to_excel(writer, sheet_name='Loan Count by Region')
            loans_by_income_sorted.to_excel(writer, sheet_name='Loan Count by Income Category')
            loans_by_income_grade_sorted.to_excel(writer, sheet_name='Loan Count by Income Category and Grade')
            print(f"Berhasil membuat report: report/{periode}.xlsx")



def df_to_db(ti):
    # koneksi ke database
    database = '/db/loan.db'
    conn = sqlite3.connect(database)
    
    # mengambil df_list dari task fetch_clean_task
    df_list = ti.xcom_pull(task_ids = 'fetch_clean_task')

    for df in df_list:
        df.to_sql(name = 'loan',
                    con = conn,
                    if_exists = 'append',
                    index = False)
        print("Done Created DB")

# fungsi baru
# def fungsi_baru():

with DAG("dss_dag", # Dag id
         start_date = datetime(2014, 1, 1), # Berjalan mulai 29
        #  schedule_interval = '*/5 * * * *', # setiap 5 menit
        schedule_interval = '@monthly', # setiap bulan
        catchup = False):
    
    fetch_clean_task = PythonOperator(
        task_id = 'fetch_clean_task',
        python_callable = fetch_clean
    )

    # task df_to_db
    df_to_db_task = PythonOperator(
        task_id = 'df_to_db_task',
        python_callable = df_to_db
    )

    # task df_to_db
    report_generator_task = PythonOperator(
        task_id = 'report_generator_task',
        python_callable = report_generator
    )

    # task_baru
    

    # task_a -> task_b -> task_c
    # fetch_clean_task >> df_to_db_task >> report_generator_task
    # task_a -> [task_b, task_c]
    fetch_clean_task >> [df_to_db_task, report_generator_task]

