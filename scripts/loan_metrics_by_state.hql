Hive DDL statements:
*********************

Loan Data
*************
CREATE TABLE cu_raw_data.loan_data 
(
application_id  string,
term_months int,
loan_type string,
origination_date date,
loan_decision_type string,
loan_approved_amount double,
requested_amount double,
funded_amount double,
funded_date date,
rate double,
interest_repayable double,
total_amount_payable double,
monthly_emi double,
denial_reason string) 
partitioned by (origination_year int)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

Applicatio Data
***********************
CREATE TABLE cu_raw_data.application_data 
(
application_id  string,
applicant_id string,
application_date date,
application_status string,
branch int,
credit_score int,
product_applied_for string,
applicant_income double,
applicant_debts double,
applicant_annual_income double) 
partitioned by (product string)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

Applicant Data
***********************
CREATE TABLE cu_raw_data.applicant_data 
(
applicant_id string,
firstname string,
lastname string,
address string,
phone string,
city string,
state string,
zip int,
gender string,
age int,
race string,
martial_status string,
employment_status string,
date_of_birth date,
ssn string, 
occupation string) 
partitioned by (applicant_state string)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

Loan Metrics By State
************************
CREATE TABLE cu_metrics_data.loan_metrics_by_state 
(
loan_decision_type string,
loan_type string,
dv_loan_approved_sum double,
dv_loan_approved_count BIGINT,
dv_loan_approved_avg double,
dv_requested_amount_sum double,
dv_dti_average double,
weighted_rate double,
dv_applicant_annual_income_average double) 
partitioned by (p_state string)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");