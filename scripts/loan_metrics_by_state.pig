SET job.name 'Loan Application Metrics By State'
SET tez.task.resource.memory.mb 2048;

-- LOAD LOAN DATA
loan_data = LOAD 'cu_raw_data.loan_data' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- LOAD APPLICATION DATA
application_data = LOAD 'cu_raw_data.application_data' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- LOAD APPLICANT DATA
applicant_data = LOAD 'cu_raw_data.applicant_data' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- FILTER ONLY APPROVED RECORDS FROM LOAN DATA
approved_loan_data = FILTER loan_data BY loan_decision_type == 'Approved'; 

-- FILTER ONLY LOAN APPLICATION FROM APPLICATION DATA
loan_application_data = FILTER application_data BY product == 'loan'; 

-- ITERATE LOAN DATA AND CALCULATE WEIGHT ON LOAN APPROVED AMOUNT
filtered_loan_data = FOREACH approved_loan_data {
  GENERATE application_id,loan_approved_amount,rate,(rate/100 * loan_approved_amount) as weighted,
  loan_decision_type,loan_type,requested_amount;
}

-- ITERATE APPLICATION DATA AND CALCULATE DTI BASED ON APPLICANT MONTHLY INCOME AND DEPTS
filtered_loan_application_data = FOREACH application_data {
  GENERATE application_id,applicant_id,(applicant_debts/applicant_income)*100 as dti,
  applicant_annual_income;
}

-- ITERATE APPLICANT DATA
filtered_loan_applicant_data = FOREACH applicant_data {
  GENERATE applicant_id,state;
}

-- JOIN LOAN, APPLICATION AND APPLICANT DATA
join_application_and_applicant_data = JOIN filtered_loan_application_data BY applicant_id, filtered_loan_applicant_data BY applicant_id;
join_to_find_state = JOIN filtered_loan_data BY application_id, join_application_and_applicant_data BY application_id;

-- GROUP BY STATE, LOAN DECISION TYPE AND LOAN TYPE
loan_data_group_by_state_and_type = GROUP join_to_find_state BY (state,loan_decision_type,loan_type);

-- APPLY AGGREGATE FUNCTION
calculated_data = FOREACH loan_data_group_by_state_and_type {
	GENERATE 
    FLATTEN(group),
    SUM(join_to_find_state.loan_approved_amount) as dv_loan_approved_sum,
    COUNT(join_to_find_state.loan_approved_amount) as dv_loan_approved_count,
    SUM(join_to_find_state.requested_amount) as dv_requested_amount_sum,
    AVG(join_to_find_state.dti) as dv_dti_average,
    SUM(join_to_find_state.weighted) as weighted_sum,
    AVG(join_to_find_state.applicant_annual_income) as dv_applicant_annual_income_average;
}

-- ITERATE AND ADD ADDITIONAL CALCULATIONS
final_calculated_data = FOREACH calculated_data {
	GENERATE loan_decision_type,loan_type,dv_loan_approved_sum,dv_loan_approved_count, 
    (dv_loan_approved_sum/dv_loan_approved_count) as dv_loan_approved_avg,
    dv_requested_amount_sum,dv_dti_average,(weighted_sum/dv_loan_approved_sum) * 100 as weighted_rate,
    dv_applicant_annual_income_average,state;   
}

-- LOAD EMPTY OUTPUT TABLE 
loan_metrics_by_state_data = LOAD 'cu_metrics_data.loan_metrics_by_state' USING org.apache.hive.hcatalog.pig.HCatLoader();
result = UNION loan_metrics_by_state_data,final_calculated_data;

STORE  result INTO 'cu_metrics_data.loan_metrics_by_state'  USING org.apache.hive.hcatalog.pig.HCatStorer(); 