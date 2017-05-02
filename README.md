# Loan Application Metrics by State

In our use case we are going to use Pig to calculate and find loan application metrics using loan, application and applicant mock data. These mock data are prepared by us with specific criteria in mind and the fields are included in each datasets as per the standard data model used in finance industry in United States (US).

Usually applicants are from different states applied for loan and the overall summary of loan application metrics by state will help industry to find out branches performance by states both in terms of good and bad. We have used interesting calculations like average DTI (Depts. to Income), weighted average interest rate and so on by state.

The following calculations are included in the Pig scripts and which are grouped by states, loan decision type and loan type,

1.	Average DTI (Depts. to Income) of applicants.
2.	Total loan approved amount.
3.	Loan approved count.
4.	Total requested amount by applicants.
5.	Weighted Average interest rate.
6.	Average applicant annual income.
7.	Average approved loan.

Notes: 
Datasets folder for mock data.
Scripts folder for Hive and Pig scripts.