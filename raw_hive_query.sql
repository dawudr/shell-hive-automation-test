Select N.ic_key,N.sty_rcnum,N.lms_party_account_no,
N.lms_status_code,N.lms_customer_create_date 
from raw.nectar_dat_initial1 N
order by N.ic_key,N.lms_status_code
