SELECT T.TRANSACTION_ID 
	, concat('GB', CHECK_DIGITS, SWIFT_CODE, REPLACE(T.SORT_CODE, '-', ''), T.ACCOUNT_NUMBER) AS IBAN
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK02_TRANSACTIONS AS T
JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK02_SWIFT_CODES AS C
	ON T.BANK = C.BANK 
