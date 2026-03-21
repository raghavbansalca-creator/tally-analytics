#!/usr/bin/env python3
"""
Test ALL [C] tagged questions from the Tally Question Bank against classify_intent().
Reports PASS/FAIL/SKIP for each question.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from chat_engine import classify_intent

# ── Define all [C] questions with expected actions ──────────────────────────
# Format: (question_number, section, question_text, expected_action, acceptable_actions)
# acceptable_actions is a set of action values that count as PASS
# "SKIP" in acceptable means we skip (sql_query/complex not testable with keyword classifier)

QUESTIONS = [
    # SECTION 1.1: Profit & Loss
    (1, "1.1 P&L", "Show me the P&L", {"report_pl"}),
    (2, "1.1 P&L", "What is the net profit?", {"report_pl"}),
    (3, "1.1 P&L", "Show profit and loss for April 2025 to September 2025", {"report_pl"}),
    (4, "1.1 P&L", "P&L for Q1 FY 2025-26", {"report_pl"}),
    (5, "1.1 P&L", "What is the gross profit margin?", {"report_pl"}),
    (6, "1.1 P&L", "Compare this quarter's P&L with last quarter", {"report_pl"}),
    (7, "1.1 P&L", "What are my top 5 expenses?", {"pl_drilldown", "report_pl"}),
    (8, "1.1 P&L", "What is my EBITDA?", {"report_pl"}),
    (9, "1.1 P&L", "Show me operating profit vs non-operating income", {"report_pl"}),
    (10, "1.1 P&L", "Month-on-month revenue trend for this FY", {"report_pl"}),
    (11, "1.1 P&L", "What percentage of revenue is spent on salaries?", {"report_pl"}),
    (12, "1.1 P&L", "Show me the P&L in horizontal format (comparative)", {"report_pl"}),
    (15, "1.1 P&L", "Which expense head grew the most YoY?", {"report_pl"}),

    # SECTION 1.2: Balance Sheet
    (16, "1.2 BS", "Show me the balance sheet", {"report_bs"}),
    (17, "1.2 BS", "Balance sheet as on 30th September 2025", {"report_bs"}),
    (18, "1.2 BS", "What is the total capital?", {"report_bs", "pl_drilldown"}),
    (19, "1.2 BS", "What is the current ratio?", {"report_bs"}),
    (20, "1.2 BS", "What is the debt-equity ratio?", {"report_bs"}),
    (21, "1.2 BS", "Show me the working capital position", {"report_bs"}),
    (22, "1.2 BS", "What are my fixed assets?", {"report_bs"}),
    (23, "1.2 BS", "How much cash and bank balance do we have?", {"report_bs"}),
    (24, "1.2 BS", "What is the net worth of the company?", {"report_bs"}),
    (26, "1.2 BS", "What is the return on capital employed?", {"report_bs", "report_pl"}),

    # SECTION 1.3: Trial Balance
    (29, "1.3 TB", "Show me the trial balance", {"report_tb"}),
    (30, "1.3 TB", "Trial balance as on 31st December 2025", {"report_tb"}),
    (31, "1.3 TB", "Does the trial balance tally?", {"report_tb"}),
    (32, "1.3 TB", "Which ledgers have the highest debit balance?", {"report_tb"}),
    (33, "1.3 TB", "Show me only ledgers with balance above 1 lakh", {"report_tb"}),
    (34, "1.3 TB", "Show me group-wise trial balance summary", {"report_tb"}),
    (35, "1.3 TB", "How many ledgers have zero balance?", {"SKIP"}),  # sql_query

    # SECTION 1.4: Cash Flow (only Q40 is [C])
    (40, "1.4 Cash Flow", "Show me cash inflows vs outflows for this month", {"voucher_summary", "report_pl", "SKIP"}),

    # SECTION 2.1: Ledger Statements
    (41, "2.1 Ledger", "Show me the ledger of HDFC Bank", {"ledger_detail"}),
    (42, "2.1 Ledger", "Statement of account for Reliance Industries", {"ledger_detail"}),
    (43, "2.1 Ledger", "What is the closing balance of Petty Cash?", {"ledger_detail"}),
    (44, "2.1 Ledger", "Show all transactions in Salary account", {"ledger_detail"}),
    (45, "2.1 Ledger", "Ledger of Rent for April 2025 only", {"ledger_detail"}),
    (46, "2.1 Ledger", "Show me the opening balance of all bank accounts", {"SKIP"}),  # sql_query
    (47, "2.1 Ledger", "Which ledger has the highest closing balance?", {"SKIP"}),  # sql_query
    (48, "2.1 Ledger", "Show me all ledgers under Indirect Expenses", {"search", "SKIP", "pl_drilldown"}),
    (49, "2.1 Ledger", "What is the balance of all Duties & Taxes ledgers?", {"SKIP"}),  # sql_query

    # SECTION 2.2: Ledger Search & Discovery
    (51, "2.2 Search", "Search for ledger pharma", {"search"}),
    (52, "2.2 Search", "Which group does the ledger Professional Fees belong to?", {"search"}),
    (53, "2.2 Search", "How many ledgers are under Sundry Debtors?", {"SKIP"}),  # sql_query
    (54, "2.2 Search", "List all bank accounts", {"SKIP"}),  # sql_query
    (55, "2.2 Search", "Which ledgers have PAN recorded?", {"SKIP"}),  # sql_query
    (56, "2.2 Search", "Which ledgers have GSTIN?", {"SKIP"}),  # sql_query
    (57, "2.2 Search", "Show me all ledgers without a parent group", {"SKIP"}),  # sql_query
    (58, "2.2 Search", "Find all ledgers with transport in the name", {"search"}),
    (59, "2.2 Search", "How many ledgers are there in total?", {"SKIP"}),  # sql_query
    (60, "2.2 Search", "List all cost centres", {"SKIP"}),  # sql_query

    # SECTION 3.1: Debtors
    (61, "3.1 Debtors", "Show me all outstanding debtors", {"debtors"}),
    (62, "3.1 Debtors", "Who owes us the most money?", {"debtors"}),
    (63, "3.1 Debtors", "Total receivables outstanding?", {"debtors"}),
    (64, "3.1 Debtors", "How many debtors have outstanding above 1 lakh?", {"debtors"}),
    (69, "3.1 Debtors", "Show me the receivable turnover ratio", {"debtors", "report_pl"}),
    (70, "3.1 Debtors", "List debtors from Maharashtra", {"SKIP"}),  # sql_query
    (71, "3.1 Debtors", "Which debtors have exceeded their credit limit?", {"debtors", "SKIP"}),
    (72, "3.1 Debtors", "Show me the top 10 debtors by balance", {"debtors"}),
    (73, "3.1 Debtors", "What percentage of total sales is outstanding?", {"debtors", "report_pl"}),
    (74, "3.1 Debtors", "Show me debtors with no transactions in last 6 months", {"SKIP"}),  # sql_query
    (75, "3.1 Debtors", "Debtor concentration — top 5 debtors as % of total", {"debtors"}),

    # SECTION 3.2: Creditors
    (76, "3.2 Creditors", "Show me all outstanding creditors", {"creditors"}),
    (77, "3.2 Creditors", "How much do we owe in total?", {"creditors"}),
    (78, "3.2 Creditors", "Who are our top 5 creditors?", {"creditors"}),
    (82, "3.2 Creditors", "Show me payable turnover ratio", {"creditors", "report_pl"}),
    (83, "3.2 Creditors", "List all creditors from Delhi", {"SKIP"}),  # sql_query
    (84, "3.2 Creditors", "Which creditor has the largest unpaid balance?", {"creditors"}),

    # SECTION 4.2: GST Ledger Queries
    (101, "4.2 GST Ledger", "Show me the CGST ledger", {"ledger_detail", "search"}),
    (102, "4.2 GST Ledger", "Show me the SGST ledger", {"ledger_detail", "search"}),
    (103, "4.2 GST Ledger", "Show me the IGST ledger", {"ledger_detail", "search"}),
    (104, "4.2 GST Ledger", "What is the balance in Input CGST?", {"ledger_detail", "search"}),
    (105, "4.2 GST Ledger", "Total tax collected this month", {"SKIP"}),  # multi-ledger sum
    (106, "4.2 GST Ledger", "Show all Duties & Taxes ledgers with balances", {"SKIP"}),  # sql_query
    (107, "4.2 GST Ledger", "Net GST payable (output - input)", {"SKIP"}),  # multi-ledger calc
    (108, "4.2 GST Ledger", "Show me all tax ledger movements", {"SKIP"}),  # multi-ledger

    # SECTION 5: TDS
    (114, "5 TDS", "Total TDS payable as on date", {"ledger_detail", "search", "SKIP"}),
    (117, "5 TDS", "Show advance tax paid", {"ledger_detail", "search"}),

    # SECTION 6.1: Voucher Queries
    (119, "6.1 Voucher", "Voucher summary by type", {"voucher_summary"}),
    (120, "6.1 Voucher", "How many sales invoices this month?", {"voucher_summary"}),
    (121, "6.1 Voucher", "Show me all journal entries", {"SKIP"}),  # sql_query
    (122, "6.1 Voucher", "Show me all payment vouchers above 50000", {"SKIP"}),  # sql_query
    (123, "6.1 Voucher", "List all contra entries", {"SKIP"}),  # sql_query
    (124, "6.1 Voucher", "Show me voucher number gaps in sales invoices", {"SKIP"}),  # sql_query
    (125, "6.1 Voucher", "Which voucher types are used the most?", {"voucher_summary"}),
    (126, "6.1 Voucher", "Show me all credit notes this FY", {"SKIP"}),  # sql_query
    (127, "6.1 Voucher", "Show me all debit notes", {"SKIP"}),  # sql_query
    (128, "6.1 Voucher", "Show me the day book for 15th October 2025", {"SKIP"}),  # sql_query
    (129, "6.1 Voucher", "Total value of all receipts this month", {"SKIP"}),  # sql_query
    (130, "6.1 Voucher", "How many vouchers were posted today?", {"voucher_summary", "SKIP"}),

    # SECTION 6.2: Transaction Pattern Analysis
    (131, "6.2 Txn Pattern", "Show me transactions above 10 lakh", {"SKIP"}),
    (132, "6.2 Txn Pattern", "Which party has the most transactions?", {"SKIP"}),
    (133, "6.2 Txn Pattern", "Show me all cash transactions above 2 lakh", {"SKIP"}),
    (134, "6.2 Txn Pattern", "Average transaction value by voucher type", {"SKIP"}),
    (135, "6.2 Txn Pattern", "Show me all round-figure transactions (exact thousands/lakhs)", {"SKIP"}),
    (136, "6.2 Txn Pattern", "Transactions posted on Sundays or holidays", {"SKIP"}),
    (137, "6.2 Txn Pattern", "Show me all transactions with narration containing adjustment", {"SKIP"}),
    (138, "6.2 Txn Pattern", "Show me same-amount same-party transactions (potential duplicates)", {"SKIP"}),
    (139, "6.2 Txn Pattern", "Benford's Law analysis on first digits", {"SKIP"}),
    (140, "6.2 Txn Pattern", "Show me all transactions with blank narration", {"SKIP"}),

    # SECTION 7: Bank Reconciliation
    (142, "7 Bank Recon", "What is the bank balance per books?", {"ledger_detail"}),
    (143, "7 Bank Recon", "Show me all cheques issued but not cleared", {"SKIP"}),  # trn_bank
    (144, "7 Bank Recon", "Show me all cheques deposited but not collected", {"SKIP"}),  # trn_bank
    (146, "7 Bank Recon", "Show me instrument-wise bank details", {"SKIP"}),  # trn_bank
    (149, "7 Bank Recon", "Bank-wise balance summary", {"SKIP"}),  # multi-ledger
    (150, "7 Bank Recon", "Show me all NEFT/RTGS transactions", {"SKIP"}),  # trn_bank

    # SECTION 8.1: Stock Queries
    (151, "8.1 Stock", "How many stock items do we have?", {"SKIP"}),
    (152, "8.1 Stock", "List all stock items", {"SKIP"}),
    (153, "8.1 Stock", "Show me stock items under group Tablets", {"SKIP"}),
    (154, "8.1 Stock", "What is the closing stock value?", {"SKIP"}),
    (155, "8.1 Stock", "Show me opening vs closing stock comparison", {"SKIP"}),
    (156, "8.1 Stock", "Which stock items have zero closing balance?", {"SKIP"}),
    (157, "8.1 Stock", "List items with HSN code", {"SKIP"}),
    (158, "8.1 Stock", "Which items are missing HSN codes?", {"SKIP"}),
    (159, "8.1 Stock", "Stock group-wise summary", {"SKIP"}),
    (160, "8.1 Stock", "Show me all batch-wise items", {"SKIP"}),

    # SECTION 9.1: Sales Analysis
    (172, "9.1 Sales", "Total sales this month", {"pl_drilldown", "report_pl"}),
    (173, "9.1 Sales", "Total sales this FY", {"report_pl", "pl_drilldown"}),
    (174, "9.1 Sales", "Monthly sales trend", {"SKIP"}),  # monthly aggregation
    (175, "9.1 Sales", "Party-wise sales summary", {"SKIP"}),
    (176, "9.1 Sales", "Top 10 customers by sales", {"SKIP"}),
    (177, "9.1 Sales", "Sales to a specific customer this year", {"SKIP"}),
    (180, "9.1 Sales", "Average invoice value", {"SKIP"}),
    (181, "9.1 Sales", "Sales return ratio", {"SKIP"}),
    (182, "9.1 Sales", "Day-wise sales for this week", {"SKIP"}),
    (183, "9.1 Sales", "Which day has the highest sales?", {"SKIP"}),
    (184, "9.1 Sales", "Sales concentration — Pareto (80/20) analysis", {"SKIP"}),

    # SECTION 9.2: Purchase Analysis
    (185, "9.2 Purchase", "Total purchases this month", {"pl_drilldown", "report_pl"}),
    (186, "9.2 Purchase", "Total purchases this FY", {"report_pl", "pl_drilldown"}),
    (187, "9.2 Purchase", "Supplier-wise purchase summary", {"SKIP"}),
    (188, "9.2 Purchase", "Top 10 suppliers by purchase value", {"SKIP"}),
    (189, "9.2 Purchase", "Purchase return ratio", {"SKIP"}),
    (190, "9.2 Purchase", "Monthly purchase trend", {"SKIP"}),
    (191, "9.2 Purchase", "Compare purchases vs sales trend", {"SKIP"}),
    (192, "9.2 Purchase", "Average purchase order value", {"SKIP"}),

    # SECTION 10: Expense Analysis
    (193, "10 Expense", "Show me all indirect expenses", {"pl_drilldown"}),
    (194, "10 Expense", "What did we spend on rent this year?", {"ledger_detail"}),
    (195, "10 Expense", "Expense-wise breakup for this month", {"pl_drilldown", "report_pl"}),
    (196, "10 Expense", "Which expense grew the most vs last month?", {"report_pl", "pl_drilldown"}),
    (197, "10 Expense", "Show me all expenses above 1 lakh in a single transaction", {"SKIP"}),
    (198, "10 Expense", "Staff welfare expenses this FY", {"ledger_detail"}),
    (199, "10 Expense", "Travel and conveyance for October", {"ledger_detail"}),
    (200, "10 Expense", "Professional fees paid to whom?", {"ledger_detail"}),
    (201, "10 Expense", "Electricity expense monthly trend", {"ledger_detail", "SKIP"}),
    (202, "10 Expense", "Show me all depreciation entries", {"ledger_detail"}),
    (203, "10 Expense", "Total repairs and maintenance this year", {"ledger_detail"}),
    (204, "10 Expense", "Expense as percentage of revenue — each head", {"report_pl"}),

    # SECTION 11: Ratio Analysis
    (205, "11 Ratios", "Current ratio", {"report_bs"}),
    (206, "11 Ratios", "Quick ratio / Acid test ratio", {"report_bs"}),
    (207, "11 Ratios", "Debt-equity ratio", {"report_bs"}),
    (208, "11 Ratios", "Return on equity (ROE)", {"report_bs", "report_pl"}),
    (209, "11 Ratios", "Return on assets (ROA)", {"report_bs", "report_pl"}),
    (210, "11 Ratios", "Return on capital employed (ROCE)", {"report_bs", "report_pl"}),
    (211, "11 Ratios", "Net profit margin", {"report_pl"}),
    (212, "11 Ratios", "Gross profit margin", {"report_pl"}),
    (213, "11 Ratios", "Operating profit margin", {"report_pl"}),
    (214, "11 Ratios", "Debtors turnover ratio", {"debtors", "report_pl"}),
    (215, "11 Ratios", "Creditors turnover ratio", {"creditors", "report_pl"}),
    (217, "11 Ratios", "Interest coverage ratio", {"report_pl"}),
    (218, "11 Ratios", "Asset turnover ratio", {"report_bs", "report_pl"}),
    (219, "11 Ratios", "Give me a complete ratio analysis dashboard", {"report_bs", "report_pl", "SKIP"}),
    (220, "11 Ratios", "Proprietary ratio", {"report_bs"}),
    (221, "11 Ratios", "Working capital turnover ratio", {"report_bs", "report_pl"}),
    (222, "11 Ratios", "Cash ratio", {"report_bs"}),

    # SECTION 12.1: Anomaly Detection
    (223, "12.1 Anomaly", "Are there any duplicate vouchers?", {"SKIP"}),
    (224, "12.1 Anomaly", "Show me all backdated entries", {"SKIP"}),
    (225, "12.1 Anomaly", "Any voucher number gaps?", {"SKIP"}),
    (226, "12.1 Anomaly", "Show me all manual journal entries", {"SKIP"}),
    (227, "12.1 Anomaly", "Transactions on Sundays", {"SKIP"}),
    (229, "12.1 Anomaly", "Any debit-credit mismatches in vouchers?", {"SKIP"}),
    (230, "12.1 Anomaly", "Cash payments above 10000 (Section 40A(3) risk)", {"SKIP"}),
    (231, "12.1 Anomaly", "Cash receipts above 2 lakh (Section 269ST risk)", {"SKIP"}),
    (233, "12.1 Anomaly", "Transactions just below reporting thresholds", {"SKIP"}),
    (234, "12.1 Anomaly", "Benford's Law distribution of transaction amounts", {"SKIP"}),
    (235, "12.1 Anomaly", "Show me all year-end adjustments (last week of March)", {"SKIP"}),
    (236, "12.1 Anomaly", "Round number transactions concentration", {"SKIP"}),
    (237, "12.1 Anomaly", "Unusual narrations (containing adjustment, correction)", {"SKIP"}),
    (238, "12.1 Anomaly", "Vouchers with amount exceeding 1 crore", {"SKIP"}),

    # SECTION 12.2: Ledger Verification
    (239, "12.2 Ledger Verify", "Ledgers with debit balance that should be credit (and vice versa)", {"SKIP"}),
    (240, "12.2 Ledger Verify", "Expense ledgers with credit balance", {"SKIP"}),
    (241, "12.2 Ledger Verify", "Income ledgers with debit balance", {"SKIP"}),
    (242, "12.2 Ledger Verify", "Ledgers with very high turnover but low closing balance", {"SKIP"}),
    (243, "12.2 Ledger Verify", "Ledgers used only once this year", {"SKIP"}),
    (244, "12.2 Ledger Verify", "Dormant ledgers (no movement all year)", {"SKIP"}),
    (245, "12.2 Ledger Verify", "Sundry Debtors with credit balance", {"SKIP"}),
    (246, "12.2 Ledger Verify", "Sundry Creditors with debit balance", {"SKIP"}),
    (247, "12.2 Ledger Verify", "Show me ledgers without any group", {"SKIP"}),

    # SECTION 13: Payroll
    (248, "13 Payroll", "Total salary expense this month", {"ledger_detail"}),
    (249, "13 Payroll", "Total salary expense this FY", {"pl_drilldown", "ledger_detail"}),
    (253, "13 Payroll", "Monthly payroll cost trend", {"ledger_detail", "SKIP"}),
    (254, "13 Payroll", "Salary as percentage of revenue", {"report_pl"}),
    (256, "13 Payroll", "Bonus and incentive paid this year", {"ledger_detail"}),

    # SECTION 14: Compliance
    (258, "14 Compliance", "Estimated taxable income", {"report_pl"}),
    (263, "14 Compliance", "Interest on borrowed capital", {"ledger_detail"}),
    (264, "14 Compliance", "Preliminary expenses written off", {"ledger_detail"}),
    (267, "14 Compliance", "Is the company profitable for dividend declaration?", {"report_pl"}),
    (271, "14 Compliance", "CSR threshold check (net profit > 5 crore)", {"report_pl"}),

    # SECTION 15: Multi-Period Comparative
    (272, "15 Comparative", "Compare P&L this quarter vs last quarter", {"report_pl"}),
    (275, "15 Comparative", "Month-on-month expense trend", {"report_pl", "pl_drilldown"}),
    (276, "15 Comparative", "Quarterly sales comparison", {"report_pl", "pl_drilldown", "SKIP"}),
    (278, "15 Comparative", "Which expense head had the biggest increase this quarter?", {"report_pl", "pl_drilldown"}),
    (279, "15 Comparative", "Seasonal sales pattern analysis", {"SKIP"}),
    (280, "15 Comparative", "Moving average of daily sales", {"SKIP"}),

    # SECTION 16: Client-Level Business Queries
    (281, "16 Client Biz", "Am I making profit?", {"report_pl"}),
    (282, "16 Client Biz", "How much money do people owe me?", {"debtors"}),
    (283, "16 Client Biz", "How much do I owe to suppliers?", {"creditors"}),
    (284, "16 Client Biz", "How much money is in my bank?", {"ledger_detail", "report_bs"}),
    (285, "16 Client Biz", "What were my total sales today?", {"SKIP"}),
    (286, "16 Client Biz", "How is my business doing this month?", {"report_pl"}),
    (287, "16 Client Biz", "Can I afford to buy equipment worth 10 lakh?", {"report_bs"}),
    (288, "16 Client Biz", "Is my business healthy?", {"report_pl", "report_bs"}),
    (290, "16 Client Biz", "Should I give more credit to customer X?", {"debtors", "SKIP"}),
    (292, "16 Client Biz", "Am I spending too much on anything?", {"report_pl", "pl_drilldown"}),
    (293, "16 Client Biz", "What was my highest sale this year?", {"SKIP"}),
    (294, "16 Client Biz", "How many customers bought from us this month?", {"SKIP"}),
    (295, "16 Client Biz", "Is my business growing?", {"report_pl"}),

    # SECTION 17: Staff Operational
    (296, "17 Staff Ops", "Did I enter the voucher for invoice #5432?", {"SKIP"}),
    (297, "17 Staff Ops", "What is the last voucher number for sales?", {"SKIP"}),
    (298, "17 Staff Ops", "How many entries did I make today?", {"SKIP"}),
    (300, "17 Staff Ops", "Is there a ledger for ABC Pharma?", {"search"}),
    (301, "17 Staff Ops", "Show me all entries I made for XYZ party", {"ledger_detail", "SKIP"}),
    (303, "17 Staff Ops", "Check if this invoice is already entered", {"SKIP"}),
    (304, "17 Staff Ops", "Where did the 50000 from HDFC go?", {"ledger_detail", "SKIP"}),
    (305, "17 Staff Ops", "Show me today's day book", {"SKIP"}),

    # SECTION 18: Advanced Advisory
    (308, "18 Advisory", "What will be the year-end profit at current run rate?", {"report_pl"}),
    (313, "18 Advisory", "What are the top 5 risks in this company's financials?", {"SKIP"}),
    (315, "18 Advisory", "Explain why profit dropped this month", {"report_pl"}),
    (316, "18 Advisory", "Suggest areas to cut costs", {"report_pl", "pl_drilldown"}),

    # SECTION 19: Data Quality
    (326, "19 Data Quality", "How many ledgers are unused (zero transactions)?", {"SKIP"}),
    (327, "19 Data Quality", "Are there duplicate ledgers for the same party?", {"SKIP"}),
    (328, "19 Data Quality", "Ledgers with missing GSTIN", {"SKIP"}),
    (329, "19 Data Quality", "Ledgers with missing PAN", {"SKIP"}),
    (330, "19 Data Quality", "Ledgers without email/phone", {"SKIP"}),
    (331, "19 Data Quality", "Stock items without HSN code", {"SKIP"}),
    (332, "19 Data Quality", "Vouchers with blank narration", {"SKIP"}),
    (333, "19 Data Quality", "Groups with no ledgers under them", {"SKIP"}),
    (334, "19 Data Quality", "How clean is the data overall?", {"SKIP"}),
    (335, "19 Data Quality", "Entries with suspense account", {"SKIP"}),

    # SECTION 20: Edge Cases
    (336, "20 Edge Cases", "", {"chat"}),  # empty question
    (337, "20 Edge Cases", "asdfghjkl", {"chat"}),
    (338, "20 Edge Cases", "Delete all my data", {"chat"}),
    (339, "20 Edge Cases", "What is the weather today?", {"chat"}),
    (340, "20 Edge Cases", "P&L for 2019 (before data range)", {"report_pl", "chat"}),
    (341, "20 Edge Cases", "Balance sheet as on tomorrow's date", {"report_bs"}),
    (342, "20 Edge Cases", "Show me ledger for XYZ_NONEXISTENT_PARTY", {"search", "chat", "ledger_detail"}),
    (344, "20 Edge Cases", "Show me the ledger", {"chat", "search"}),
    (345, "20 Edge Cases", "P&L and BS and TB and debtors all at once", {"report_pl", "report_bs", "report_tb", "debtors"}),
    (347, "20 Edge Cases", "What does ISDEEMEDPOSITIVE mean?", {"chat"}),
    (351, "20 Edge Cases", "Why is the trial balance not tallying?", {"report_tb"}),
    (352, "20 Edge Cases", "Explain this entry: voucher #1234", {"SKIP"}),
    (353, "20 Edge Cases", "mujhe P&L dikhao", {"report_pl"}),
    (354, "20 Edge Cases", "Show me P&L... actually no, show me BS", {"report_bs", "report_pl"}),
    (355, "20 Edge Cases", "What can you do?", {"chat"}),
]


def run_tests():
    results = []
    pass_count = 0
    fail_count = 0
    skip_count = 0

    section_stats = {}  # section -> {"pass": N, "fail": N, "skip": N, "total": N}

    print("=" * 100)
    print("TALLY QUESTION BANK — classify_intent() TEST RESULTS")
    print("=" * 100)
    print()

    for q_num, section, question, acceptable in QUESTIONS:
        # Initialize section stats
        if section not in section_stats:
            section_stats[section] = {"pass": 0, "fail": 0, "skip": 0, "total": 0}
        section_stats[section]["total"] += 1

        # Check if SKIP
        if acceptable == {"SKIP"} or (len(acceptable) == 1 and "SKIP" in acceptable):
            skip_count += 1
            section_stats[section]["skip"] += 1
            results.append((q_num, section, question, "SKIP", "-", "-", True))
            continue

        # Remove SKIP from acceptable if mixed
        real_acceptable = acceptable - {"SKIP"}

        try:
            result = classify_intent(question)
            actual_action = result.get("action", "UNKNOWN")
        except Exception as e:
            actual_action = f"ERROR: {e}"

        passed = actual_action in real_acceptable
        status = "PASS" if passed else "FAIL"

        if passed:
            pass_count += 1
            section_stats[section]["pass"] += 1
        else:
            fail_count += 1
            section_stats[section]["fail"] += 1

        results.append((q_num, section, question, status, sorted(real_acceptable), actual_action, passed))

        # Print each result
        icon = "OK" if passed else "XX"
        print(f"  [{icon}] Q{q_num:3d} | {status:4s} | expected: {sorted(real_acceptable)} | got: {actual_action}")
        if not passed:
            print(f"         ↳ \"{question}\"")

    # ── Summary ──
    total_testable = pass_count + fail_count
    total_all = pass_count + fail_count + skip_count

    print()
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print()
    print(f"  Total questions tested:  {total_all}")
    print(f"  Testable (non-SKIP):     {total_testable}")
    print(f"  PASS:                    {pass_count}")
    print(f"  FAIL:                    {fail_count}")
    print(f"  SKIP (sql_query/complex):{skip_count}")
    print(f"  Pass rate (testable):    {pass_count}/{total_testable} = {100*pass_count/total_testable:.1f}%" if total_testable > 0 else "  Pass rate: N/A")
    print()

    # ── Pass rate by section ──
    print("-" * 100)
    print(f"  {'SECTION':<25s} {'PASS':>5s} {'FAIL':>5s} {'SKIP':>5s} {'TOTAL':>5s} {'RATE (testable)':>18s}")
    print("-" * 100)
    for section in sorted(section_stats.keys()):
        s = section_stats[section]
        testable = s["pass"] + s["fail"]
        rate = f"{100*s['pass']/testable:.0f}%" if testable > 0 else "N/A"
        print(f"  {section:<25s} {s['pass']:>5d} {s['fail']:>5d} {s['skip']:>5d} {s['total']:>5d} {rate:>18s}")
    print("-" * 100)
    print()

    # ── All FAILed questions ──
    failures = [(q_num, section, question, expected, actual) for q_num, section, question, status, expected, actual, _ in results if status == "FAIL"]
    if failures:
        print("=" * 100)
        print(f"ALL FAILED QUESTIONS ({len(failures)})")
        print("=" * 100)
        for q_num, section, question, expected, actual in failures:
            print(f"\n  Q{q_num} [{section}]")
            print(f"    Question: \"{question}\"")
            print(f"    Expected: {expected}")
            print(f"    Actual:   {actual}")
    else:
        print("  No failures! All testable questions passed.")

    print()
    print("=" * 100)
    print("END OF REPORT")
    print("=" * 100)


if __name__ == "__main__":
    run_tests()
