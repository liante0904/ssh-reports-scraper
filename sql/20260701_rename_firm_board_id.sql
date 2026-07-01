ALTER TABLE tbl_sec_reports RENAME COLUMN sec_firm_order TO firm_id;
ALTER TABLE tbl_sec_reports RENAME COLUMN article_board_order TO board_id;
ALTER INDEX idx_tb_sec_reports_reg_dt_sec_firm RENAME TO idx_tb_sec_reports_reg_dt_firm_id;
