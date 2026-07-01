import os

target_dir = "/home/ubuntu/workspace/external.reports-hub/apps/scrapers/ssh-reports-scraper"
exclude_files = {
    "TBM_SEC_FIRM_INFO.sql",
    "TBM_SEC_FIRM_BOARD_INFO.sql",
    "20260701_add_firm_info_views.sql",
    "20260701_rename_firm_board_id.sql",
    "TB_SEC_REPORTS.sql"
}

def replace_in_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = content.replace('firm_id', 'firm_id')
    new_content = new_content.replace('board_id', 'board_id')

    # firm_utils.py 내부에서 tbm_sec_firm_info 등을 v_sec_firm_info 로 변경
    if filepath.endswith("firm_utils.py"):
        new_content = new_content.replace('tbm_sec_firm_info', 'v_sec_firm_info')
        new_content = new_content.replace('tbm_sec_firm_board_info', 'v_sec_firm_board_info')

    if content != new_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filepath}")

for root, _, files in os.walk(target_dir):
    if '.git' in root or '__pycache__' in root or '.venv' in root:
        continue
    for file in files:
        if file in exclude_files:
            continue
        if file.endswith('.py') or file.endswith('.md') or file.endswith('.yaml') or file.endswith('.sql'):
            replace_in_file(os.path.join(root, file))
