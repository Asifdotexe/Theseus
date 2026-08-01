import re
import datetime
from pathlib import Path

def update_date():
    index_path = Path("index.html")
    if not index_path.exists():
        return

    content = index_path.read_text(encoding="utf-8")
    original_content = content
    
    now = datetime.datetime.now()
    iso_date = now.strftime("%Y-%m-%d")
    human_date = now.strftime("%B %d, %Y").replace(" 0", " ") # Format like 'July 26, 2026'

    # Update dateModified in JSON-LD
    content = re.sub(r'"dateModified": "\d{4}-\d{2}-\d{2}"', f'"dateModified": "{iso_date}"', content)
    
    # Update time tag
    content = re.sub(
        r'<time datetime="\d{4}-\d{2}-\d{2}">[^<]+</time>',
        f'<time datetime="{iso_date}">{human_date}</time>',
        content
    )
    
    if content != original_content:
        index_path.write_text(content, encoding="utf-8")
        print(f"Updated index.html date to {iso_date}")

if __name__ == "__main__":
    update_date()
