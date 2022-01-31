from datetime import datetime

dates = ["Mar 23, 1994 12:01 PM","Mar 24, 1995 12:02 PM","Mar 25, 1996 12:03 PM","Mar 26, 1997 12:04 PM"]
for d in dates:
    date_obj = datetime.strptime(d, "%b %d, %Y %I:%M %p")
    print(date_obj.isoformat())