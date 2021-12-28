from datetime import datetime

def lineSplitter(line):
    givendatetime,itemid,sale=line.split(",")
    date_time_obj=datetime.strptime(givendatetime, '%Y-%m-%d %H:%M')
    if(date_time_obj.weekday()>4):
        return (itemid,0), sale

    else:
        return (itemid,1), sale
