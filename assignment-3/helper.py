from datetime import datetime

def lineSplitterByWeek(line):
    givendatetime,itemid,sale=line.split(",")
    date_time_obj=datetime.strptime(givendatetime, '%Y-%m-%d %H:%M')
    if(date_time_obj.weekday()>4):
        # 0 for weekend
        return (itemid,0), sale
    else:
        # 1 for weekend
        return (itemid,1), sale

def lineSplitterByMonth(line):
    givendatetime,itemid,sale=line.split(",")
    date_time_obj=datetime.strptime(givendatetime, '%Y-%m-%d %H:%M')
    return (itemid, date_time_obj.month), sale

def getSubCategoryOfCategory(category):
    rawoutputfile = open("sales_data/categories.csv", "r")
    subcategories = []
    next(rawoutputfile)
    for line in rawoutputfile:
        Category, Subcategory = line.split(',')
        trimmedKey = Subcategory.rstrip("\n")
        if(Category in subcategories):
            subcategories.append(trimmedKey)
        if(Category in category):
            subcategories.append(trimmedKey)
    return subcategories


def getItemIdsFromSubCategory(subcategories):
    rawoutputfile = open("sales_data/item_categories.csv", "r")
    items = []
    next(rawoutputfile)
    for line in rawoutputfile:
        itemid, itemname = line.split(',')
        if(itemname.rstrip("\n") in subcategories):
            items.append(itemid)
    return items


subclothes=getSubCategoryOfCategory("Clothes")
Subsubclothes=getItemIdsFromSubCategory(subclothes)
print(Subsubclothes)
