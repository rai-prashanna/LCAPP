from datetime import datetime
import re

def printMapper(mapper):
    for key in mapper:
        print(f"{key}: {mapper.get(key)}")

def LoadItemIdItemName():
    rawoutputfile = open("sales_data/item_categories.csv", "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        itemid, itemname = line.split(',')
        mapper[itemid] = itemname.rstrip("\n")
    return mapper

def LoadCategoryMapper():
    rawoutputfile = open("sales_data/categories.csv", "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        Category, Subcategory = line.split(',')
        trimmedKey=Subcategory.rstrip("\n")
        mapper[trimmedKey] = Category
    return mapper

def MapItemIDtoName(ItemCategoryMapper):
    SalesbyName = {}
    rawoutputfile = open("assingment1.out/part-00000", "r")
    for line in rawoutputfile:
        result = re.sub('[()]', '', line)
        itemid, sales = result.split(',')
        itemid=itemid.strip('\'')
        sales=sales.strip(' ').rstrip("\n")
        itemName=ItemCategoryMapper.get(itemid)
        if itemName in SalesbyName:
            previous_sales=SalesbyName[itemName]
            SalesbyName[itemName]=previous_sales+int(sales)
        else:
            SalesbyName[itemName] = int(sales)
    return SalesbyName

def SalesofEachSubCategory(SalesbyName,CategoryMapper):
    SubCategorySales = {}
    for itemName in SalesbyName:
        subtotal=SalesbyName[itemName]
        category=CategoryMapper[itemName]
        if category in SubCategorySales:
            previous_value=SubCategorySales[category]
            SubCategorySales[category]=previous_value+subtotal
        else:
            SubCategorySales[category] = subtotal
    return SubCategorySales

def salesOfEachCategory(SubCategorySales,CategoryMapper):
    categorySales={}
    for itemName in SubCategorySales:
        subtotal = SubCategorySales[itemName]
        category = CategoryMapper[itemName]
        if category in categorySales:
            previous_value = categorySales[category]
            categorySales[category] = previous_value + subtotal
        else:
            categorySales[category] = subtotal
    return categorySales

# ItemCategoryMapper=LoadItemIdItemName()
# SalesbyName=MapItemIDtoName(ItemCategoryMapper)
# CategoryMapper = LoadCategoryMapper()
# result=subtotalingbottomToTopApproach(SalesbyName,CategoryMapper)
# print(result)


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

def monthlySalesReport(list):
    return null

