from datetime import datetime
import re
import calendar

def printMapper(mapper):
    for key in mapper:
        print(f"{key}: {mapper.get(key)}")

def LoadItemIdItemName(src):
    #"sales_data/item_categories.csv"
    rawoutputfile = open(src, "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        itemid, itemname = line.split(',')
        mapper[itemid] = itemname.rstrip("\n")
    return mapper

def LoadCategoryMapper(src):
    #"sales_data/categories.csv"
    rawoutputfile = open(src, "r")
    mapper={}
    next(rawoutputfile)
    for line in rawoutputfile:
        Category, Subcategory = line.split(',')
        trimmedKey=Subcategory.rstrip("\n")
        mapper[trimmedKey] = Category
    return mapper

def MapItemIDtoName(ItemCategoryMapper,src):
    #"assingment1.out/part-00000"
    SalesbyName = {}
    rawoutputfile = open(src, "r")
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
    print(SubCategorySales)
    return SubCategorySales

def weeklySalesSubCategory(SalesbyName,CategoryMapper):
    SubCategorySales = {}
    for (itemName,week) in SalesbyName:
        subtotal=SalesbyName[(itemName,week)]
        category=CategoryMapper[itemName]
        if (category,week) in SubCategorySales:
            previous_value=SubCategorySales[(category,week)]
            SubCategorySales[(category,week)]=previous_value+subtotal
        else:
            SubCategorySales[(category,week)] = subtotal

    return SubCategorySales

def weeklySalessecondCategory(SalesbyName,CategoryMapper):
    SubCategorySales = {}
    for (itemName,week) in SalesbyName:
        subtotal=SalesbyName[(itemName,week)]
        category=CategoryMapper[itemName]
        if (category,week) in SubCategorySales:
            previous_value=SubCategorySales[(category,week)]
            SubCategorySales[(category,week)]=previous_value+subtotal
        else:
            SubCategorySales[(category,week)] = subtotal
    return SubCategorySales

def weeklysalesOfEachCategory(SubCategorySales,CategoryMapper):
    categorySales={}
    for (itemName,week) in SubCategorySales:
        subtotal = SubCategorySales[(itemName,week)]
        category = CategoryMapper[itemName]
        if (category,week) in categorySales:
            previous_value = categorySales[(category,week)]
            categorySales[(category,week)] = previous_value + subtotal
        else:
            categorySales[(category,week)] = subtotal
    for (itemName,week) in categorySales:
        if (itemName,week) in SubCategorySales:
            intermediate_value = SubCategorySales[(itemName,week)]
            previous_value=categorySales[(itemName,week) ]
            categorySales[(itemName,week) ]=previous_value+intermediate_value
            #okay
    for (itemName,week) in categorySales:
        if (itemName) in CategoryMapper:
            parentitemaname = CategoryMapper[itemName]
            intermediate_value = categorySales[(itemName,week)]
            previous_value=categorySales[(parentitemaname,week) ]
            categorySales[(parentitemaname,week) ]=previous_value+intermediate_value
    return categorySales

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
    for itemName in categorySales:
        if itemName in CategoryMapper:
            parentName=CategoryMapper[itemName]
            intermediate_value = categorySales[itemName]
            previous_value=categorySales[parentName]
            categorySales[parentName]=previous_value+intermediate_value
    for itemName in SubCategorySales:
        subtotal = SubCategorySales[itemName]
        category = CategoryMapper[itemName]
        if category in SubCategorySales:
            previous_value = SubCategorySales[category]
            categorySales[category] = previous_value + subtotal
    return categorySales



def salesOfAllCategory(categorySales,CategoryMapper):
    categoryAllSales={}
    for itemName in categorySales:
        subtotal = categorySales[itemName]
        category = CategoryMapper[itemName]
        if category in categorySales:
            previous_value = categorySales[category]
            categorySales[category] = previous_value + subtotal
        else:
            categorySales[category] = subtotal
    return categorySales
def lineSplitterByWeek(line):
    givendatetime,itemid,sale=line.split(",")
    date_time_obj=datetime.strptime(givendatetime, '%Y-%m-%d %H:%M')
    if(date_time_obj.weekday()>4):
        # 0 for weekend
        return (itemid,0), sale
    else:
        # 1 for weekday
        return (itemid,1), sale

def weeklyconvertIdtoItemName(tuples,src):
    itemMapper=LoadItemIdItemName(src)
    itemSales={}
    for tuple in tuples:
        ((item_id,week),sales)=tuple
        itemName=itemMapper[item_id]
        if week == 0:
            if (itemName,"weekend") in itemSales:
                previous_value = itemSales[(itemName,"weekend")]
                itemSales[(itemName,"weekend")] = sales+previous_value
            else:
                itemSales[(itemName, "weekend")] = sales
        else:
            if (itemName,"weekday") in itemSales:
                previous_value = itemSales[(itemName,"weekday")]
                itemSales[(itemName,"weekday")] = sales+previous_value
            else:
                itemSales[(itemName, "weekday")] = sales
    return itemSales



def lineSplitterByMonth(line):
    givendatetime,itemid,sale=line.split(",")
    date_time_obj=datetime.strptime(givendatetime, '%Y-%m-%d %H:%M')
    return (itemid, date_time_obj.month), sale

def getSubCategoryOfCategory(category,src):
    #"sales_data/categories.csv"
    rawoutputfile = open(src, "r")
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


def getItemIdsFromSubCategory(subcategories,src):
    #"sales_data/item_categories.csv"
    rawoutputfile = open(src, "r")
    items = []
    next(rawoutputfile)
    for line in rawoutputfile:
        itemid, itemname = line.split(',')
        if(itemname.rstrip("\n") in subcategories):
            items.append(itemid)
    return items

def convertIdtoItemName(filterdataframe,src):
    itemMapper=LoadItemIdItemName(src)
    itemSales={}
    for row in filterdataframe:
        itemId=row.Item_Id
        month=row.month
        sales=row.Sale
        itemName=itemMapper[itemId]
        itemSales[(itemName,calendar.month_name[month])]=sales
    return itemSales

def monthlysalesofSubcategory(monthlyItemSales,CategoryMapper):
    categorySales={}
    for (itemName,month) in monthlyItemSales:
        subtotal = monthlyItemSales[(itemName,month)]
        category = CategoryMapper[itemName]
        if (category,month) in categorySales:
            previous_value = categorySales[(category,month)]
            categorySales[(category,month)] = previous_value + subtotal
        else:
            categorySales[(category,month)] = subtotal
    return categorySales
#need fix
def monthlysalesOfEachCategory(monthlySubCategorySales,CategoryMapper):
    categorySales={}
    for (itemName,month) in monthlySubCategorySales:
        subtotal = monthlySubCategorySales[(itemName,month)]
        category = CategoryMapper[itemName]
        if (category,month) in categorySales:
            previous_value = categorySales[(category,month)]
            categorySales[(category,month)] = previous_value + subtotal
        else:
            categorySales[(category,month)] = subtotal


    for (itemName, month) in categorySales:
        if (itemName) in CategoryMapper:
            parentitemaname = CategoryMapper[itemName]
            intermediate_value = categorySales[(itemName, month)]
            previous_value = categorySales[(parentitemaname, month)]
            categorySales[(parentitemaname, month)] = previous_value + intermediate_value

    for (itemName,month) in categorySales:
        if (itemName, month) in monthlySubCategorySales:
            intermediate_value = monthlySubCategorySales[(itemName, month)]
            previous_value = categorySales[(itemName, month)]
            categorySales[(itemName, month)] = previous_value + intermediate_value
    return categorySales


